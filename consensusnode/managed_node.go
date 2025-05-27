package consensusnode

import (
	"context"
	"encoding/hex"
	"encoding/json" // For marshaling SyncRequestPayload
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sort" // For sorting events by frame and timestamp
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"

	"github.com/blockchain/consensus/dag"    // Path to your DAG implementation
	"github.com/blockchain/consensus/logger" // Path to your custom logger
)

// YourStorageInterface is a placeholder for your specific storage interface.
type YourStorageInterface interface {
	Get(key []byte) ([]byte, error)
	Put(key []byte, value []byte) error
	Delete(key []byte) error
}

// ManagedNode is the central object for network operations and consensus.
type ManagedNode struct {
	config       NodeConfig
	host         host.Host
	privKey      crypto.PrivKey
	ownPubKeyHex string // Hex string of this node's public key.
	pubsub       *pubsub.PubSub
	dagStore     *dag.DagStore

	topicStorageMap sync.Map

	peers       map[peer.ID]*ManagedPeerInfo
	peerMutex   sync.RWMutex
	reconnectWG sync.WaitGroup

	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup

	streamHandlers     map[protocol.ID]network.StreamHandler
	topicSubscriptions map[string]*pubsub.Subscription
	topicHandlers      map[string]func(msg *pubsub.Message)

	keyValueCache               *lru.Cache[string, []byte]
	transactionChan             chan []byte
	fetchingBlocks              sync.Map
	feeAddresses                []string
	feeAddressesMux             sync.RWMutex
	lastProcessedFinalizedFrame uint64

	connectedPeerPubKeys map[peer.ID]string
	peerPubKeyMutex      sync.RWMutex

	// recentEventsBuffer stores recently received/created events to select as OtherParents.
	// Key: EventID, Value: *dag.Event. This is a simplified mechanism.
	// A more robust solution might use an LRU cache or a time-windowed buffer.
	recentEventsBuffer map[dag.EventID]*dag.Event
	recentEventsMutex  sync.RWMutex
	maxRecentEvents    int // Max number of events to keep in recentEventsBuffer
}

const (
	FRAMES_TO_KEEP_AFTER_FINALIZED uint64 = 5
	MIN_FRAMES_BEFORE_PRUNING      uint64 = FRAMES_TO_KEEP_AFTER_FINALIZED + 5
	MAX_OTHER_PARENTS              int    = 2  // Max number of other parents to select for a new event
	MAX_UNDECIDED_FRAME_DIFFERENCE uint64 = 10 // Ngưỡng chênh lệch frame chưa quyết định

)

// NewManagedNode creates and initializes a new ManagedNode.
func NewManagedNode(ctx context.Context, cfg NodeConfig) (*ManagedNode, error) {
	privKey, err := loadPrivateKey(cfg.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to load private key: %w", err)
	}

	var ownPubKeyHex string
	if privKey != nil {
		pubKey := privKey.GetPublic()
		rawPubKeyBytes, extractionError := pubKey.Raw()
		if extractionError != nil {
			// log.Printf("Warning: Could not get raw public key bytes using pubKey.Raw(): %v. Attempting with specific key type.", extractionError)
			if ecdsaPriv, ok := privKey.(*crypto.Secp256k1PrivateKey); ok {
				ecdsaTypedPubKey := ecdsaPriv.GetPublic().(*crypto.Secp256k1PublicKey)
				ecdsaBytes, ecdsaErr := ecdsaTypedPubKey.Raw()
				if ecdsaErr == nil {
					rawPubKeyBytes = ecdsaBytes
					extractionError = nil
				} else {
					extractionError = ecdsaErr
				}
			}
		}
		if extractionError == nil && rawPubKeyBytes != nil {
			ownPubKeyHex = hex.EncodeToString(rawPubKeyBytes)
		} else {
			log.Printf("Could not extract final public key bytes from private key. Error (if any): %v", extractionError)
		}
	}
	if ownPubKeyHex == "" && privKey != nil {
		log.Printf("CRITICAL WARNING: ownPubKeyHex is empty despite having a private key. Node selection might not function correctly.")
	}
	if ownPubKeyHex != "" {
		log.Printf("===== Node Public Key (Hex): %s =====", ownPubKeyHex)
	} else {
		log.Printf("===== Node Public Key (Hex): (Could not determine) =====")
	}

	connManager, err := connmgr.NewConnManager(
		cfg.MinConnections,
		cfg.MaxConnections,
		connmgr.WithGracePeriod(cfg.ConnectionGracePeriod),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection manager: %w", err)
	}

	libp2pHost, err := libp2p.New(
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings(cfg.ListenAddress),
		libp2p.ConnectionManager(connManager),
		libp2p.EnableRelayService(),
		libp2p.EnableNATService(),
		libp2p.NATPortMap(),
		libp2p.EnableHolePunching(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}

	pubsubOptions := []pubsub.Option{
		pubsub.WithMaxMessageSize(cfg.MaxMessageSize),
		pubsub.WithMessageSignaturePolicy(pubsub.StrictSign), // Enforce message signing
		pubsub.WithFloodPublish(true),                        // Use flood publishing
	}
	ps, err := pubsub.NewGossipSub(ctx, libp2pHost, pubsubOptions...)
	if err != nil {
		_ = libp2pHost.Close()
		return nil, fmt.Errorf("failed to create pubsub: %w", err)
	}

	nodeCtx, cancel := context.WithCancel(ctx)

	cache, err := lru.New[string, []byte](cfg.KeyValueCacheSize)
	if err != nil {
		_ = libp2pHost.Close()
		cancel()
		return nil, fmt.Errorf("failed to create LRU cache: %w", err)
	}

	initialStakeData := make(map[string]uint64)
	if len(cfg.AllStakers) > 0 {
		log.Printf("Populating DagStore.stake from cfg.AllStakers. Configured staker count: %d", len(cfg.AllStakers))
		for _, staker := range cfg.AllStakers {
			if staker.PubKeyHex == "" {
				log.Printf("Warning: Staker in config has empty PubKeyHex. Skipping. Stake value was: %d", staker.Stake)
				continue
			}
			initialStakeData[staker.PubKeyHex] = staker.Stake
			log.Printf("  Staker %s... registered in initialStakeData with stake %d", staker.PubKeyHex[:min(10, len(staker.PubKeyHex))], staker.Stake)
		}
	} else {
		log.Printf("CRITICAL WARNING: cfg.AllStakers is empty. Consensus will likely stall as other nodes' stakes are unknown.")
	}
	dagStoreInstance := dag.NewDagStore(initialStakeData)

	mn := &ManagedNode{
		config:                      cfg,
		host:                        libp2pHost,
		privKey:                     privKey,
		ownPubKeyHex:                ownPubKeyHex,
		pubsub:                      ps,
		dagStore:                    dagStoreInstance,
		peers:                       make(map[peer.ID]*ManagedPeerInfo),
		ctx:                         nodeCtx,
		cancelFunc:                  cancel,
		streamHandlers:              make(map[protocol.ID]network.StreamHandler),
		topicSubscriptions:          make(map[string]*pubsub.Subscription),
		topicHandlers:               make(map[string]func(msg *pubsub.Message)),
		keyValueCache:               cache,
		transactionChan:             make(chan []byte, cfg.TransactionChanBuffer),
		connectedPeerPubKeys:        make(map[peer.ID]string),
		lastProcessedFinalizedFrame: 0,
		recentEventsBuffer:          make(map[dag.EventID]*dag.Event),
		maxRecentEvents:             100, // Example size, make configurable if needed
	}

	if mn.config.NodeType == "consensus" {
		mn.RegisterStreamHandler(TransactionsRequestProtocol, mn.transactionsRequestHandler)
		log.Printf("Registered stream handler for %s (Node Type: %s)", TransactionsRequestProtocol, mn.config.NodeType)
		mn.RegisterStreamHandler(SyncRequestProtocol, mn.syncRequestHandler)
		log.Printf("Registered stream handler for %s (Node Type: %s)", SyncRequestProtocol, mn.config.NodeType)
	}

	mn.displayNodeInfo()
	return mn, nil
}

// Start initiates the node's operations.
func (mn *ManagedNode) Start() error {
	for protoID, handler := range mn.streamHandlers {
		mn.host.SetStreamHandler(protoID, handler)
		log.Printf("Set stream handler for protocol %s on host.", protoID)
	}

	if err := mn.connectToBootstrapPeers(); err != nil {
		log.Printf("NOTE: Failed to connect to some bootstrap peers: %v", err)
	}

	mn.wg.Add(1)
	go mn.peerHealthMonitor()

	mn.setupConnectionNotifier()

	// Subscribe to a general event topic for gossiping new events
	// The handler will add received events to the DAG and recentEventsBuffer.
	// Ensure EVENT_TOPIC_NAME is defined, e.g., "consensus/events"
	const EVENT_TOPIC_NAME = "consensus/events/v1"
	err := mn.SubscribeAndHandle(EVENT_TOPIC_NAME, mn.eventTopicHandler)
	if err != nil {
		log.Printf("CRITICAL: Failed to subscribe to event topic %s: %v. Event gossiping will not work.", EVENT_TOPIC_NAME, err)
		// Depending on requirements, this could be a fatal error.
	} else {
		log.Printf("Successfully subscribed to event topic: %s", EVENT_TOPIC_NAME)
	}

	log.Printf("ManagedNode (%s) started successfully.", mn.host.ID())

	if mn.config.NodeType == "consensus" {
		mn.wg.Add(1)
		go mn.consensusLoop()
	}
	return nil
}

// eventTopicHandler processes events received from the PubSub topic.
func (mn *ManagedNode) eventTopicHandler(msg *pubsub.Message) {
	// log.Printf("Received event from PubSub topic from peer %s", msg.GetFrom().String())

	event, err := dag.Unmarshal(msg.Data)
	if err != nil {
		log.Printf("Error unmarshaling event from PubSub message (from %s): %v", msg.GetFrom().String(), err)
		return
	}

	// Validate event signature (important for security)
	// This requires access to the creator's public key, which might need to be fetched or managed.
	// For now, skipping full signature validation for brevity, but it's crucial.
	// Example:
	// pubKeyBytes, _ := hex.DecodeString(hex.EncodeToString(event.EventData.Creator)) // Assuming creator is pubkey
	// pubKey, _ := crypto.UnmarshalPublicKey(pubKeyBytes)
	// hashToVerify, _ := event.Hash() // Ensure Hash() doesn't include signature
	// valid, _ := pubKey.Verify(hashToVerify.Bytes(), event.Signature)
	// if !valid {
	// 	log.Printf("Invalid signature for event %s from %s. Discarding.", event.GetEventId().Short(), msg.GetFrom().String())
	// 	return
	// }

	if err := mn.dagStore.AddEvent(event); err != nil {
		if !strings.Contains(err.Error(), "already exists") { // Don't log "already exists" as error
			log.Printf("Error adding event %s (from PubSub peer %s) to DagStore: %v", event.GetEventId().Short(), msg.GetFrom().String(), err)
		}
		return
	}
	// log.Printf("Added event %s from PubSub (peer %s) to DagStore.", event.GetEventId().Short(), msg.GetFrom().String())

	// Add to recent events buffer
	mn.addEventToRecentBuffer(event)
}

// Stop gracefully shuts down the ManagedNode.
func (mn *ManagedNode) Stop() error {
	log.Println("Stopping ManagedNode...")
	mn.cancelFunc()

	mn.cancelAllReconnects()
	mn.reconnectWG.Wait()

	// Unsubscribe from all topics
	mn.peerMutex.RLock() // Lock for reading topicSubscriptions
	topicsToUnsub := make([]string, 0, len(mn.topicSubscriptions))
	for topicName := range mn.topicSubscriptions {
		topicsToUnsub = append(topicsToUnsub, topicName)
	}
	mn.peerMutex.RUnlock()

	for _, topicName := range topicsToUnsub {
		if err := mn.UnsubscribeFromTopic(topicName); err != nil {
			log.Printf("Error unsubscribing from topic %s during shutdown: %v", topicName, err)
		}
	}

	if err := mn.host.Close(); err != nil {
		log.Printf("Error closing libp2p host: %v", err)
	}

	mn.wg.Wait()
	log.Println("ManagedNode stopped.")
	return nil
}

// Host returns the libp2p host instance.
func (mn *ManagedNode) Host() host.Host {
	return mn.host
}

// Context returns the node's context.
func (mn *ManagedNode) Context() context.Context {
	return mn.ctx
}

// getOwnPublicKeyHex returns the hex-encoded public key of the current node.
func (mn *ManagedNode) getOwnPublicKeyHex() (string, error) {
	if mn.ownPubKeyHex == "" {
		if mn.privKey != nil {
			pubKey := mn.privKey.GetPublic()
			var tempPubKeyBytes []byte
			var tempErr error
			tempPubKeyBytes, tempErr = pubKey.Raw()
			if tempErr != nil {
				if ecdsaPriv, ok := mn.privKey.(*crypto.Secp256k1PrivateKey); ok {
					ecdsaTypedPubKey := ecdsaPriv.GetPublic().(*crypto.Secp256k1PublicKey)
					tempPubKeyBytes, tempErr = ecdsaTypedPubKey.Raw()
				}
			}
			if tempErr == nil && tempPubKeyBytes != nil {
				mn.ownPubKeyHex = hex.EncodeToString(tempPubKeyBytes)
			} else {
				return "", fmt.Errorf("failed to re-derive node's own public key: %v", tempErr)
			}
			if mn.ownPubKeyHex == "" {
				return "", errors.New("ownPubKeyHex is still empty after re-derivation")
			}
		} else {
			return "", errors.New("private key is nil, cannot derive public key")
		}
	}
	return mn.ownPubKeyHex, nil
}

// getConnectedPeerPublicKeys returns a map of connected peer.IDs to their public key hex strings.
func (mn *ManagedNode) getConnectedPeerPublicKeys() (map[peer.ID]string, error) {
	mn.peerPubKeyMutex.RLock()
	defer mn.peerPubKeyMutex.RUnlock()
	keysCopy := make(map[peer.ID]string, len(mn.connectedPeerPubKeys))
	for pID, pubKeyHex := range mn.connectedPeerPubKeys {
		keysCopy[pID] = pubKeyHex
	}
	return keysCopy, nil
}

// GetDagStore returns the instance of DagStore.
func (mn *ManagedNode) GetDagStore() *dag.DagStore {
	return mn.dagStore
}

// addEventToRecentBuffer adds an event to the recent events buffer, evicting oldest if full.
// This is a very basic FIFO eviction.
func (mn *ManagedNode) addEventToRecentBuffer(event *dag.Event) {
	if event == nil {
		return
	}
	mn.recentEventsMutex.Lock()
	defer mn.recentEventsMutex.Unlock()

	if _, exists := mn.recentEventsBuffer[event.GetEventId()]; exists {
		return // Already in buffer
	}

	if len(mn.recentEventsBuffer) >= mn.maxRecentEvents {
		// Basic FIFO eviction: find the "oldest" (e.g., lowest timestamp or random for simplicity)
		// This is not efficient. An LRU cache or a queue with timestamps would be better.
		var oldestID dag.EventID
		var oldestTimestamp int64 = -1

		for id, e := range mn.recentEventsBuffer {
			if oldestTimestamp == -1 || e.EventData.Timestamp < oldestTimestamp {
				oldestTimestamp = e.EventData.Timestamp
				oldestID = id
			}
		}
		if !oldestID.IsZero() {
			delete(mn.recentEventsBuffer, oldestID)
		}
	}
	mn.recentEventsBuffer[event.GetEventId()] = event
}

// selectOtherParents selects a diverse set of recent events as other parents.
func (mn *ManagedNode) selectOtherParents(selfCreatorHex string) []dag.EventID {
	mn.recentEventsMutex.RLock()
	defer mn.recentEventsMutex.RUnlock()

	var candidates []*dag.Event
	candidateScores := make(map[dag.EventID]int) // Higher score is better

	for _, event := range mn.recentEventsBuffer {
		eventCreatorHex := hex.EncodeToString(event.EventData.Creator)
		if eventCreatorHex == selfCreatorHex {
			continue // Don't select an event from the same creator as self-parent
		}

		// Score based on recency (higher frame, then higher timestamp)
		// This is implicitly handled by sorting later, but can be a factor
		score := 0 // Base score

		// Prioritize IS-CLOTHO events
		// Note: Accessing mn.dagStore requires careful consideration of locks if this function
		// itself is called under a lock that might conflict with dagStore locks.
		// Assuming mn.dagStore.GetEvent is thread-safe or appropriately locked internally.
		if storedEvent, exists := mn.dagStore.GetEvent(event.GetEventId()); exists {
			if storedEvent.ClothoStatus == dag.ClothoIsClotho {
				score += 100 // High score for IS-CLOTHO
			}
		}
		candidateScores[event.GetEventId()] = score
		candidates = append(candidates, event)
	}

	// Attempt to use SelectReferenceNode to pick one strong candidate
	var refNodeEventID dag.EventID
	if len(mn.peers) > 0 && mn.dagStore != nil {
		heights := make(map[NodeID]uint64)
		inDegrees := make(map[NodeID]uint64)
		var candidateNodeIDs []NodeID // NodeIDs for SelectReferenceNode (peer public keys)

		// Populate heights, inDegrees, and candidateNodeIDs from known/connected peers
		// This requires mn.connectedPeerPubKeys and access to mn.dagStore
		mn.peerPubKeyMutex.RLock()
		for _, pubKeyHex := range mn.connectedPeerPubKeys {
			if pubKeyHex == selfCreatorHex || pubKeyHex == "" {
				continue
			}
			nodeID := GetNodeIDFromString(pubKeyHex)
			candidateNodeIDs = append(candidateNodeIDs, nodeID)

			if h, ok := mn.dagStore.GetHeightForNode(pubKeyHex); ok {
				heights[nodeID] = h
			} else {
				heights[nodeID] = 0 // Default if no height found
			}
			if id, ok := mn.dagStore.GetInDegreeForNode(pubKeyHex); ok {
				inDegrees[nodeID] = id
			} else {
				inDegrees[nodeID] = 0 // Default
			}
		}
		mn.peerPubKeyMutex.RUnlock()

		if len(candidateNodeIDs) > 0 {
			selectedRefNodePubKeyHex, err := SelectReferenceNode(heights, inDegrees, candidateNodeIDs)
			if err == nil && selectedRefNodePubKeyHex != "" {
				// Get the latest event from this selected reference node
				latestEventID, exists := mn.dagStore.GetLatestEventIDByCreatorPubKeyHex(string(selectedRefNodePubKeyHex))
				if exists && !latestEventID.IsZero() {
					if _, isSelfParentCand := candidateScores[latestEventID]; !isSelfParentCand && hex.EncodeToString(mn.dagStore.GetEventCreator(latestEventID)) != selfCreatorHex {
						refNodeEventID = latestEventID
						// Give it a very high score to ensure it's considered
						// Check if it's already in candidates, if not add it
						foundInCandidates := false
						for _, cand := range candidates {
							if cand.GetEventId() == refNodeEventID {
								foundInCandidates = true
								break
							}
						}
						if !foundInCandidates {
							if refEvent, ok := mn.dagStore.GetEvent(refNodeEventID); ok {
								candidates = append(candidates, refEvent)
							}
						}
						candidateScores[refNodeEventID] = 200 // Even higher score
						log.Printf("selectOtherParents: Selected reference event %s from node %s", refNodeEventID.Short(), selectedRefNodePubKeyHex)
					}
				}
			} else if err != nil {
				// log.Printf("selectOtherParents: Error selecting reference node: %v", err)
			}
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	// Sort candidates: by score descending, then by frame descending, then by timestamp descending
	sort.Slice(candidates, func(i, j int) bool {
		scoreI := candidateScores[candidates[i].GetEventId()]
		scoreJ := candidateScores[candidates[j].GetEventId()]
		if scoreI != scoreJ {
			return scoreI > scoreJ
		}
		if candidates[i].EventData.Frame != candidates[j].EventData.Frame {
			return candidates[i].EventData.Frame > candidates[j].EventData.Frame
		}
		return candidates[i].EventData.Timestamp > candidates[j].EventData.Timestamp
	})

	var selectedParents []dag.EventID
	selectedCreators := make(map[string]struct{})

	// If a reference node event was chosen and is valid, add it first
	if !refNodeEventID.IsZero() {
		if refEvent, exists := mn.dagStore.GetEvent(refNodeEventID); exists {
			refCreatorHex := hex.EncodeToString(refEvent.EventData.Creator)
			if refCreatorHex != selfCreatorHex { // Should be true from above check
				selectedParents = append(selectedParents, refNodeEventID)
				selectedCreators[refCreatorHex] = struct{}{}
			}
		}
	}

	for _, cand := range candidates {
		if len(selectedParents) >= MAX_OTHER_PARENTS {
			break
		}
		// Avoid re-adding the reference event if it was already added
		isAlreadySelected := false
		for _, sp := range selectedParents {
			if sp == cand.GetEventId() {
				isAlreadySelected = true
				break
			}
		}
		if isAlreadySelected {
			continue
		}

		candCreatorHex := hex.EncodeToString(cand.EventData.Creator)
		if _, exists := selectedCreators[candCreatorHex]; !exists {
			selectedParents = append(selectedParents, cand.GetEventId())
			selectedCreators[candCreatorHex] = struct{}{}
		}
	}
	// log.Printf("Selected %d other parents: %v", len(selectedParents), selectedParents)
	return selectedParents
} // --- KẾT THÚC ĐOẠN CODE CẬP NHẬT ---

// calculateNextFrame determines the frame for a new event.
// Lachesis: frame(e) = max(frame(p) for p in parents_strongly_seen_by_e) + 1
// If e strongly sees roots of frame f-1, then frame(e) = f.
// This is a simplified version.
func (mn *ManagedNode) calculateNextFrame(selfParentEvent *dag.Event, otherParentEvents []*dag.Event) uint64 {
	maxParentFrame := uint64(0) // Lachesis frames are 1-indexed. 0 means no parents.

	if selfParentEvent != nil {
		if selfParentEvent.EventData.Frame > maxParentFrame {
			maxParentFrame = selfParentEvent.EventData.Frame
		}
	}
	for _, op := range otherParentEvents {
		if op != nil && op.EventData.Frame > maxParentFrame {
			maxParentFrame = op.EventData.Frame
		}
	}

	// Basic Lachesis rule: if an event strongly sees roots that form a quorum for frame F,
	// then this event's frame is F+1.
	// This simplified version just increments the max parent frame.
	// A full implementation needs to check for "strongly seeing a quorum of roots of frame F".
	if maxParentFrame == 0 { // This is the very first event by this creator in the DAG (no self-parent)
		return 1 // Start with frame 1
	}
	return maxParentFrame + 1 // Sửa lỗi logic: Frame mới phải lớn hơn frame của parent
}

// checkIfRoot determines if a new event is a Root.
// Lachesis: An event `e` is a root if frame(e) > frame(p) for all parents `p` that `e` strongly sees.
// Or, more simply, if it's the first event by its creator in its frame.
// This is a simplified version.
func (mn *ManagedNode) checkIfRoot(newEventFrame uint64, selfParentEvent *dag.Event, otherParentEvents []*dag.Event) bool {
	if selfParentEvent == nil { // First event by this creator
		return true
	}
	// If the new event's frame is greater than its self-parent's frame, it's a root for the new frame.
	if newEventFrame > selfParentEvent.EventData.Frame {
		return true
	}

	// A more detailed check: if newEventFrame is greater than ALL its parents' frames.
	// (This is still simpler than full "strongly see" logic for root determination)
	allParentsOlderFrame := true
	if selfParentEvent != nil && newEventFrame <= selfParentEvent.EventData.Frame {
		allParentsOlderFrame = false
	}
	if allParentsOlderFrame { // Only check other parents if self-parent already satisfies
		for _, op := range otherParentEvents {
			if op != nil && newEventFrame <= op.EventData.Frame {
				allParentsOlderFrame = false
				break
			}
		}
	}

	if allParentsOlderFrame && newEventFrame > 0 { // Ensure frame is positive
		return true
	}

	// If it's in the same frame as its self-parent, it's not a root by that definition.
	// The Lachesis definition is more nuanced and involves "strongly seeing" previous roots.
	return false
}

// consensusLoop is the main loop that drives the consensus process.
func (mn *ManagedNode) consensusLoop() {
	defer mn.wg.Done()

	consensusTickInterval := mn.config.ConsensusTickInterval
	if consensusTickInterval <= 0 {
		consensusTickInterval = 10 * time.Second
		log.Printf("ConsensusTickInterval not configured or invalid, using default: %v", consensusTickInterval)
	}
	logger.Info("Consensus tick interval set to: ", consensusTickInterval)
	ticker := time.NewTicker(consensusTickInterval)
	defer ticker.Stop()

	log.Println("Starting main consensus loop...")

	for {
		select {
		case <-mn.ctx.Done():
			log.Println("Stopping consensus loop due to node context cancellation.")
			return
		case <-ticker.C:
			// log.Println("--- New Consensus Round ---")
			// mn.dagStore.PrintDagStoreStatus() // Can be very verbose
			mn.dagStore.PrintDagStoreStatus() //
			// Step 1: Attempt to finalize existing events.
			// log.Printf("CONSENSUS_LOOP: Running DecideClotho(). DagStore's LastDecidedFrame: %d", mn.dagStore.GetLastDecidedFrame())
			mn.dagStore.DecideClotho() //
			// logger.Info("CONSENSUS_LOOP: DecideClotho() finished.")

			currentDagLastDecidedFrame := mn.dagStore.GetLastDecidedFrame() //
			// log.Printf("CONSENSUS_LOOP: After DecideClotho(), DagStore's LastDecidedFrame: %d. Previously processed finalized frame: %d", currentDagLastDecidedFrame, mn.lastProcessedFinalizedFrame)

			var newlyFinalizedRoots []*dag.Event
			if currentDagLastDecidedFrame > mn.lastProcessedFinalizedFrame {
				for frameToProcess := mn.lastProcessedFinalizedFrame + 1; frameToProcess <= currentDagLastDecidedFrame; frameToProcess++ {
					rootsInFrame := mn.dagStore.GetRoots(frameToProcess) //
					for _, rootID := range rootsInFrame {
						rootEvent, exists := mn.dagStore.GetEvent(rootID)                                         //
						if exists && rootEvent.EventData.IsRoot && rootEvent.ClothoStatus == dag.ClothoIsClotho { //
							newlyFinalizedRoots = append(newlyFinalizedRoots, rootEvent)
						}
					}
				}
			}

			madeProgressThisRound := len(newlyFinalizedRoots) > 0
			// logger.Error("madeProgressThisRound", madeProgressThisRound) // Gỡ lỗi
			if madeProgressThisRound {
				// log.Printf("CONSENSUS_LOOP: Found %d new Root(s) decided as Clotho to process.", len(newlyFinalizedRoots))
				sort.Slice(newlyFinalizedRoots, func(i, j int) bool { //
					if newlyFinalizedRoots[i].EventData.Frame != newlyFinalizedRoots[j].EventData.Frame { //
						return newlyFinalizedRoots[i].EventData.Frame < newlyFinalizedRoots[j].EventData.Frame //
					}
					if newlyFinalizedRoots[i].EventData.Timestamp != newlyFinalizedRoots[j].EventData.Timestamp { //
						return newlyFinalizedRoots[i].EventData.Timestamp < newlyFinalizedRoots[j].EventData.Timestamp //
					}
					return newlyFinalizedRoots[i].GetEventId().String() < newlyFinalizedRoots[j].GetEventId().String() //
				})

				// log.Println("CONSENSUS_LOOP: Finalized (Clotho) Roots in processing order:")
				maxProcessedFrameThisRound := mn.lastProcessedFinalizedFrame
				for _, finalizedRoot := range newlyFinalizedRoots {
					logger.Info(fmt.Sprintf("  Processing Finalized Root: ID %s, Frame %d, Creator %s..., TxLen: %d bytes",
						finalizedRoot.GetEventId().Short(),                      //
						finalizedRoot.EventData.Frame,                           //
						hex.EncodeToString(finalizedRoot.EventData.Creator)[:6], //
						len(finalizedRoot.EventData.Transactions)))              //
					// Application-specific processing of finalizedRoot.EventData.Transactions here.
					if finalizedRoot.EventData.Frame > maxProcessedFrameThisRound { //
						maxProcessedFrameThisRound = finalizedRoot.EventData.Frame //
					}
				}
				mn.lastProcessedFinalizedFrame = maxProcessedFrameThisRound
				// log.Printf("CONSENSUS_LOOP: Updated lastProcessedFinalizedFrame to %d", mn.lastProcessedFinalizedFrame)
				// logger.Error("lastProcessedFinalizedFrame", mn.lastProcessedFinalizedFrame) // Gỡ lỗi
				if mn.lastProcessedFinalizedFrame >= MIN_FRAMES_BEFORE_PRUNING { //
					oldestFrameToKeep := uint64(1)
					if mn.lastProcessedFinalizedFrame > FRAMES_TO_KEEP_AFTER_FINALIZED { // // Đảm bảo không trừ về 0 hoặc âm
						oldestFrameToKeep = mn.lastProcessedFinalizedFrame - FRAMES_TO_KEEP_AFTER_FINALIZED + 1 //
					} else if mn.lastProcessedFinalizedFrame >= FRAMES_TO_KEEP_AFTER_FINALIZED { // // Nếu vừa bằng
						oldestFrameToKeep = 1
					}

					// log.Printf("CONSENSUS_LOOP: Calling PruneOldEvents with oldestFrameToKeep = %d", oldestFrameToKeep)
					// logger.Error("PruneOldEvents") // Gỡ lỗi
					mn.dagStore.PruneOldEvents(oldestFrameToKeep) //
				}
			} else { // No new finalized roots, proceed to create a new event.
				// log.Printf("CONSENSUS_LOOP: No new Roots were finalized (Clotho). Proceeding to create new event and synchronize.")

				// *** BEGIN MODIFICATION ***
				// Check if there are too many undecided frames before creating a new event
				lastDecidedFrame := mn.dagStore.GetLastDecidedFrame()   //
				maxFrameWithRoots := mn.dagStore.GetMaxFrameWithRoots() // This is the new method

				if maxFrameWithRoots > lastDecidedFrame && (maxFrameWithRoots-lastDecidedFrame) > MAX_UNDECIDED_FRAME_DIFFERENCE { //
					logger.Warn(fmt.Sprintf("CONSENSUS_LOOP: Pausing event creation. Too many undecided frames. LastDecided: %d, MaxWithRoots: %d, Diff: %d, Threshold: %d",
						lastDecidedFrame, maxFrameWithRoots, (maxFrameWithRoots - lastDecidedFrame), MAX_UNDECIDED_FRAME_DIFFERENCE)) //
					goto endOfConsensusRound // Skip event creation for this tick
				}
				// *** END MODIFICATION ***

				reqCtx, cancelReq := context.WithTimeout(mn.ctx, 5*time.Second)                                                     // Shorter timeout for TX request //
				requestPayload := []byte(fmt.Sprintf(`{"action": "get_pending_transactions", "timestamp": %d}`, time.Now().Unix())) //
				// log.Printf("CONSENSUS_LOOP: Sending TransactionsRequestProtocol to Master Node...")
				responseData, err := mn.SendRequestToMasterNode(reqCtx, TransactionsRequestProtocol, requestPayload) //
				cancelReq()                                                                                          //

				var transactionsForNewBlock []byte
				if err != nil {
					// log.Printf("CONSENSUS_LOOP: Error requesting transactions from Master Node: %v. Creating event without new transactions.", err)
					transactionsForNewBlock = []byte{}
				} else {
					// log.Printf("CONSENSUS_LOOP: Received transaction response from Master Node (%d bytes).", len(responseData))
					var parseErr error
					transactionsForNewBlock, parseErr = parseTransactionsFromResponse(responseData) // Assume this exists and works //
					if parseErr != nil {
						log.Printf("CONSENSUS_LOOP: Error parsing transactions from Master Node: %v. Creating event without these transactions.", parseErr)
						transactionsForNewBlock = []byte{}
					}
				}

				ownPubKeyHex, err := mn.getOwnPublicKeyHex() //
				if err != nil {
					log.Printf("CONSENSUS_LOOP: CRITICAL error getting own public key: %v. Skipping event creation.", err)
					goto endOfConsensusRound
				}
				ownPubKeyBytes, err := hex.DecodeString(ownPubKeyHex) // Re-decode, ensure consistency //
				if err != nil {
					log.Printf("CONSENSUS_LOOP: CRITICAL error converting own public key hex to bytes: %v. Skipping event creation.", err)
					goto endOfConsensusRound
				}

				latestSelfEventID, selfEventExists := mn.dagStore.GetLatestEventIDByCreatorPubKeyHex(ownPubKeyHex) //
				var selfParentEvent *dag.Event
				var newEventIndex uint64 = 1

				if selfEventExists {
					var ok bool
					selfParentEvent, ok = mn.dagStore.GetEvent(latestSelfEventID) //
					if !ok {
						log.Printf("CONSENSUS_LOOP: Error: Latest self event ID %s exists but event not found in store. Skipping event creation.", latestSelfEventID.Short()) //
						goto endOfConsensusRound
					}
					newEventIndex = selfParentEvent.EventData.Index + 1 //
				} else {
					// log.Printf("CONSENSUS_LOOP: No previous event found for node %s. This will be its first event (Index %d).", ownPubKeyHex[:6], newEventIndex)
					latestSelfEventID = dag.EventID{} //
				}

				// Select OtherParents
				selectedOtherParentIDs := mn.selectOtherParents(ownPubKeyHex) // Sử dụng hàm đã cập nhật //
				var otherParentEventsForMeta []*dag.Event
				for _, opID := range selectedOtherParentIDs {
					if opEvent, ok := mn.dagStore.GetEvent(opID); ok { //
						otherParentEventsForMeta = append(otherParentEventsForMeta, opEvent)
					}
				}

				newEventFrame := mn.calculateNextFrame(selfParentEvent, otherParentEventsForMeta)          //
				newEventIsRoot := mn.checkIfRoot(newEventFrame, selfParentEvent, otherParentEventsForMeta) //

				newEventData := dag.EventData{ //
					Transactions: transactionsForNewBlock, //
					SelfParent:   latestSelfEventID,       //
					OtherParents: selectedOtherParentIDs,  // Use selected diverse parents //
					Creator:      ownPubKeyBytes,          //
					Index:        newEventIndex,           //
					Timestamp:    time.Now().UnixNano(),   // Higher precision timestamp //
					Frame:        newEventFrame,           //
					IsRoot:       newEventIsRoot,          //
				}

				signature, signErr := signEventData(newEventData, mn.privKey) // Assume this exists //
				if signErr != nil {
					log.Printf("CONSENSUS_LOOP: Error signing event data: %v. Skipping event creation.", signErr)
					goto endOfConsensusRound
				}
				newEvent := dag.NewEvent(newEventData, signature) //
				newEventID := newEvent.GetEventId()               // Get ID after NewEvent calculates it //

				if addEventErr := mn.dagStore.AddEvent(newEvent); addEventErr != nil { //
					log.Printf("CONSENSUS_LOOP: Error adding new local event %s to DagStore: %v", newEventID.Short(), addEventErr) //
					goto endOfConsensusRound
				}
				// log.Printf("CONSENSUS_LOOP: Created and added new local event: ID %s, Idx %d, F %d, Root %t, TxLen: %d, OPs: %d",
				// 	newEventID.Short(), newEvent.EventData.Index, newEvent.EventData.Frame, newEvent.EventData.IsRoot, len(newEvent.EventData.Transactions), len(selectedOtherParentIDs))

				// Add to recent buffer and gossip
				mn.addEventToRecentBuffer(newEvent) //
				mn.gossipEvent(newEvent)            // New function to broadcast the event //

				// Step 3: Synchronize with a few peers (not just one partner)
				// This is a simplified sync, could be more targeted.
				mn.triggerPeerSynchronization() //
			}
		endOfConsensusRound:
			// mn.dagStore.PrintDagStoreStatus() // For debugging, can be verbose
			// log.Println("--- End of Consensus Round ---")
			_ = madeProgressThisRound // Avoid unused variable warning if logs are commented out
		}
	}
}

// gossipEvent broadcasts a newly created event to the network via PubSub.
func (mn *ManagedNode) gossipEvent(event *dag.Event) {
	if event == nil {
		return
	}
	eventBytes, err := event.Marshal()
	if err != nil {
		log.Printf("Error marshaling event %s for gossip: %v", event.GetEventId().Short(), err)
		return
	}

	const EVENT_TOPIC_NAME = "consensus/events/v1" // Must match subscription
	err = mn.PublishMessage(EVENT_TOPIC_NAME, eventBytes)
	if err != nil {
		log.Printf("Error gossiping event %s to topic %s: %v", event.GetEventId().Short(), EVENT_TOPIC_NAME, err)
	} else {
		// log.Printf("Gossiped event %s to topic %s", event.GetEventId().Short(), EVENT_TOPIC_NAME)
	}
}

// triggerPeerSynchronization initiates synchronization with a few connected peers.
func (mn *ManagedNode) triggerPeerSynchronization() {
	connectedPeers := mn.Host().Network().Peers()
	if len(connectedPeers) == 0 {
		// log.Println("SYNC_TRIGGER: No connected peers to synchronize with.")
		return
	}

	// Select a few random peers to sync with (e.g., up to 2-3)
	//nolint:gosec
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	rng.Shuffle(len(connectedPeers), func(i, j int) {
		connectedPeers[i], connectedPeers[j] = connectedPeers[j], connectedPeers[i]
	})

	numPeersToSync := 0
	maxSyncPeers := 2 // Configure this as needed

	for _, peerID := range connectedPeers {
		if peerID == mn.Host().ID() { // Don't sync with self
			continue
		}
		if numPeersToSync >= maxSyncPeers {
			break
		}

		// Get partner NodeID (PubKeyHex)
		mn.peerPubKeyMutex.RLock()
		partnerNodeID, ok := mn.connectedPeerPubKeys[peerID]
		mn.peerPubKeyMutex.RUnlock()

		if !ok {
			// log.Printf("SYNC_TRIGGER: No PubKeyHex for peer %s, cannot determine partnerNodeID for sync.", peerID.String())
			continue
		}

		// log.Printf("SYNC_TRIGGER: Requesting sync with peer %s (NodeID %s...)", peerID.String(), partnerNodeID[:6])
		go mn.requestSyncWithPeer(mn.ctx, peerID, partnerNodeID) // Run in goroutine to not block consensusLoop
		numPeersToSync++
	}
	// if numPeersToSync > 0 {
	// log.Printf("SYNC_TRIGGER: Initiated synchronization with %d peer(s).", numPeersToSync)
	// }
}

// setupConnectionNotifier configures handlers for network connection events.
func (mn *ManagedNode) setupConnectionNotifier() {
	mn.host.Network().Notify(&network.NotifyBundle{
		ConnectedF: func(net network.Network, conn network.Conn) {
			peerID := conn.RemotePeer()
			// log.Printf("✅ Connected to peer: %s (Address: %s)", peerID, conn.RemoteMultiaddr())
			// logger.Error("setupConnectionNotifier - Connected to PeerID: ", peerID) // Original log

			pubKey := conn.RemotePublicKey()
			if pubKey == nil {
				log.Printf("Warning: Could not retrieve public key for peer %s from connection.", peerID)
			} else {
				rawPubKey, err := pubKey.Raw()
				if err != nil {
					log.Printf("Warning: Could not get raw public key for peer %s: %v", peerID, err)
				} else {
					pubKeyHex := hex.EncodeToString(rawPubKey)
					mn.peerPubKeyMutex.Lock()
					mn.connectedPeerPubKeys[peerID] = pubKeyHex
					mn.peerPubKeyMutex.Unlock()
					// logger.Info("setupConnectionNotifier - Stored public keys: ", mn.connectedPeerPubKeys)
					// log.Printf("Stored public key hex '%s...' for peer %s", pubKeyHex[:min(10, len(pubKeyHex))], peerID)
				}
			}

			mn.peerMutex.RLock()
			pInfo, exists := mn.peers[peerID]
			var peerType string
			if exists {
				peerType = pInfo.Type
			} else {
				peerType = "unknown_inbound"
			}
			mn.peerMutex.RUnlock()

			mn.updatePeerStatus(peerID, PeerConnected, nil, peerType)
			mn.host.Peerstore().AddAddr(peerID, conn.RemoteMultiaddr(), peerstore.ConnectedAddrTTL)
		},
		DisconnectedF: func(net network.Network, conn network.Conn) {
			peerID := conn.RemotePeer()
			// log.Printf("❌ Disconnected from peer: %s", peerID)

			mn.peerPubKeyMutex.Lock()
			delete(mn.connectedPeerPubKeys, peerID)
			mn.peerPubKeyMutex.Unlock()
			// log.Printf("Removed stored public key for peer %s due to disconnection.", peerID)

			mn.peerMutex.RLock()
			pInfo, exists := mn.peers[peerID]
			var peerType string
			if exists {
				peerType = pInfo.Type
			} else {
				peerType = "unknown_disconnected"
			}
			mn.peerMutex.RUnlock()

			mn.updatePeerStatus(peerID, PeerDisconnected, errors.New("disconnected"), peerType)

			if exists && shouldReconnect(pInfo.Type, mn.config) {
				// log.Printf("Scheduled reconnection attempt for important peer %s (Type: %s)", peerID, pInfo.Type)
				mn.tryReconnectToPeer(peerID, pInfo.Type)
			}
		},
	})
}

// min is a helper function to find the minimum of two integers.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// parseTransactionsFromResponse is a placeholder.
func parseTransactionsFromResponse(responseData []byte) ([]byte, error) {
	if responseData == nil {
		return []byte{}, nil
	}
	// log.Printf("parseTransactionsFromResponse: Received %d bytes of transaction data.", len(responseData))
	return responseData, nil // Assume direct payload for now
}

// signEventData signs the hash of the EventData.
func signEventData(eventData dag.EventData, privKey crypto.PrivKey) ([]byte, error) {
	if privKey == nil {
		return nil, errors.New("private key is nil, cannot sign event data")
	}
	tempEventForHashing := &dag.Event{EventData: eventData} // EventData is embedded

	// Ensure OtherParents are sorted before hashing for signature, if not already done by EventData creation
	// eventData.SortOtherParents() // Assuming EventData has such a method or it's done prior

	hashToSign, err := tempEventForHashing.Hash() // Hash() is from dag/event.go
	if err != nil {
		return nil, fmt.Errorf("failed to hash event data for signing: %w", err)
	}

	dataBytes := hashToSign.Bytes()
	// log.Printf("Signing hash of EventData: %s", hashToSign.String())
	signature, err := privKey.Sign(dataBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to sign event data hash: %w", err)
	}
	// log.Printf("Successfully signed event data hash. Signature length: %d", len(signature))
	return signature, nil
}

// requestSyncWithPeer initiates a synchronization process with a specific peer.
func (mn *ManagedNode) requestSyncWithPeer(ctx context.Context, partnerPeerID peer.ID, partnerNodeID string) {
	// log.Printf("SYNC_REQUEST: Attempting to start synchronization with partner %s (PeerID: %s)", partnerNodeID, partnerPeerID)

	ownPubKeyHex, err := mn.getOwnPublicKeyHex()
	if err != nil {
		log.Printf("SYNC_REQUEST: Error getting own public key for sync request to %s: %v", partnerPeerID, err)
		return
	}
	if ownPubKeyHex == "" { // Should be caught by err above
		log.Printf("SYNC_REQUEST: Own public key is empty, cannot send sync request to %s.", partnerPeerID)
		return
	}
	if partnerPeerID == mn.host.ID() {
		// log.Printf("SYNC_REQUEST: Skipping synchronization with self (PeerID: %s)", partnerPeerID)
		return
	}

	knownMaxIndices := make(map[string]uint64)
	latestEventsMap := mn.dagStore.GetLatestEventsMapSnapshot()

	for creatorPubKeyHex, latestEventID := range latestEventsMap {
		if latestEventID.IsZero() {
			continue
		}
		latestEvent, exists := mn.dagStore.GetEvent(latestEventID)
		if exists {
			knownMaxIndices[creatorPubKeyHex] = latestEvent.EventData.Index
		} else {
			// log.Printf("SYNC_REQUEST: Warning - Latest event %s for creator %s not found in DagStore for KnownMaxIndices.",
			// 	latestEventID.Short(), creatorPubKeyHex[:6])
		}
	}
	// log.Printf("SYNC_REQUEST: Prepared KnownMaxIndices to send to %s: %d items", partnerPeerID, len(knownMaxIndices))

	syncPayload := SyncRequestPayload{
		Action:          "request_events_based_on_indices",
		RequesterNodeID: ownPubKeyHex,
		KnownMaxIndices: knownMaxIndices,
	}

	requestPayloadBytes, err := json.Marshal(syncPayload)
	if err != nil {
		log.Printf("SYNC_REQUEST: Error marshaling SyncRequestPayload for partner %s: %v", partnerPeerID, err)
		return
	}

	reqCtx, cancelReq := context.WithTimeout(ctx, 30*time.Second)
	defer cancelReq()

	// log.Printf("SYNC_REQUEST: Sending SyncRequestProtocol to %s (PeerID: %s). Payload size: %d bytes.", partnerNodeID, partnerPeerID, len(requestPayloadBytes))
	responseData, err := mn.SendRequest(reqCtx, partnerPeerID, SyncRequestProtocol, requestPayloadBytes)

	if err != nil {
		// log.Printf("SYNC_REQUEST: Error sending sync request to partner %s (PeerID: %s): %v", partnerNodeID, partnerPeerID, err)
		return
	}

	// log.Printf("SYNC_REQUEST: Received sync response from partner %s (PeerID: %s). Size: %d bytes.", partnerNodeID, partnerPeerID, len(responseData))
	var syncResponse SyncResponsePayload
	if err := json.Unmarshal(responseData, &syncResponse); err != nil {
		log.Printf("SYNC_REQUEST: Error unmarshaling sync response from %s: %v", partnerPeerID, err)
		return
	}

	if len(syncResponse.Events) == 0 {
		// log.Printf("SYNC_REQUEST: Partner %s sent no new events.", partnerPeerID)
	} else {
		// log.Printf("SYNC_REQUEST: Received %d event(s) from partner %s. Processing...", len(syncResponse.Events), partnerPeerID)
		addedCount := 0
		for i, eventBytes := range syncResponse.Events {
			event, errUnmarshal := dag.Unmarshal(eventBytes)
			if errUnmarshal != nil {
				log.Printf("SYNC_REQUEST: Error unmarshaling event %d from partner %s: %v. Skipping.", i+1, partnerPeerID, errUnmarshal)
				continue
			}

			// Basic validation could be added here (e.g. signature, structure)

			if errAdd := mn.dagStore.AddEvent(event); errAdd != nil {
				if !strings.Contains(errAdd.Error(), "already exists") {
					log.Printf("SYNC_REQUEST: Error adding event %s (Idx: %d, C: %s...) from partner %s to DagStore: %v",
						event.GetEventId().Short(), event.EventData.Index, hex.EncodeToString(event.EventData.Creator)[:6], partnerPeerID, errAdd)
				}
			} else {
				addedCount++
				// log.Printf("SYNC_REQUEST: Added event %s (Idx: %d, C: %s...) from partner %s.",
				// 	event.GetEventId().Short(), event.EventData.Index, hex.EncodeToString(event.EventData.Creator)[:6], partnerPeerID)
				mn.addEventToRecentBuffer(event) // Add to recent buffer as well
			}
		}
		// log.Printf("SYNC_REQUEST: Added %d/%d events received from partner %s to DagStore.", addedCount, len(syncResponse.Events), partnerPeerID)
	}

	if len(syncResponse.PartnerLatestIndices) > 0 {
		// log.Printf("SYNC_REQUEST: Partner %s also sent their latest indices (%d items). (Currently not further processed).",
		// 	partnerPeerID, len(syncResponse.PartnerLatestIndices))
	}
}
