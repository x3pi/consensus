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
// It should define methods for getting, putting, and deleting data.
type YourStorageInterface interface {
	Get(key []byte) ([]byte, error)
	Put(key []byte, value []byte) error
	Delete(key []byte) error
	// Add other necessary storage methods here.
}

// ManagedNode is the central object for network operations and consensus.
type ManagedNode struct {
	config       NodeConfig
	host         host.Host
	privKey      crypto.PrivKey // Private key of the node.
	ownPubKeyHex string         // Hex string of this node's public key.
	pubsub       *pubsub.PubSub
	dagStore     *dag.DagStore // Instance of DagStore.

	// topicStorageMap maps topic names (string) to YourStorageInterface instances.
	topicStorageMap sync.Map

	peers       map[peer.ID]*ManagedPeerInfo // Information about known/connected peers.
	peerMutex   sync.RWMutex                 // Mutex for peers map.
	reconnectWG sync.WaitGroup               // WaitGroup for peer reconnection goroutines.

	ctx        context.Context    // Node's context, used to cancel goroutines.
	cancelFunc context.CancelFunc // Function to cancel the node's context.
	wg         sync.WaitGroup     // Manages main operational goroutines.

	// streamHandlers maps protocol.IDs to their respective network.StreamHandler.
	streamHandlers map[protocol.ID]network.StreamHandler

	// topicSubscriptions maps topic names to their pubsub.Subscription objects.
	topicSubscriptions map[string]*pubsub.Subscription
	// topicHandlers maps topic names to their message handler functions.
	topicHandlers map[string]func(msg *pubsub.Message)

	// Other internal states.
	keyValueCache *lru.Cache[string, []byte] // LRU cache for key-value data.
	// transactionChan is a channel for incoming transactions.
	transactionChan chan []byte
	// fetchingBlocks tracks blocks currently being fetched (key: blockNumber uint64, value: bool).
	fetchingBlocks sync.Map
	// feeAddresses stores a list of fee addresses.
	feeAddresses    []string
	feeAddressesMux sync.RWMutex // Mutex for feeAddresses.
	// lastProcessedFinalizedFrame tracks the last frame for which finalized Roots were processed.
	lastProcessedFinalizedFrame uint64

	// connectedPeerPubKeys stores the public key (hex string) of connected peers.
	// Key: peer.ID, Value: public key hex string.
	connectedPeerPubKeys map[peer.ID]string
	peerPubKeyMutex      sync.RWMutex // Mutex for connectedPeerPubKeys.
}

const (
	// FRAMES_TO_KEEP_AFTER_FINALIZED defines how many recent finalized frames to keep.
	// Frames older than (lastProcessedFinalizedFrame - FRAMES_TO_KEEP_AFTER_FINALIZED) will be pruned.
	FRAMES_TO_KEEP_AFTER_FINALIZED uint64 = 5
	// MIN_FRAMES_BEFORE_PRUNING is the minimum number of frames that must be processed before pruning begins.
	MIN_FRAMES_BEFORE_PRUNING uint64 = FRAMES_TO_KEEP_AFTER_FINALIZED + 5
)

// NewManagedNode creates and initializes a new ManagedNode.
func NewManagedNode(ctx context.Context, cfg NodeConfig) (*ManagedNode, error) {
	privKey, err := loadPrivateKey(cfg.PrivateKey) // From utils.go
	if err != nil {
		return nil, fmt.Errorf("failed to load private key: %w", err)
	}

	var ownPubKeyHex string
	if privKey != nil {
		pubKey := privKey.GetPublic()
		rawPubKeyBytes, extractionError := pubKey.Raw()

		if extractionError != nil {
			log.Printf("Warning: Could not get raw public key bytes using pubKey.Raw(): %v. Attempting with specific key type.", extractionError)
			// Attempt to extract key assuming a specific type, e.g., Secp256k1, if the generic Raw() fails.
			// This part might need adjustment based on the actual private key types in use.
			if ecdsaPriv, ok := privKey.(*crypto.Secp256k1PrivateKey); ok {
				log.Println("Private key is of type Secp256k1PrivateKey.")
				ecdsaTypedPubKey := ecdsaPriv.GetPublic().(*crypto.Secp256k1PublicKey)
				ecdsaBytes, ecdsaErr := ecdsaTypedPubKey.Raw()
				if ecdsaErr == nil {
					log.Println("Successfully retrieved raw public key using ecdsaTypedPubKey.Raw().")
					rawPubKeyBytes = ecdsaBytes
					extractionError = nil // Clear previous error
				} else {
					log.Printf("Failed to get raw public key from ecdsaTypedPubKey.Raw(): %v", ecdsaErr)
					extractionError = ecdsaErr // Keep the specific error
				}
			} else {
				log.Printf("Private key is not of a known specific type (e.g., Secp256k1PrivateKey). Initial error from pubKey.Raw() is kept.")
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
	}
	ps, err := pubsub.NewGossipSub(ctx, libp2pHost, pubsubOptions...)
	if err != nil {
		_ = libp2pHost.Close() // Attempt to close host if pubsub creation fails.
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
	if ownPubKeyHex != "" && cfg.InitialStake > 0 {
		initialStakeData[ownPubKeyHex] = cfg.InitialStake
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
		lastProcessedFinalizedFrame: 0, // Initialize to 0 (or your starting frame - 1).
	}

	// Register stream handlers if this node is a consensus node.
	// This ensures that the target node (e.g., one with PeerID 12D3KooWJ2s3WiamQxiHK43yU3fsTHrgMLNteW1Hr5RpqcKdZUzu)
	// has mn.config.NodeType == "consensus" for this code block to execute for it.
	if mn.config.NodeType == "consensus" {
		// Register handler for transaction requests.
		mn.RegisterStreamHandler(TransactionsRequestProtocol, mn.transactionsRequestHandler)
		log.Printf("Registered stream handler for %s (Node Type: %s)", TransactionsRequestProtocol, mn.config.NodeType)

		// Register handler for synchronization requests.
		// THIS IS THE HANDLER FOR THE PROTOCOL THAT MIGHT CAUSE ISSUES IF NOT SET UP CORRECTLY.
		mn.RegisterStreamHandler(SyncRequestProtocol, mn.syncRequestHandler)
		log.Printf("Registered stream handler for %s (Node Type: %s)", SyncRequestProtocol, mn.config.NodeType)
	}

	mn.displayNodeInfo() // From utils.go

	return mn, nil
}

// Start initiates the node's operations, including connecting to peers and starting services.
func (mn *ManagedNode) Start() error {
	// Set up stream handlers registered during NewManagedNode.
	for protoID, handler := range mn.streamHandlers {
		mn.host.SetStreamHandler(protoID, handler)
		log.Printf("Set stream handler for protocol %s on host.", protoID)
	}

	if err := mn.connectToBootstrapPeers(); err != nil { // From peer_manager.go
		log.Printf("NOTE: Failed to connect to some bootstrap peers: %v", err)
		// Depending on requirements, this might not be a fatal error.
	}

	mn.wg.Add(1)
	go mn.peerHealthMonitor() // From peer_manager.go

	mn.setupConnectionNotifier() // Sets up handlers for peer connect/disconnect events.

	log.Printf("ManagedNode (%s) started successfully.", mn.host.ID())

	if mn.config.NodeType == "consensus" {
		mn.wg.Add(1)
		go mn.consensusLoop() // Starts the main consensus logic loop.
	}
	return nil
}

// Stop gracefully shuts down the ManagedNode.
func (mn *ManagedNode) Stop() error {
	log.Println("Stopping ManagedNode...")
	mn.cancelFunc() // Signal all node-scoped goroutines to stop.

	mn.cancelAllReconnects() // From peer_manager.go
	mn.reconnectWG.Wait()    // Wait for any active reconnection attempts to finish.

	if err := mn.host.Close(); err != nil {
		log.Printf("Error closing libp2p host: %v", err)
		// Continue with other shutdown tasks even if host closing fails.
	}

	mn.wg.Wait() // Wait for all main operational goroutines to complete.
	log.Println("ManagedNode stopped.")
	return nil
}

// Host returns the libp2p host instance.
func (mn *ManagedNode) Host() host.Host {
	return mn.host
}

// PubSubInstance returns the PubSub instance.
func (mn *ManagedNode) PubSubInstance() *pubsub.PubSub {
	return mn.pubsub
}

// Context returns the node's context.
func (mn *ManagedNode) Context() context.Context {
	return mn.ctx
}

// Config returns the node's configuration.
func (mn *ManagedNode) Config() NodeConfig {
	return mn.config
}

// getOwnPublicKeyHex returns the hex-encoded public key of the current node.
// It attempts to derive it from the private key if not already cached.
func (mn *ManagedNode) getOwnPublicKeyHex() (string, error) {
	if mn.ownPubKeyHex == "" {
		if mn.privKey != nil {
			pubKey := mn.privKey.GetPublic()
			var tempPubKeyBytes []byte
			var tempErr error
			// Attempt generic Raw() first.
			tempPubKeyBytes, tempErr = pubKey.Raw()
			if tempErr != nil {
				// If generic fails, try specific types (e.g., Secp256k1).
				if ecdsaPriv, ok := mn.privKey.(*crypto.Secp256k1PrivateKey); ok {
					ecdsaTypedPubKey := ecdsaPriv.GetPublic().(*crypto.Secp256k1PublicKey)
					tempPubKeyBytes, tempErr = ecdsaTypedPubKey.Raw()
				}
				// Add other key type checks if necessary.
			}

			if tempErr == nil && tempPubKeyBytes != nil {
				mn.ownPubKeyHex = hex.EncodeToString(tempPubKeyBytes)
			} else {
				return "", fmt.Errorf("failed to re-derive node's own public key: %v", tempErr)
			}

			if mn.ownPubKeyHex == "" { // Should not happen if derivation was successful.
				return "", errors.New("ownPubKeyHex is still empty after attempting re-derivation in getOwnPublicKeyHex")
			}
		} else {
			return "", errors.New("private key is nil, cannot derive public key in getOwnPublicKeyHex")
		}
	}
	return mn.ownPubKeyHex, nil
}

// getConnectedPeerPublicKeys returns a map of connected peer.IDs to their public key hex strings.
func (mn *ManagedNode) getConnectedPeerPublicKeys() (map[peer.ID]string, error) {
	mn.peerPubKeyMutex.RLock()
	defer mn.peerPubKeyMutex.RUnlock()

	// logger.Error is used here in the original code. If this is for debugging, consider logger.Debug.
	// For now, keeping it as per original to not change "logic" which might include debug logging levels.
	logger.Error("getConnectedPeerPublicKeys - current connectedPeerPubKeys map: ", mn.connectedPeerPubKeys)

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

// selectConsensusPartner randomly selects a connected peer as a partner for synchronization and event creation.
// It excludes the node itself and any peers designated as "master".
func (mn *ManagedNode) selectConsensusPartner() (NodeID, peer.ID, error) {
	ownKeyHex, err := mn.getOwnPublicKeyHex()
	if err != nil {
		return "", "", fmt.Errorf("failed to get own public key for partner selection: %w", err)
	}
	if ownKeyHex == "" { // Should be caught by the error above, but as a safeguard.
		return "", "", errors.New("own public key is empty, cannot select consensus partner")
	}

	connectedPeersMap, err := mn.getConnectedPeerPublicKeys()
	if err != nil {
		return "", "", fmt.Errorf("failed to get connected peer public keys for partner selection: %w", err)
	}
	logger.Info("selectConsensusPartner - Connected peers with known public keys: ", connectedPeersMap)

	if len(connectedPeersMap) == 0 {
		log.Println("No connected peers with known public keys available for partner selection.")
		return "", "", errors.New("no connected peers with known public keys for selection")
	}

	var candidateNodeIDs []NodeID
	nodeIDToPeerIDMap := make(map[NodeID]peer.ID)

	mn.peerMutex.RLock() // Lock to safely access mn.peers
	for pID, peerPubKeyHex := range connectedPeersMap {
		if peerPubKeyHex == ownKeyHex { // Do not select self.
			continue
		}

		peerInfo, exists := mn.peers[pID]
		if !exists {
			log.Printf("Warning: Peer %s has a public key but no detailed info in mn.peers. Skipping for partner selection.", pID)
			continue
		}

		// Exclude "master" nodes from being consensus partners.
		if peerInfo.Type == "master" {
			log.Printf("selectConsensusPartner: Skipping peer %s (Type: %s) as it is a master node.", pID, peerInfo.Type)
			continue
		}

		// Assuming NodeID is derived from the public key hex string.
		currentNodeID := GetNodeIDFromString(peerPubKeyHex) // From node_selection.go
		candidateNodeIDs = append(candidateNodeIDs, currentNodeID)
		nodeIDToPeerIDMap[currentNodeID] = pID
	}
	mn.peerMutex.RUnlock()

	if len(candidateNodeIDs) == 0 {
		log.Println("No valid candidates for consensus partner after filtering (e.g., all were self or master nodes, or no other peers).")
		return "", "", errors.New("no valid candidates after filtering")
	}

	//nolint:gosec // Using math/rand for non-cryptographic random selection is acceptable here.
	source := rand.NewSource(time.Now().UnixNano())
	randomGenerator := rand.New(source)
	randomIndex := randomGenerator.Intn(len(candidateNodeIDs))
	selectedNodeID := candidateNodeIDs[randomIndex]

	selectedPID, pidExists := nodeIDToPeerIDMap[selectedNodeID]
	if !pidExists {
		// This should not happen if the maps are consistent.
		log.Printf("CRITICAL ERROR: Could not find PeerID for selected NodeID %s", selectedNodeID)
		return "", "", fmt.Errorf("internal error: could not map selected NodeID %s back to PeerID", selectedNodeID)
	}

	log.Printf("Randomly selected consensus partner: NodeID %s (PeerID: %s)", selectedNodeID, selectedPID)
	logger.Info("Selected partner NodeID: ", selectedNodeID)
	return selectedNodeID, selectedPID, nil
}

// parseTransactionsFromResponse is a placeholder function.
// It needs to be implemented based on the actual response format from the master node.
func parseTransactionsFromResponse(responseData []byte) ([]byte, error) {
	if responseData == nil {
		return []byte{}, nil // No data means no transactions.
	}
	log.Printf("parseTransactionsFromResponse: Received %d bytes of transaction data.", len(responseData))
	// Actual parsing logic would go here. For now, assume responseData is the direct transaction payload.
	return responseData, nil
}

// signEventData signs the hash of the EventData.
func signEventData(eventData dag.EventData, privKey crypto.PrivKey) ([]byte, error) {
	if privKey == nil {
		return nil, errors.New("private key is nil, cannot sign event data")
	}
	// Create a temporary event shell just for hashing, as Event.Hash() operates on EventData.
	tempEventForHashing := &dag.Event{EventData: eventData}
	hashToSign, err := tempEventForHashing.Hash() // Hash() is from dag/event.go
	if err != nil {
		return nil, fmt.Errorf("failed to hash event data for signing: %w", err)
	}

	dataBytes := hashToSign.Bytes()
	log.Printf("Signing hash of EventData: %s", hashToSign.String())
	signature, err := privKey.Sign(dataBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to sign event data hash: %w", err)
	}
	log.Printf("Successfully signed event data hash. Signature length: %d", len(signature))
	return signature, nil
}

// getPublicKeyBytesFromHexString is a placeholder/helper.
// Converts a hex string representation of a public key to a byte slice.
func getPublicKeyBytesFromHexString(hexStr string) ([]byte, error) {
	if hexStr == "" {
		return nil, errors.New("cannot convert empty hex string to public key bytes")
	}
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, fmt.Errorf("failed to decode hex string '%s' to bytes: %w", hexStr, err)
	}
	return bytes, nil
}

// calculateNextFrame is a placeholder for frame calculation logic.
// The actual frame calculation in Lachesis can be more complex,
// depending on parent frames and strong seeing relationships.
func (mn *ManagedNode) calculateNextFrame(selfParentEvent *dag.Event, otherParentEvent *dag.Event) uint64 {
	if selfParentEvent == nil { // This implies it's the first event by this creator.
		return 1 // Assuming frames start at 1.
	}
	// Simplified: increment self-parent's frame.
	// A more accurate Lachesis implementation would consider max(frame(parents)) + 1
	// under certain conditions (e.g., strongly seeing roots of the previous frame).
	nextFrame := selfParentEvent.EventData.Frame + 1
	if otherParentEvent != nil && (otherParentEvent.EventData.Frame+1) > nextFrame {
		// This simplistic adjustment might not align with Lachesis rules.
		// Frame progression is typically based on the highest frame of parents it strongly sees.
		// For now, this reflects a basic increment.
		// nextFrame = otherParentEvent.EventData.Frame + 1
	}
	return nextFrame
}

// checkIfRoot is a placeholder for determining if a new event is a Root.
// In Lachesis, an event is a root if it's the first event by its creator in a new frame,
// or under specific conditions related to its parents and their frames.
func (mn *ManagedNode) checkIfRoot(newEventData dag.EventData, selfParentEvent *dag.Event, otherParentEvent *dag.Event) bool {
	// If no self-parent, it's the first event by this creator, thus a root for its frame.
	if selfParentEvent == nil {
		return true
	}
	// If the new event's frame is greater than its self-parent's frame, it's a root for the new frame.
	if newEventData.Frame > selfParentEvent.EventData.Frame {
		return true
	}

	// More complex Lachesis logic: An event e is a root if frame(e) > frame(p)
	// for all parents p that e strongly sees.
	// The current placeholder is a simplification.
	// For now, if it's in a new frame relative to its self-parent, it's considered a root.
	return false // Default if not fitting simpler conditions above.
}

// consensusLoop is the main loop that drives the consensus process.
// It periodically attempts to finalize events, create new events, and synchronize with peers.
func (mn *ManagedNode) consensusLoop() {
	defer mn.wg.Done()

	consensusTickInterval := mn.config.ConsensusTickInterval
	if consensusTickInterval <= 0 {
		// Default if not set or invalid in config.
		consensusTickInterval = 10 * time.Second
		log.Printf("ConsensusTickInterval not configured or invalid, using default: %v", consensusTickInterval)
	}
	logger.Info("Consensus tick interval set to: ", consensusTickInterval)
	ticker := time.NewTicker(consensusTickInterval)
	defer ticker.Stop()

	log.Println("Starting main consensus loop (Finality Check -> If no progress: Create New Event & Sync)...")

	for {
		select {
		case <-mn.ctx.Done():
			log.Println("Stopping consensus loop due to node context cancellation.")
			return
		case <-ticker.C:
			log.Println("--- New Consensus Round ---")
			// logger.Info("Current DagStore snapshot:", mn.dagStore.GetAllEventsSnapshot()) // Potentially very verbose
			mn.dagStore.PrintDagStoreStatus()
			// Step 1: Attempt to finalize existing events and process the "next block".
			log.Printf("CONSENSUS_LOOP: Running DecideClotho() to determine Clotho events. DagStore's LastDecidedFrame: %d", mn.dagStore.GetLastDecidedFrame())
			mn.dagStore.DecideClotho() // Updates Clotho status for Roots.
			logger.Info("CONSENSUS_LOOP: DecideClotho() finished.")

			currentDagLastDecidedFrame := mn.dagStore.GetLastDecidedFrame()
			log.Printf("CONSENSUS_LOOP: After DecideClotho(), DagStore's LastDecidedFrame: %d. Previously processed finalized frame: %d", currentDagLastDecidedFrame, mn.lastProcessedFinalizedFrame)

			var newlyFinalizedRoots []*dag.Event
			if currentDagLastDecidedFrame > mn.lastProcessedFinalizedFrame {
				for frameToProcess := mn.lastProcessedFinalizedFrame + 1; frameToProcess <= currentDagLastDecidedFrame; frameToProcess++ {
					rootsInFrame := mn.dagStore.GetRoots(frameToProcess)
					for _, rootID := range rootsInFrame {
						rootEvent, exists := mn.dagStore.GetEvent(rootID)
						if exists && rootEvent.EventData.IsRoot && rootEvent.ClothoStatus == dag.ClothoIsClotho {
							newlyFinalizedRoots = append(newlyFinalizedRoots, rootEvent)
						}
					}
				}
			}

			if len(newlyFinalizedRoots) > 0 {
				log.Printf("CONSENSUS_LOOP: Found %d new Root(s) decided as Clotho to process.", len(newlyFinalizedRoots))
				// Sort for deterministic processing.
				sort.Slice(newlyFinalizedRoots, func(i, j int) bool {
					if newlyFinalizedRoots[i].EventData.Frame != newlyFinalizedRoots[j].EventData.Frame {
						return newlyFinalizedRoots[i].EventData.Frame < newlyFinalizedRoots[j].EventData.Frame
					}
					// Tie-break by timestamp, then by ID for full determinism.
					if newlyFinalizedRoots[i].EventData.Timestamp != newlyFinalizedRoots[j].EventData.Timestamp {
						return newlyFinalizedRoots[i].EventData.Timestamp < newlyFinalizedRoots[j].EventData.Timestamp
					}
					return newlyFinalizedRoots[i].GetEventId().String() < newlyFinalizedRoots[j].GetEventId().String()
				})

				log.Println("CONSENSUS_LOOP: Finalized (Clotho) Roots in processing order:")
				maxProcessedFrameThisRound := mn.lastProcessedFinalizedFrame
				for _, finalizedRoot := range newlyFinalizedRoots {
					log.Printf("  - Processing Finalized Root: ID %s, Frame %d, Creator %s, Timestamp %d, Transactions: %d bytes",
						finalizedRoot.GetEventId().String(),
						finalizedRoot.EventData.Frame,
						hex.EncodeToString(finalizedRoot.EventData.Creator),
						finalizedRoot.EventData.Timestamp,
						len(finalizedRoot.EventData.Transactions))
					// Application-specific processing of finalizedRoot.EventData.Transactions would go here.
					if finalizedRoot.EventData.Frame > maxProcessedFrameThisRound {
						maxProcessedFrameThisRound = finalizedRoot.EventData.Frame
					}
				}
				mn.lastProcessedFinalizedFrame = maxProcessedFrameThisRound
				log.Printf("CONSENSUS_LOOP: Updated lastProcessedFinalizedFrame to %d", mn.lastProcessedFinalizedFrame)

				// Prune old events after processing finalized frames.
				if mn.lastProcessedFinalizedFrame >= MIN_FRAMES_BEFORE_PRUNING {
					oldestFrameToKeep := uint64(1) // Default to keeping from frame 1.
					if mn.lastProcessedFinalizedFrame > FRAMES_TO_KEEP_AFTER_FINALIZED {
						oldestFrameToKeep = mn.lastProcessedFinalizedFrame - FRAMES_TO_KEEP_AFTER_FINALIZED + 1
					}
					log.Printf("CONSENSUS_LOOP: Calling PruneOldEvents with oldestFrameToKeep = %d (lastProcessedFinalizedFrame=%d, FRAMES_TO_KEEP_AFTER_FINALIZED=%d, MIN_FRAMES_BEFORE_PRUNING=%d)",
						oldestFrameToKeep, mn.lastProcessedFinalizedFrame, FRAMES_TO_KEEP_AFTER_FINALIZED, MIN_FRAMES_BEFORE_PRUNING)
					mn.dagStore.PruneOldEvents(oldestFrameToKeep)
				} else {
					log.Printf("CONSENSUS_LOOP: Skipping pruning. lastProcessedFinalizedFrame (%d) < MIN_FRAMES_BEFORE_PRUNING (%d)",
						mn.lastProcessedFinalizedFrame, MIN_FRAMES_BEFORE_PRUNING)
				}
			} else { // No new finalized roots, proceed to create a new event.
				log.Printf("CONSENSUS_LOOP: No new Roots were finalized (Clotho). Proceeding to create new event and synchronize.")

				// Step 2: Create a new event (if no finality progress).
				// Request transactions from the master node.
				reqCtx, cancelReq := context.WithTimeout(mn.ctx, 20*time.Second)
				// Example payload; adjust as needed for your application.
				requestPayload := []byte(fmt.Sprintf(`{"action": "get_pending_transactions", "timestamp": %d, "request_id": "client_periodic_%d"}`, time.Now().Unix(), time.Now().Nanosecond()))

				log.Printf("CONSENSUS_LOOP: Sending TransactionsRequestProtocol to Master Node for transactions (Payload: %s)...", string(requestPayload))
				responseData, err := mn.SendRequestToMasterNode(reqCtx, TransactionsRequestProtocol, requestPayload) // From application_services.go
				cancelReq()

				var transactionsForNewBlock []byte
				if err != nil {
					log.Printf("CONSENSUS_LOOP: Error requesting transactions from Master Node: %v. Will attempt to create event without new transactions.", err)
					transactionsForNewBlock = []byte{} // Create an empty event if TX fetching fails.
				} else {
					log.Printf("CONSENSUS_LOOP: Received transaction response from Master Node (%d bytes).", len(responseData))
					var parseErr error
					transactionsForNewBlock, parseErr = parseTransactionsFromResponse(responseData)
					if parseErr != nil {
						log.Printf("CONSENSUS_LOOP: Error parsing transactions from Master Node: %v. Will attempt to create event without these transactions.", parseErr)
						transactionsForNewBlock = []byte{}
					}
				}

				ownPubKeyHex, err := mn.getOwnPublicKeyHex()
				if err != nil {
					log.Printf("CONSENSUS_LOOP: CRITICAL error getting own public key: %v. Skipping event creation.", err)
					goto endOfConsensusRound // Skip to end of this round.
				}
				ownPubKeyBytes, err := getPublicKeyBytesFromHexString(ownPubKeyHex)
				if err != nil {
					log.Printf("CONSENSUS_LOOP: CRITICAL error converting own public key hex to bytes: %v. Skipping event creation.", err)
					goto endOfConsensusRound
				}

				latestSelfEventID, selfEventExists := mn.dagStore.GetLatestEventIDByCreatorPubKeyHex(ownPubKeyHex)
				var selfParentEvent *dag.Event
				var newEventIndex uint64 = 1 // Default for the first event.

				if selfEventExists {
					var ok bool
					selfParentEvent, ok = mn.dagStore.GetEvent(latestSelfEventID)
					if !ok {
						log.Printf("CONSENSUS_LOOP: Error: Latest self event ID %s exists but event not found in store. Skipping event creation.", latestSelfEventID.String())
						goto endOfConsensusRound
					}
					newEventIndex = selfParentEvent.EventData.Index + 1
				} else {
					log.Printf("CONSENSUS_LOOP: No previous event found for node %s. This will be its first event (Index %d).", ownPubKeyHex, newEventIndex)
					latestSelfEventID = dag.EventID{} // Zero EventID for no self-parent.
				}

				var otherParents []dag.EventID
				var otherParentEventForMeta *dag.Event // Used for metadata like frame calculation.

				partnerNodeID, partnerPeerID, partnerErr := mn.selectConsensusPartner()
				if partnerErr != nil {
					logger.Error("CONSENSUS_LOOP (pre-create event): Failed to select consensus partner: ", partnerErr)
					log.Printf("CONSENSUS_LOOP (pre-create event): Event will not have an otherParent due to partner selection failure: %v", partnerErr)
				} else {
					log.Printf("CONSENSUS_LOOP (pre-create event): Selected partner: NodeID=%s, PeerID=%s", partnerNodeID, partnerPeerID)
					// Get partner's latest event to use as otherParent.
					partnerLatestEventID, partnerLatestExists := mn.dagStore.GetLatestEventIDByCreatorPubKeyHex(string(partnerNodeID))
					if partnerLatestExists {
						otherParents = append(otherParents, partnerLatestEventID)
						otherParentEventForMeta, _ = mn.dagStore.GetEvent(partnerLatestEventID) // Error check might be needed.
						log.Printf("CONSENSUS_LOOP: Using event %s from partner %s as otherParent.", partnerLatestEventID.String(), partnerNodeID)
					} else {
						log.Printf("CONSENSUS_LOOP: Partner %s has no events yet; new event will not have an otherParent from this partner.", partnerNodeID)
					}
				}

				// Calculate Frame and IsRoot for the new event.
				// This is a simplified calculation; Lachesis has more detailed rules.
				newEventFrame := mn.calculateNextFrame(selfParentEvent, otherParentEventForMeta)
				newEventIsRoot := mn.checkIfRoot(dag.EventData{Frame: newEventFrame}, selfParentEvent, otherParentEventForMeta)

				newEventData := dag.EventData{
					Transactions: transactionsForNewBlock,
					SelfParent:   latestSelfEventID,
					OtherParents: otherParents, // May be empty.
					Creator:      ownPubKeyBytes,
					Index:        newEventIndex,
					Timestamp:    time.Now().Unix(),
					Frame:        newEventFrame,
					IsRoot:       newEventIsRoot,
				}

				signature, signErr := signEventData(newEventData, mn.privKey)
				if signErr != nil {
					log.Printf("CONSENSUS_LOOP: Error signing event data: %v. Skipping event creation.", signErr)
					goto endOfConsensusRound
				}
				newEvent := dag.NewEvent(newEventData, signature) // From dag/event.go

				if addEventErr := mn.dagStore.AddEvent(newEvent); addEventErr != nil {
					log.Printf("CONSENSUS_LOOP: Error adding new event %s to DagStore: %v", newEvent.GetEventId().String(), addEventErr)
					goto endOfConsensusRound
				}
				log.Printf("CONSENSUS_LOOP: Created and added new local event: ID %s, Index %d, Frame %d, IsRoot %t, TxLen: %d, OtherParents: %v",
					newEvent.GetEventId().String(), newEvent.EventData.Index, newEvent.EventData.Frame, newEvent.EventData.IsRoot, len(newEvent.EventData.Transactions), newEvent.EventData.OtherParents)

				// Step 3: Synchronize with the selected partner (if any).
				if partnerErr == nil && partnerPeerID != "" {
					log.Printf("CONSENSUS_LOOP: Starting synchronization with partner %s (PeerID: %s)...", partnerNodeID, partnerPeerID)
					mn.requestSyncWithPeer(mn.ctx, partnerPeerID, string(partnerNodeID))
					log.Printf("CONSENSUS_LOOP: Synchronization with partner %s (PeerID: %s) completed or attempted.", partnerNodeID, partnerPeerID)
				} else {
					log.Printf("CONSENSUS_LOOP: Skipping synchronization step as no partner was selected or an error occurred.")
				}
			}
		endOfConsensusRound: // Label for goto.
			mn.dagStore.PrintDagStoreStatus()

			log.Println("--- End of Consensus Round ---")
		}
	}
}

// setupConnectionNotifier configures handlers for network connection events.
// It's updated to store the public key of peers upon successful connection.
func (mn *ManagedNode) setupConnectionNotifier() {
	mn.host.Network().Notify(&network.NotifyBundle{
		ConnectedF: func(net network.Network, conn network.Conn) {
			peerID := conn.RemotePeer()
			log.Printf("✅ Connected to peer: %s (Address: %s)", peerID, conn.RemoteMultiaddr())
			logger.Error("setupConnectionNotifier - Connected to PeerID: ", peerID) // Original log uses Error, kept for consistency.

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
					logger.Info("setupConnectionNotifier - Stored public keys: ", mn.connectedPeerPubKeys)
					log.Printf("Stored public key hex '%s...' for peer %s", pubKeyHex[:min(10, len(pubKeyHex))], peerID)
				}
			}

			mn.peerMutex.RLock() // Use RLock for reading pInfo.Type.
			pInfo, exists := mn.peers[peerID]
			var peerType string
			if exists {
				peerType = pInfo.Type
			} else {
				peerType = "unknown_inbound" // Default type for unexpected inbound connections.
			}
			mn.peerMutex.RUnlock()

			mn.updatePeerStatus(peerID, PeerConnected, nil, peerType)                               // From peer_manager.go
			mn.host.Peerstore().AddAddr(peerID, conn.RemoteMultiaddr(), peerstore.ConnectedAddrTTL) // From peer_manager.go
		},
		DisconnectedF: func(net network.Network, conn network.Conn) {
			peerID := conn.RemotePeer()
			log.Printf("❌ Disconnected from peer: %s", peerID)

			mn.peerPubKeyMutex.Lock()
			delete(mn.connectedPeerPubKeys, peerID)
			mn.peerPubKeyMutex.Unlock()
			log.Printf("Removed stored public key for peer %s due to disconnection.", peerID)

			mn.peerMutex.RLock()
			pInfo, exists := mn.peers[peerID]
			var peerType string // Determine type for status update and reconnect logic.
			if exists {
				peerType = pInfo.Type
			} else {
				// If peer info doesn't exist, it might have been an ephemeral connection
				// or not yet fully registered. Use a sensible default or handle accordingly.
				peerType = "unknown_disconnected"
			}
			mn.peerMutex.RUnlock()

			mn.updatePeerStatus(peerID, PeerDisconnected, errors.New("disconnected"), peerType) // From peer_manager.go

			// Decide whether to attempt reconnection.
			if exists && shouldReconnect(pInfo.Type, mn.config) { // From peer_manager.go
				log.Printf("Scheduled reconnection attempt for important peer %s (Type: %s)", peerID, pInfo.Type)
				mn.tryReconnectToPeer(peerID, pInfo.Type) // From peer_manager.go
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

// requestSyncWithPeer initiates a synchronization process with a specific peer.
// This node (Node A - Initiator) requests events from the partner node (Node B - Provider).
func (mn *ManagedNode) requestSyncWithPeer(ctx context.Context, partnerPeerID peer.ID, partnerNodeID string) {
	log.Printf("SYNC_REQUEST: Attempting to start synchronization with partner %s (PeerID: %s)", partnerNodeID, partnerPeerID)

	ownPubKeyHex, err := mn.getOwnPublicKeyHex()
	if err != nil {
		log.Printf("SYNC_REQUEST: Error getting own public key for sync request to %s: %v", partnerPeerID, err)
		return
	}
	if ownPubKeyHex == "" {
		log.Printf("SYNC_REQUEST: Own public key is empty, cannot send sync request to %s.", partnerPeerID)
		return
	}
	if partnerPeerID == mn.host.ID() {
		log.Printf("SYNC_REQUEST: Skipping synchronization with self (PeerID: %s)", partnerPeerID)
		return
	}

	// 1. Prepare the payload for the synchronization request.
	// This payload contains information about the latest events known by the current node (Node A).
	knownMaxIndices := make(map[string]uint64)
	latestEventsMap := mn.dagStore.GetLatestEventsMapSnapshot() // map[creatorPubKeyHex]EventID

	for creatorPubKeyHex, latestEventID := range latestEventsMap {
		if latestEventID.IsZero() { // Skip zero EventIDs.
			continue
		}
		latestEvent, exists := mn.dagStore.GetEvent(latestEventID)
		if exists {
			knownMaxIndices[creatorPubKeyHex] = latestEvent.EventData.Index
		} else {
			// This case should ideally not happen if DagStore is consistent.
			log.Printf("SYNC_REQUEST: Warning - Latest event %s for creator %s not found in DagStore when preparing KnownMaxIndices for sync request.",
				latestEventID.String()[:6], creatorPubKeyHex[:6])
		}
	}
	log.Printf("SYNC_REQUEST: Prepared KnownMaxIndices to send to %s: %d items", partnerPeerID, len(knownMaxIndices))

	// Use the SyncRequestPayload struct (defined in stream_manager.go or similar).
	syncPayload := SyncRequestPayload{
		Action:          "request_events_based_on_indices", // A clear action string.
		RequesterNodeID: ownPubKeyHex,
		KnownMaxIndices: knownMaxIndices,
	}

	requestPayloadBytes, err := json.Marshal(syncPayload)
	if err != nil {
		log.Printf("SYNC_REQUEST: Error marshaling SyncRequestPayload for partner %s: %v", partnerPeerID, err)
		return
	}

	// 2. Send the request using mn.SendRequest (from stream_manager.go).
	reqCtx, cancelReq := context.WithTimeout(ctx, 30*time.Second) // Timeout for the sync request.
	defer cancelReq()

	log.Printf("SYNC_REQUEST: Sending SyncRequestProtocol to %s (PeerID: %s). Payload size: %d bytes.", partnerNodeID, partnerPeerID, len(requestPayloadBytes))
	// log.Printf("SYNC_REQUEST: Outgoing payload: %s", string(requestPayloadBytes)) // Log detailed payload for debugging if necessary.

	responseData, err := mn.SendRequest(reqCtx, partnerPeerID, SyncRequestProtocol, requestPayloadBytes)

	if err != nil {
		log.Printf("SYNC_REQUEST: Error sending sync request to partner %s (PeerID: %s): %v", partnerNodeID, partnerPeerID, err)
		// "Failed to negotiate protocol" error can occur here if the target node (partnerPeerID)
		// does not support SyncRequestProtocol or its handler is not set up correctly.
		return
	}

	// 3. Process the responseData received from the partner.
	log.Printf("SYNC_REQUEST: Received sync response from partner %s (PeerID: %s). Size: %d bytes.", partnerNodeID, partnerPeerID, len(responseData))
	// log.Printf("SYNC_REQUEST: Response data: %s", string(responseData)) // Log detailed response for debugging if necessary.

	var syncResponse SyncResponsePayload // This struct should also be defined (e.g., in stream_manager.go).
	if err := json.Unmarshal(responseData, &syncResponse); err != nil {
		log.Printf("SYNC_REQUEST: Error unmarshaling sync response from %s: %v", partnerPeerID, err)
		return
	}

	if len(syncResponse.Events) == 0 {
		log.Printf("SYNC_REQUEST: Partner %s sent no new events (possibly already in sync or partner has no new events).", partnerPeerID)
	} else {
		log.Printf("SYNC_REQUEST: Received %d event(s) from partner %s. Processing...", len(syncResponse.Events), partnerPeerID)
		addedCount := 0
		for i, eventBytes := range syncResponse.Events {
			event, errUnmarshal := dag.Unmarshal(eventBytes) // Use dag.Unmarshal from consensus/dag/event.go.
			if errUnmarshal != nil {
				log.Printf("SYNC_REQUEST: Error unmarshaling event %d from partner %s: %v. Skipping this event.", i+1, partnerPeerID, errUnmarshal)
				continue
			}
			// Basic validation of the event (e.g., signature, structure) could be added here.

			// Add the event to the current node's DagStore.
			if errAdd := mn.dagStore.AddEvent(event); errAdd != nil {
				// Error could be due to invalid event, already exists, or other DagStore issues.
				// Example: if strings.Contains(errAdd.Error(), "already exists") { log.Printf(...) }
				log.Printf("SYNC_REQUEST: Error adding event %s (Index: %d, Creator: %s) from partner %s to DagStore: %v",
					event.GetEventId().String()[:6], event.EventData.Index, hex.EncodeToString(event.EventData.Creator)[:6], partnerPeerID, errAdd)
			} else {
				addedCount++
				log.Printf("SYNC_REQUEST: Successfully added event %s (Index: %d, Creator: %s) from partner %s.",
					event.GetEventId().String()[:6], event.EventData.Index, hex.EncodeToString(event.EventData.Creator)[:6], partnerPeerID)
			}
		}
		log.Printf("SYNC_REQUEST: Added %d/%d events received from partner %s to DagStore.", addedCount, len(syncResponse.Events), partnerPeerID)
	}

	// Optionally, process syncResponse.PartnerLatestIndices if needed.
	if len(syncResponse.PartnerLatestIndices) > 0 {
		log.Printf("SYNC_REQUEST: Partner %s also sent information about their latest indices (%d items). (Currently, this info is not further processed).",
			partnerPeerID, len(syncResponse.PartnerLatestIndices))
		// Logic to update knowledge about the partner's state could be added here.
	}
}
