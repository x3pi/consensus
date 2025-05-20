package consensusnode

import (
	"context" // Không sử dụng trực tiếp, nhưng là một phần của crypto.Secp256k1PrivateKey
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sort" // Thêm import để sắp xếp event theo frame và timestamp
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

	// Thay thế "github.com/blockchain/consensus/dag" bằng đường dẫn thực tế của bạn
	"github.com/blockchain/consensus/dag" // [cite: consensus/dag/dag.go]
	"github.com/blockchain/consensus/logger"
)

// YourStorageInterface là một placeholder cho interface storage của bạn.
type YourStorageInterface interface {
	Get(key []byte) ([]byte, error)
	Put(key []byte, value []byte) error
	Delete(key []byte) error
	// Thêm các phương thức khác nếu cần
}

// ManagedNode là đối tượng trung tâm cho các hoạt động mạng và đồng thuận.
type ManagedNode struct {
	config       NodeConfig
	host         host.Host
	privKey      crypto.PrivKey // Khóa riêng tư của node
	ownPubKeyHex string         // Chuỗi hex public key của chính node này
	pubsub       *pubsub.PubSub
	dagStore     *dag.DagStore // Instance của DagStore

	topicStorageMap sync.Map // Key: string (tên topic), Value: YourStorageInterface

	peers       map[peer.ID]*ManagedPeerInfo // Thông tin về các peer đã biết/kết nối
	peerMutex   sync.RWMutex
	reconnectWG sync.WaitGroup // WaitGroup cho các goroutine kết nối lại

	ctx        context.Context    // Context của node, dùng để hủy các goroutine
	cancelFunc context.CancelFunc // Hàm hủy context của node
	wg         sync.WaitGroup     // Để quản lý các goroutine chính

	// Stream Handlers
	streamHandlers map[protocol.ID]network.StreamHandler

	// Topic Subscriptions
	topicSubscriptions map[string]*pubsub.Subscription
	topicHandlers      map[string]func(msg *pubsub.Message)

	// Trạng thái nội bộ khác
	keyValueCache               *lru.Cache[string, []byte]
	transactionChan             chan []byte
	fetchingBlocks              sync.Map // Theo dõi các block đang được tìm kiếm (key: uint64, value: bool)
	feeAddresses                []string
	feeAddressesMux             sync.RWMutex
	lastProcessedFinalizedFrame uint64 // Frame cuối cùng mà các Root hoàn tất đã được xử lý

	// Map để lưu trữ public key (dạng hex string) của các peer đã kết nối.
	// Key: peer.ID, Value: chuỗi hex của public key.
	connectedPeerPubKeys map[peer.ID]string
	peerPubKeyMutex      sync.RWMutex // Mutex cho connectedPeerPubKeys
}

// NewManagedNode tạo và khởi tạo một ManagedNode mới.
func NewManagedNode(ctx context.Context, cfg NodeConfig) (*ManagedNode, error) {
	privKey, err := loadPrivateKey(cfg.PrivateKey) // utils.go
	if err != nil {
		return nil, fmt.Errorf("không thể tải khóa riêng tư: %w", err)
	}

	var ownPubKeyHex string
	if privKey != nil {
		var finalPubKeyBytes []byte
		var extractionError error

		pubKey := privKey.GetPublic()
		finalPubKeyBytes, extractionError = pubKey.Raw() // Thử lấy raw bytes trực tiếp

		if extractionError != nil { // Nếu thất bại, thử với loại khóa cụ thể
			log.Printf("Cảnh báo: Không thể lấy raw public key bytes bằng pubKey.Raw() ban đầu: %v. Thử loại khóa cụ thể.", extractionError)
			if ecdsaPriv, ok := privKey.(*crypto.Secp256k1PrivateKey); ok {
				log.Println("Khóa riêng tư là loại Secp256k1PrivateKey.")
				ecdsaTypedPubKey := ecdsaPriv.GetPublic().(*crypto.Secp256k1PublicKey)
				var ecdsaBytes []byte
				var ecdsaErr error
				ecdsaBytes, ecdsaErr = ecdsaTypedPubKey.Raw()
				if ecdsaErr == nil {
					log.Println("Lấy raw public key thành công bằng ecdsaTypedPubKey.Raw().")
					finalPubKeyBytes = ecdsaBytes
					extractionError = nil
				} else {
					log.Printf("Không thể lấy raw public key từ ecdsaTypedPubKey.Raw(): %v", ecdsaErr)
					extractionError = ecdsaErr
				}
			} else {
				log.Printf("Khóa riêng tư không phải là loại Secp256k1PrivateKey đã biết. Lỗi ban đầu từ pubKey.Raw() được giữ lại.")
			}
		}

		if extractionError == nil && finalPubKeyBytes != nil {
			ownPubKeyHex = hex.EncodeToString(finalPubKeyBytes)
		} else {
			log.Printf("Không thể trích xuất public key bytes cuối cùng từ private key. Lỗi (nếu có): %v", extractionError)
		}
	}

	if ownPubKeyHex == "" && privKey != nil {
		log.Printf("CẢNH BÁO QUAN TRỌNG: ownPubKeyHex rỗng mặc dù có private key. Lựa chọn node có thể không hoạt động đúng.")
	}

	cm, err := connmgr.NewConnManager(
		cfg.MinConnections,
		cfg.MaxConnections,
		connmgr.WithGracePeriod(cfg.ConnectionGracePeriod),
	)
	if err != nil {
		return nil, fmt.Errorf("không thể tạo connection manager: %w", err)
	}

	libp2pHost, err := libp2p.New(
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings(cfg.ListenAddress),
		libp2p.ConnectionManager(cm),
		libp2p.EnableRelayService(),
		libp2p.EnableNATService(),
		libp2p.NATPortMap(),
		libp2p.EnableHolePunching(),
	)
	if err != nil {
		return nil, fmt.Errorf("không thể tạo libp2p host: %w", err)
	}

	psOptions := []pubsub.Option{
		pubsub.WithMaxMessageSize(cfg.MaxMessageSize),
	}
	ps, err := pubsub.NewGossipSub(ctx, libp2pHost, psOptions...)
	if err != nil {
		_ = libp2pHost.Close()
		return nil, fmt.Errorf("không thể tạo pubsub: %w", err)
	}

	nodeCtx, cancel := context.WithCancel(ctx)

	cache, err := lru.New[string, []byte](cfg.KeyValueCacheSize)
	if err != nil {
		_ = libp2pHost.Close()
		cancel()
		return nil, fmt.Errorf("không thể tạo LRU cache: %w", err)
	}

	initialStakeData := make(map[string]uint64)
	if ownPubKeyHex != "" && cfg.InitialStake > 0 { // [cite: consensusnode/config.go]
		initialStakeData[ownPubKeyHex] = cfg.InitialStake
	}
	dagStoreInstance := dag.NewDagStore(initialStakeData) // [cite: consensus/dag/dag.go]

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
		lastProcessedFinalizedFrame: 0, // Khởi tạo là 0 (hoặc frame bắt đầu của bạn - 1)
	}

	if mn.config.NodeType == "consensus" {
		mn.RegisterStreamHandler(TransactionsRequestProtocol, mn.transactionsRequestHandler)
		log.Printf("Đã đăng ký stream handler cho %s (Loại Node: %s)", TransactionsRequestProtocol, mn.config.NodeType)

		mn.RegisterStreamHandler(SyncRequestProtocol, mn.syncRequestHandler) // THIS LINE
		log.Printf("Đã đăng ký stream handler cho %s (Loại Node: %s)", SyncRequestProtocol, mn.config.NodeType)
	}

	mn.displayNodeInfo() // [cite: consensusnode/utils.go]

	return mn, nil
}

// Start khởi tạo các hoạt động của node.
func (mn *ManagedNode) Start() error {
	for protoID, handler := range mn.streamHandlers {
		mn.host.SetStreamHandler(protoID, handler)
		log.Printf("Đã đăng ký stream handler cho protocol %s", protoID)
	}

	if err := mn.connectToBootstrapPeers(); err != nil { // [cite: consensusnode/peer_manager.go]
		log.Printf("LƯU Ý: Không thể kết nối tới một số bootstrap peer: %v", err)
	}

	mn.wg.Add(1)
	go mn.peerHealthMonitor() // [cite: consensusnode/peer_manager.go]

	mn.setupConnectionNotifier()

	log.Printf("ManagedNode (%s) đã khởi động thành công.", mn.host.ID())

	if mn.config.NodeType == "consensus" {
		mn.wg.Add(1)
		go mn.consensusLoop()
	}
	return nil
}

// Stop tắt ManagedNode một cách an toàn.
func (mn *ManagedNode) Stop() error {
	log.Println("Đang dừng ManagedNode...")
	mn.cancelFunc()

	mn.cancelAllReconnects() // [cite: consensusnode/peer_manager.go]
	mn.reconnectWG.Wait()

	if err := mn.host.Close(); err != nil {
		log.Printf("Lỗi khi đóng libp2p host: %v", err)
	}

	mn.wg.Wait()
	log.Println("ManagedNode đã dừng.")
	return nil
}

// Host trả về libp2p host instance.
func (mn *ManagedNode) Host() host.Host {
	return mn.host
}

// PubSubInstance trả về pubsub instance.
func (mn *ManagedNode) PubSubInstance() *pubsub.PubSub {
	return mn.pubsub
}

// Context trả về context của node.
func (mn *ManagedNode) Context() context.Context {
	return mn.ctx
}

// Config trả về cấu hình của node.
func (mn *ManagedNode) Config() NodeConfig {
	return mn.config
}

// getOwnPublicKeyHex trả về public key dạng hex của node hiện tại.
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
				return "", fmt.Errorf("không thể lấy lại public key của chính node: %v", tempErr)
			}
			if mn.ownPubKeyHex == "" {
				return "", errors.New("ownPubKeyHex vẫn rỗng sau khi thử lấy lại trong getOwnPublicKeyHex")
			}
		} else {
			return "", errors.New("private key is nil, cannot derive public key in getOwnPublicKeyHex")
		}
	}
	return mn.ownPubKeyHex, nil
}

// getConnectedPeerPublicKeys trả về một map các peer.ID đang kết nối tới public key hex của chúng.
func (mn *ManagedNode) getConnectedPeerPublicKeys() (map[peer.ID]string, error) {
	mn.peerPubKeyMutex.RLock()
	defer mn.peerPubKeyMutex.RUnlock()
	keysCopy := make(map[peer.ID]string, len(mn.connectedPeerPubKeys))
	for pID, pubKeyHex := range mn.connectedPeerPubKeys {
		keysCopy[pID] = pubKeyHex
	}
	return keysCopy, nil
}

// GetDagStore trả về instance của DagStore.
func (mn *ManagedNode) GetDagStore() *dag.DagStore {
	return mn.dagStore
}

// selectConsensusPartner chọn một node đối tác để đồng bộ và tạo event một cách ngẫu nhiên.
func (mn *ManagedNode) selectConsensusPartner() (NodeID, peer.ID, error) {
	ownKeyHex, err := mn.getOwnPublicKeyHex()
	if err != nil {
		return "", "", fmt.Errorf("không thể lấy public key của chính node: %w", err)
	}
	if ownKeyHex == "" {
		return "", "", errors.New("public key của chính node là rỗng, không thể tiếp tục lựa chọn node")
	}

	connectedPeersMap, err := mn.getConnectedPeerPublicKeys()
	if err != nil {
		return "", "", fmt.Errorf("không thể lấy public key của các peer đang kết nối: %w", err)
	}
	logger.Info("selectConsensusPartner - connectedPeersMap: ", connectedPeersMap) // [cite: consensus/logger/logger.go]

	if len(connectedPeersMap) == 0 {
		log.Println("Không có peer nào đang kết nối với public key đã biết để chọn làm đối tác.")
		return "", "", errors.New("no connected peers with known public keys for selection")
	}

	var candidateNodeIDs []NodeID
	nodeIDToPeerIDMap := make(map[NodeID]peer.ID)

	for pID, peerPubKeyHex := range connectedPeersMap {
		if peerPubKeyHex == ownKeyHex {
			continue
		}
		currentNodeID := GetNodeIDFromString(peerPubKeyHex) // [cite: consensusnode/node_selection.go]
		candidateNodeIDs = append(candidateNodeIDs, currentNodeID)
		nodeIDToPeerIDMap[currentNodeID] = pID
	}

	if len(candidateNodeIDs) == 0 {
		log.Println("Không có ứng cử viên hợp lệ nào sau khi lọc (ví dụ: tất cả đều là chính mình).")
		return "", "", errors.New("no valid candidates after filtering (e.g., all are self)")
	}

	source := rand.NewSource(time.Now().UnixNano())
	randomGenerator := rand.New(source)
	randomIndex := randomGenerator.Intn(len(candidateNodeIDs))
	selectedNodeID := candidateNodeIDs[randomIndex]

	selectedPID, pidExists := nodeIDToPeerIDMap[selectedNodeID]
	if !pidExists {
		log.Printf("Lỗi nghiêm trọng: Không tìm thấy PeerID cho NodeID đã chọn %s", selectedNodeID)
		return "", "", fmt.Errorf("internal error: could not map selected NodeID %s back to PeerID", selectedNodeID)
	}

	log.Printf("Node tham chiếu được chọn ngẫu nhiên cho đồng thuận: %s (PeerID: %s)", selectedNodeID, selectedPID)
	return selectedNodeID, selectedPID, nil
}

// parseTransactionsFromResponse: Placeholder - Cần triển khai dựa trên định dạng response thực tế.
func parseTransactionsFromResponse(responseData []byte) ([]byte, error) {
	if responseData == nil {
		return []byte{}, nil
	}
	log.Printf("parseTransactionsFromResponse: Received %d bytes of transaction data.", len(responseData))
	return responseData, nil
}

// signEventData: Ký hash của EventData.
func signEventData(eventData dag.EventData, privKey crypto.PrivKey) ([]byte, error) {
	if privKey == nil {
		return nil, errors.New("private key is nil, cannot sign event data")
	}
	tempEventForHashing := &dag.Event{EventData: eventData} // [cite: consensus/dag/event.go]
	hashToSign, err := tempEventForHashing.Hash()           // [cite: consensus/dag/event.go]
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

// getPublicKeyBytesFromHexString: Placeholder.
func getPublicKeyBytesFromHexString(hexStr string) ([]byte, error) {
	if hexStr == "" {
		return nil, errors.New("cannot convert empty hex string to public key bytes")
	}
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, fmt.Errorf("failed to decode hex string to bytes: %w", err)
	}
	return bytes, nil
}

// calculateNextFrame: Placeholder.
func (mn *ManagedNode) calculateNextFrame(selfParentEvent *dag.Event, otherParentEvent *dag.Event) uint64 {
	if selfParentEvent == nil {
		return 1
	}
	return selfParentEvent.EventData.Frame + 1
}

// checkIfRoot: Placeholder.
func (mn *ManagedNode) checkIfRoot(newEventData dag.EventData, otherParentEvent *dag.Event) bool {
	if otherParentEvent == nil {
		return true
	}
	return otherParentEvent.EventData.IsRoot
}

// consensusLoop là vòng lặp chính thực hiện các bước đồng thuận.
func (mn *ManagedNode) consensusLoop() {
	defer mn.wg.Done()
	// Tần suất thực hiện một vòng đồng thuận nên được cấu hình
	consensusTickInterval := 15 * time.Second // Ví dụ, có thể lấy từ mn.config
	if mn.config.ConsensusTickInterval > 0 {  // Giả sử bạn thêm ConsensusTickInterval vào NodeConfig
		consensusTickInterval = mn.config.ConsensusTickInterval
	}
	ticker := time.NewTicker(consensusTickInterval)
	defer ticker.Stop()

	log.Println("Bắt đầu vòng lặp đồng thuận chính (Kiểm tra Finality -> Nếu không có tiến triển, Tạo Event Mới)...")

	for {
		select {
		case <-mn.ctx.Done():
			log.Println("Dừng vòng lặp đồng thuận do context của node đã hủy.")
			return
		case <-ticker.C:
			log.Println("--- Vòng đồng thuận mới ---")

			// Bước 1: Cố gắng hoàn tất các event hiện có và xử lý "block tiếp theo"
			log.Printf("CONSENSUS_LOOP: Chạy DecideClotho() để xác định các Clotho events. LastDecidedFrame hiện tại của DagStore: %d", mn.dagStore.GetLastDecidedFrame())
			mn.dagStore.DecideClotho() // Cập nhật trạng thái Clotho cho các Roots [cite: consensus/dag/dag.go]
			currentDagLastDecidedFrame := mn.dagStore.GetLastDecidedFrame()
			log.Printf("CONSENSUS_LOOP: Sau DecideClotho(), LastDecidedFrame của DagStore là: %d. Frame cuối cùng đã xử lý: %d", currentDagLastDecidedFrame, mn.lastProcessedFinalizedFrame)

			// Lấy các roots đã được quyết định là Clotho kể từ lần xử lý cuối cùng
			// Cần một hàm trong DagStore, ví dụ: GetClothoRootsForProcessing(startFrame uint64) []*dag.Event
			// Hàm này sẽ trả về các root có ClothoStatus = ClothoIsClotho và Frame > startFrame và Frame <= currentDagLastDecidedFrame
			// Để đơn giản, ta sẽ duyệt các frame từ mn.lastProcessedFinalizedFrame + 1 đến currentDagLastDecidedFrame
			var newlyFinalizedRoots []*dag.Event
			if currentDagLastDecidedFrame > mn.lastProcessedFinalizedFrame {
				for frameToProcess := mn.lastProcessedFinalizedFrame + 1; frameToProcess <= currentDagLastDecidedFrame; frameToProcess++ {
					rootsInFrame := mn.dagStore.GetRoots(frameToProcess) // [cite: consensus/dag/dag.go]
					for _, rootID := range rootsInFrame {
						rootEvent, exists := mn.dagStore.GetEvent(rootID)                                         // [cite: consensus/dag/dag.go]
						if exists && rootEvent.EventData.IsRoot && rootEvent.ClothoStatus == dag.ClothoIsClotho { // [cite: consensus/dag/event.go]
							newlyFinalizedRoots = append(newlyFinalizedRoots, rootEvent)
						}
					}
				}
			}

			if len(newlyFinalizedRoots) > 0 {
				log.Printf("CONSENSUS_LOOP: Tìm thấy %d Root mới được quyết định là Clotho để xử lý.", len(newlyFinalizedRoots))

				// Sắp xếp các root theo Frame, sau đó có thể theo Timestamp hoặc tiêu chí khác để xử lý theo thứ tự
				sort.Slice(newlyFinalizedRoots, func(i, j int) bool {
					if newlyFinalizedRoots[i].EventData.Frame != newlyFinalizedRoots[j].EventData.Frame {
						return newlyFinalizedRoots[i].EventData.Frame < newlyFinalizedRoots[j].EventData.Frame
					}
					// Nếu cùng frame, có thể sắp xếp theo Creator hoặc Timestamp để có thứ tự xác định
					return newlyFinalizedRoots[i].EventData.Timestamp < newlyFinalizedRoots[j].EventData.Timestamp
				})

				log.Println("CONSENSUS_LOOP: Các Root được hoàn tất (Clotho) theo thứ tự xử lý:")
				maxProcessedFrameThisRound := mn.lastProcessedFinalizedFrame
				for _, finalizedRoot := range newlyFinalizedRoots {
					log.Printf("  - Xử lý Root Hoàn Tất: ID %s, Frame %d, Creator %s, Timestamp %d, Transactions: %d bytes",
						finalizedRoot.GetEventId().String(),
						finalizedRoot.EventData.Frame,
						hex.EncodeToString(finalizedRoot.EventData.Creator),
						finalizedRoot.EventData.Timestamp,
						len(finalizedRoot.EventData.Transactions))
					// TODO: Logic xử lý "block" thực sự ở đây:
					// 1. Thu thập các giao dịch từ finalizedRoot (và các event nó strongly see).
					// 2. Áp dụng các giao dịch vào state machine.
					// 3. Ghi nhận block đã được xử lý.
					if finalizedRoot.EventData.Frame > maxProcessedFrameThisRound {
						maxProcessedFrameThisRound = finalizedRoot.EventData.Frame
					}
				}
				mn.lastProcessedFinalizedFrame = maxProcessedFrameThisRound
				log.Printf("CONSENSUS_LOOP: Đã cập nhật lastProcessedFinalizedFrame thành %d", mn.lastProcessedFinalizedFrame)
				// Sau khi xử lý các block hoàn tất, vòng lặp sẽ tiếp tục ở tick tiếp theo
				// để kiểm tra xem có thêm block nào hoàn tất nữa không.
			} else {
				log.Printf("CONSENSUS_LOOP: Không có Root nào mới được hoàn tất (Clotho) trong phạm vi frame [%d - %d]. Tiến hành tạo event mới.", mn.lastProcessedFinalizedFrame+1, currentDagLastDecidedFrame)

				// Bước 2: Nếu không có tiến triển về hoàn tất, tạo event mới
				// (Bao gồm lấy TX từ master, tạo block local, chọn partner)

				// 2a. Lấy giao dịch từ Master Node
				reqCtx, cancelReq := context.WithTimeout(mn.ctx, 20*time.Second)                                                                                                                                                       // [cite: consensus/cmd/consensus_app/app.go]
				requestPayload := []byte(fmt.Sprintf("{\"action\": \"get_pending_transactions_for_block_creation\", \"timestamp\": %d, \"request_id\": \"consensus_loop_need_data_%d\"}", time.Now().Unix(), time.Now().Nanosecond())) // [cite: consensus/cmd/consensus_app/app.go]

				log.Printf("CONSENSUS_LOOP: Đang gửi TransactionsRequestProtocol tới Master Node để lấy giao dịch (Payload: %s)...", string(requestPayload))
				responseData, err := mn.SendRequestToMasterNode(reqCtx, TransactionsRequestProtocol, requestPayload) // [cite: consensusnode/application_services.go]
				cancelReq()                                                                                          // [cite: consensus/cmd/consensus_app/app.go]

				var transactionsForNewBlock []byte
				if err != nil {
					log.Printf("CONSENSUS_LOOP: Lỗi khi gửi TransactionsRequestProtocol đến Master Node: %v. Sẽ thử tạo block không có giao dịch mới.", err)
					transactionsForNewBlock = []byte{}
				} else {
					log.Printf("CONSENSUS_LOOP: Đã nhận phản hồi giao dịch từ Master Node (%d bytes).", len(responseData))
					var parseErr error
					transactionsForNewBlock, parseErr = parseTransactionsFromResponse(responseData)
					if parseErr != nil {
						log.Printf("CONSENSUS_LOOP: Lỗi khi phân tích cú pháp giao dịch từ Master Node: %v. Sẽ thử tạo block không có giao dịch mới.", parseErr)
						transactionsForNewBlock = []byte{}
					}
				}

				// 2b. Tạo event block local
				ownPubKeyHex, err := mn.getOwnPublicKeyHex()
				if err != nil {
					log.Printf("CONSENSUS_LOOP: Lỗi nghiêm trọng khi lấy public key của chính node: %v. Bỏ qua tạo event.", err)
					goto endOfConsensusRound // Nhảy đến cuối vòng lặp ticker
				}
				ownPubKeyBytes, err := getPublicKeyBytesFromHexString(ownPubKeyHex)
				if err != nil {
					log.Printf("CONSENSUS_LOOP: Lỗi nghiêm trọng khi chuyển đổi public key hex sang bytes: %v. Bỏ qua tạo event.", err)
					goto endOfConsensusRound
				}

				latestSelfEventID, selfEventExists := mn.dagStore.GetLatestEventIDByCreatorPubKeyHex(ownPubKeyHex) // [cite: consensus/dag/dag.go]
				var selfParentEvent *dag.Event
				var newEventIndex uint64 = 1

				if selfEventExists {
					var ok bool
					selfParentEvent, ok = mn.dagStore.GetEvent(latestSelfEventID) // [cite: consensus/dag/dag.go]
					if !ok {
						log.Printf("CONSENSUS_LOOP: Lỗi: latest self event ID %s tồn tại nhưng không tìm thấy event trong store. Bỏ qua tạo event.", latestSelfEventID.String())
						goto endOfConsensusRound
					}
					newEventIndex = selfParentEvent.EventData.Index + 1
				} else {
					log.Printf("CONSENSUS_LOOP: Không tìm thấy event nào trước đó của node %s. Đây sẽ là event đầu tiên (Index %d).", ownPubKeyHex, newEventIndex)
					latestSelfEventID = dag.EventID{}
				}

				var otherParents []dag.EventID
				var otherParentEventForMeta *dag.Event // Dùng cho tính toán Frame/IsRoot

				// Chọn đối tác NGAY TRƯỚC KHI tạo event, để có thể dùng làm otherParent nếu muốn
				partnerNodeID, partnerPeerID, partnerErr := mn.selectConsensusPartner()
				if partnerErr != nil {
					logger.Error("CONSENSUS_LOOP (pre-create event): ", partnerErr) // [cite: consensus/logger/logger.go]
					log.Printf("CONSENSUS_LOOP (pre-create event): Không thể chọn node đối tác: %v. Event sẽ không có otherParent.", partnerErr)
				} else {
					log.Printf("CONSENSUS_LOOP (pre-create event): Đã chọn đối tác: NodeID=%s, PeerID=%s", partnerNodeID, partnerPeerID)
					// Lấy event mới nhất của partner để làm otherParent
					// Chú ý: partnerNodeID là kiểu NodeID (string), cần dùng nó để lấy public key hex
					partnerLatestEventID, partnerLatestExists := mn.dagStore.GetLatestEventIDByCreatorPubKeyHex(string(partnerNodeID)) // [cite: consensus/dag/dag.go]
					if partnerLatestExists {
						otherParents = append(otherParents, partnerLatestEventID)
						otherParentEventForMeta, _ = mn.dagStore.GetEvent(partnerLatestEventID) // [cite: consensus/dag/dag.go]
						log.Printf("CONSENSUS_LOOP: Sử dụng event %s của partner %s làm otherParent.", partnerLatestEventID.String(), partnerNodeID)
					} else {
						log.Printf("CONSENSUS_LOOP: Partner %s chưa có event nào, event mới sẽ không có otherParent từ partner này.", partnerNodeID)
					}
				}

				newEventFrame := mn.calculateNextFrame(selfParentEvent, otherParentEventForMeta)
				newEventIsRoot := mn.checkIfRoot(dag.EventData{}, otherParentEventForMeta)

				newEventData := dag.EventData{
					Transactions: transactionsForNewBlock,
					SelfParent:   latestSelfEventID,
					OtherParents: otherParents, // Có thể có otherParent từ partner đã chọn
					Creator:      ownPubKeyBytes,
					Index:        newEventIndex,
					Timestamp:    time.Now().Unix(),
					Frame:        newEventFrame,
					IsRoot:       newEventIsRoot,
				}

				signature, signErr := signEventData(newEventData, mn.privKey)
				if signErr != nil {
					log.Printf("CONSENSUS_LOOP: Lỗi khi ký event data: %v. Bỏ qua tạo event.", signErr)
					goto endOfConsensusRound
				}

				newEvent := dag.NewEvent(newEventData, signature) // [cite: consensus/dag/event.go]

				if addEventErr := mn.dagStore.AddEvent(newEvent); addEventErr != nil { // [cite: consensus/dag/dag.go]
					log.Printf("CONSENSUS_LOOP: Lỗi khi thêm event block mới %s vào DagStore: %v", newEvent.GetEventId().String(), addEventErr)
					goto endOfConsensusRound
				}
				log.Printf("CONSENSUS_LOOP: Đã tạo và thêm event block local mới: ID %s, Index %d, Frame %d, IsRoot %t, TxLen: %d, OtherParents: %v",
					newEvent.GetEventId().String(), newEvent.EventData.Index, newEvent.EventData.Frame, newEvent.EventData.IsRoot, len(newEvent.EventData.Transactions), newEvent.EventData.OtherParents)
				logger.Error(mn.dagStore)
				// marshaledEvent, marshalErr := newEvent.Marshal() // [cite: consensus/dag/event.go]
				// if marshalErr != nil {
				// 	log.Printf("CONSENSUS_LOOP: Lỗi khi marshal event mới %s: %v", newEvent.GetEventId().String(), marshalErr)
				// } else {
				// 	eventTopic := "consensus/events"                                            // Ví dụ
				// 	if pubErr := mn.PublishMessage(eventTopic, marshaledEvent); pubErr != nil { // [cite: consensusnode/pubsub_manager.go]
				// 		log.Printf("CONSENSUS_LOOP: Lỗi khi publish event mới %s lên topic %s: %v", newEvent.GetEventId().String(), eventTopic, pubErr)
				// 	} else {
				// 		log.Printf("CONSENSUS_LOOP: Đã publish event mới %s lên topic %s", newEvent.GetEventId().String(), eventTopic)
				// 	}
				// }
				// Nếu đã chọn partner ở trên và muốn đồng bộ ngay:
				if partnerErr == nil && partnerPeerID != "" {
					// Gọi hàm requestSyncWithPeer một cách đồng bộ
					log.Printf("CONSENSUS_LOOP: Initiating synchronous sync with partner %s (PeerID: %s)...", partnerNodeID, partnerPeerID)
					mn.requestSyncWithPeer(mn.ctx, partnerPeerID, string(partnerNodeID))
					log.Printf("CONSENSUS_LOOP: Synchronous sync with partner %s (PeerID: %s) completed.", partnerNodeID, partnerPeerID)
				}

			}
		endOfConsensusRound:
			log.Println("--- Kết thúc vòng đồng thuận ---")
		}
	}
}

// setupConnectionNotifier cập nhật để lưu trữ public key của peer khi kết nối.
func (mn *ManagedNode) setupConnectionNotifier() {
	mn.host.Network().Notify(&network.NotifyBundle{
		ConnectedF: func(net network.Network, conn network.Conn) {
			peerID := conn.RemotePeer()
			log.Printf("✅ Đã kết nối tới peer: %s (Địa chỉ: %s)", peerID, conn.RemoteMultiaddr())
			logger.Error("setupConnectionNotifier - peerID", peerID) // [cite: consensus/logger/logger.go]
			pubKey := conn.RemotePublicKey()
			if pubKey == nil {
				log.Printf("Cảnh báo: Không thể lấy public key cho peer %s từ kết nối.", peerID)
			} else {
				rawPubKey, err := pubKey.Raw()
				if err != nil {
					log.Printf("Cảnh báo: Không thể lấy raw public key cho peer %s: %v", peerID, err)
				} else {
					pubKeyHex := hex.EncodeToString(rawPubKey)
					mn.peerPubKeyMutex.Lock()
					mn.connectedPeerPubKeys[peerID] = pubKeyHex
					mn.peerPubKeyMutex.Unlock()
					logger.Info("setupConnectionNotifier - mn.connectedPeerPubKeys", mn.connectedPeerPubKeys) // [cite: consensus/logger/logger.go]
					log.Printf("Đã lưu trữ public key hex '%s...' cho peer %s", pubKeyHex[:min(10, len(pubKeyHex))], peerID)
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
			mn.updatePeerStatus(peerID, PeerConnected, nil, peerType)                               // [cite: consensusnode/peer_manager.go]
			mn.host.Peerstore().AddAddr(peerID, conn.RemoteMultiaddr(), peerstore.ConnectedAddrTTL) // [cite: consensusnode/peer_manager.go]
		},
		DisconnectedF: func(net network.Network, conn network.Conn) {
			peerID := conn.RemotePeer()
			log.Printf("❌ Đã ngắt kết nối từ peer: %s", peerID)
			mn.peerPubKeyMutex.Lock()
			delete(mn.connectedPeerPubKeys, peerID)
			mn.peerPubKeyMutex.Unlock()
			log.Printf("Đã xóa public key đã lưu cho peer %s do ngắt kết nối.", peerID)
			mn.peerMutex.RLock()
			pInfo, exists := mn.peers[peerID]
			var peerType string
			if exists {
				peerType = pInfo.Type
			}
			mn.peerMutex.RUnlock()
			mn.updatePeerStatus(peerID, PeerDisconnected, errors.New("đã ngắt kết nối"), peerType) // [cite: consensusnode/peer_manager.go]
			if exists && shouldReconnect(pInfo.Type, mn.config) {                                  // [cite: consensusnode/peer_manager.go]
				log.Printf("Lên lịch kết nối lại cho peer quan trọng %s (Loại: %s)", peerID, pInfo.Type)
				mn.tryReconnectToPeer(peerID, pInfo.Type) // [cite: consensusnode/peer_manager.go]
			}
		},
	})
}

// min helper
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Các hàm placeholder như loadPrivateKey, RegisterStreamHandler, transactionsRequestHandler,
// displayNodeInfo, connectToBootstrapPeers, peerHealthMonitor, updatePeerStatus, tryReconnectToPeer,
// shouldReconnect, cancelAllReconnects, PublishMessage cần được định nghĩa ở nơi khác trong package consensusnode.
// Đảm bảo rằng tất cả các package được import (ví dụ "github.com/blockchain/consensus/dag")
// được định đường dẫn chính xác và có thể truy cập trong cấu trúc dự án của bạn.

// requestSyncWithPeer initiates a synchronization process with a specific peer.
func (mn *ManagedNode) requestSyncWithPeer(ctx context.Context, partnerPeerID peer.ID, partnerNodeID string) {
	log.Printf("CONSENSUS_LOOP: Attempting to initiate sync with partner %s (PeerID: %s)", partnerNodeID, partnerPeerID)

	// 1. Prepare the sync request payload.
	// This payload could indicate the current state of this node's DAG,
	// for example, the hash of its latest event, its current frame, etc.
	// The specific content depends on your synchronization strategy.
	// For this example, let's send a simple message.
	// You might want to include your own NodeID/PubKeyHex so the partner knows who is requesting.
	ownPubKeyHex, err := mn.getOwnPublicKeyHex()
	if err != nil {
		log.Printf("CONSENSUS_LOOP: Error getting own public key for sync request to %s: %v", partnerPeerID, err)
		return
	}

	// Example payload: could be JSON, Borsh, etc.
	// Let's assume we want to tell the partner our latest event ID.
	latestSelfEventID, selfEventExists := mn.dagStore.GetLatestEventIDByCreatorPubKeyHex(ownPubKeyHex) //
	var selfLatestEventIDStr string
	if selfEventExists {
		selfLatestEventIDStr = latestSelfEventID.String()
	}

	requestPayload := []byte(fmt.Sprintf("{\"action\": \"request_sync\", \"requester_node_id\": \"%s\", \"latest_event_id\": \"%s\", \"timestamp\": %d}",
		ownPubKeyHex, selfLatestEventIDStr, time.Now().Unix()))

	// 2. Send the request using mn.SendRequest (from stream_manager.go)
	// Use a timeout for the request.
	reqCtx, cancelReq := context.WithTimeout(ctx, 30*time.Second) // Adjust timeout as needed
	defer cancelReq()

	log.Printf("CONSENSUS_LOOP: Sending SyncRequestProtocol to %s (Payload: %s)", partnerPeerID, string(requestPayload))
	responseData, err := mn.SendRequest(reqCtx, partnerPeerID, SyncRequestProtocol, requestPayload) //

	if err != nil {
		log.Printf("CONSENSUS_LOOP: Error sending sync request to partner %s (PeerID: %s): %v", partnerNodeID, partnerPeerID, err)
		return
	}

	// 3. Process the responseData.
	// The responseData would contain the information needed to sync up,
	// e.g., missing events, a list of event hashes, etc.
	log.Printf("CONSENSUS_LOOP: Received sync response from partner %s (PeerID: %s): %s", partnerNodeID, partnerPeerID, string(responseData))

	// TODO: Implement logic to parse responseData and update the local DAG (mn.dagStore)
	// This might involve:
	// - Unmarshalling events sent by the partner.
	// - Adding valid events to mn.dagStore.AddEvent(event).
	// - Handling potential conflicts or forks if your protocol supports it.
	// Example:
	// var syncResponse SomeSyncResponseType
	// if err := json.Unmarshal(responseData, &syncResponse); err != nil {
	//     log.Printf("CONSENSUS_LOOP: Error unmarshalling sync response from %s: %v", partnerPeerID, err)
	//     return
	// }
	// for _, eventBytes := range syncResponse.MissingEvents {
	//     event, err := dag.Unmarshal(eventBytes) //
	//     if err != nil {
	//         // handle error
	//         continue
	//     }
	//     if err := mn.dagStore.AddEvent(event); err != nil { //
	//         // handle error
	//     }
	// }
}
