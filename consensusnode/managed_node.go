package consensusnode

import (
	"context" // Không sử dụng trực tiếp, nhưng là một phần của crypto.Secp256k1PrivateKey
	"encoding/hex"
	"encoding/json" // Thêm import này để marshal SyncRequestPayload
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
	"github.com/blockchain/consensus/dag" // [cite:0]
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
	if ownPubKeyHex != "" && cfg.InitialStake > 0 { // [cite:1]
		initialStakeData[ownPubKeyHex] = cfg.InitialStake
	}
	dagStoreInstance := dag.NewDagStore(initialStakeData) // [cite:0]

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
		streamHandlers:              make(map[protocol.ID]network.StreamHandler), // Khởi tạo map này
		topicSubscriptions:          make(map[string]*pubsub.Subscription),
		topicHandlers:               make(map[string]func(msg *pubsub.Message)),
		keyValueCache:               cache,
		transactionChan:             make(chan []byte, cfg.TransactionChanBuffer),
		connectedPeerPubKeys:        make(map[peer.ID]string),
		lastProcessedFinalizedFrame: 0, // Khởi tạo là 0 (hoặc frame bắt đầu của bạn - 1)
	}

	// ================= PHẦN QUAN TRỌNG ĐỂ ĐĂNG KÝ HANDLER =================
	// Đảm bảo rằng node đích (node có PeerID 12D3KooWJ2s3WiamQxiHK43yU3fsTHrgMLNteW1Hr5RpqcKdZUzu)
	// có mn.config.NodeType == "consensus" để đoạn mã này được thực thi cho nó.
	if mn.config.NodeType == "consensus" {
		// Đăng ký handler cho yêu cầu giao dịch
		mn.RegisterStreamHandler(TransactionsRequestProtocol, mn.transactionsRequestHandler)
		log.Printf("Đã đăng ký stream handler cho %s (Loại Node: %s)", TransactionsRequestProtocol, mn.config.NodeType)

		// Đăng ký handler cho yêu cầu đồng bộ hóa
		// ĐÂY LÀ TRÌNH XỬ LÝ CHO PROTOCOL GÂY RA LỖI
		mn.RegisterStreamHandler(SyncRequestProtocol, mn.syncRequestHandler) // [cite:2]
		log.Printf("Đã đăng ký stream handler cho %s (Loại Node: %s)", SyncRequestProtocol, mn.config.NodeType)
	}
	// =======================================================================

	mn.displayNodeInfo() // [cite:3]

	return mn, nil
}

// Start khởi tạo các hoạt động của node.
func (mn *ManagedNode) Start() error {
	// Thiết lập các stream handler đã được đăng ký trong NewManagedNode
	for protoID, handler := range mn.streamHandlers {
		mn.host.SetStreamHandler(protoID, handler)
		log.Printf("Đã đăng ký stream handler cho protocol %s trên host.", protoID)
	}

	if err := mn.connectToBootstrapPeers(); err != nil { // [cite:4]
		log.Printf("LƯU Ý: Không thể kết nối tới một số bootstrap peer: %v", err)
	}

	mn.wg.Add(1)
	go mn.peerHealthMonitor() // [cite:4]

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

	mn.cancelAllReconnects() // [cite:4]
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
	logger.Error("connectedPeerPubKeys: ", mn.connectedPeerPubKeys)
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
	logger.Info("selectConsensusPartner - connectedPeersMap: ", connectedPeersMap) // [cite:5]

	if len(connectedPeersMap) == 0 {
		log.Println("Không có peer nào đang kết nối với public key đã biết để chọn làm đối tác.")
		return "", "", errors.New("no connected peers with known public keys for selection")
	}

	var candidateNodeIDs []NodeID
	nodeIDToPeerIDMap := make(map[NodeID]peer.ID)
	logger.Error("nodeIDToPeerIDMap: ", nodeIDToPeerIDMap)

	for pID, peerPubKeyHex := range connectedPeersMap {
		if peerPubKeyHex == ownKeyHex { // Không chọn chính mình làm đối tác đồng bộ
			continue
		}
		// Lấy thông tin chi tiết của peer từ mn.peers
		peerInfo, exists := mn.peers[pID]
		logger.Info("peerInfo", peerInfo)

		if !exists {
			log.Printf("Cảnh báo: Peer %s có public key nhưng không tìm thấy thông tin trong mn.peers. Bỏ qua.", pID)
			continue
		}
		logger.Info("peer type", peerInfo.Type)
		// **LOẠI BỎ NODE "MASTER" KHỎI DANH SÁCH ỨNG CỬ VIÊN**
		if peerInfo.Type == "master" {
			log.Printf("selectConsensusPartner: Bỏ qua peer %s (Type: %s) vì là master node.", pID, peerInfo.Type)
			continue
		}
		currentNodeID := GetNodeIDFromString(peerPubKeyHex) // [cite:6]
		candidateNodeIDs = append(candidateNodeIDs, currentNodeID)
		nodeIDToPeerIDMap[currentNodeID] = pID
	}

	if len(candidateNodeIDs) == 0 {
		log.Println("Không có ứng cử viên hợp lệ nào sau khi lọc (ví dụ: tất cả đều là chính mình hoặc không có peer nào khác).")
		return "", "", errors.New("no valid candidates after filtering")
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
	logger.Info(selectedNodeID)
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
	tempEventForHashing := &dag.Event{EventData: eventData} // [cite:7]
	hashToSign, err := tempEventForHashing.Hash()           // [cite:7]
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
	// Logic phức tạp hơn có thể cần thiết ở đây dựa trên định nghĩa Root của bạn.
	// Ví dụ: một event là root nếu frame của nó lớn hơn frame của otherParentEvent
	// hoặc nếu otherParentEvent là root của frame trước đó.
	// Để đơn giản, nếu có otherParent, nó không phải là root trừ khi otherParent là root.
	// Điều này có thể cần điều chỉnh cho phù hợp với thuật toán Lachesis.
	// Theo Lachesis, một event là root nếu frame của nó > frame của tất cả các parent mà nó thấy mạnh.
	// Trong trường hợp này, otherParentEvent là parent duy nhất được xem xét cho isRoot.
	// Nếu newEventData.Frame > otherParentEvent.EventData.Frame, thì nó có thể là root.
	// Hoặc, nếu newEventData.Frame == otherParentEvent.EventData.Frame + 1 và otherParentEvent là root.
	// Logic hiện tại: nếu có otherParent, chỉ là root nếu otherParent là root.
	// Điều này có thể không đúng hoàn toàn với Lachesis.
	// Một cách tiếp cận đơn giản hơn: một event là root nếu nó không có other parents
	// hoặc nếu frame của nó là frame đầu tiên của một "vòng" mới.
	// Lachesis: Root là event đầu tiên của một node trong một frame.
	// Frame của event được xác định là max(frame(parents)) + 1 nếu nó thấy mạnh các root của frame trước.
	// IsRoot thường được xác định khi tạo event, dựa trên parents của nó.
	// Nếu một event không có other parents, nó có thể là root của frame của nó.
	// Nếu nó có other parents, nó là root nếu frame của nó là mới.

	// Logic đơn giản: nếu không có otherParentEvent (event đầu tiên của partner trong DAG cục bộ), coi là root.
	// Hoặc nếu frame của event mới lớn hơn frame của otherParentEvent.
	if otherParentEvent == nil {
		return true // Có thể là root nếu không có other parent
	}
	// Nếu frame của event mới dự kiến lớn hơn frame của otherParent, nó có thể là root.
	// Điều này phụ thuộc vào cách frame được tính toán.
	// Giả sử `calculateNextFrame` đã tính đúng frame cho event mới.
	// Nếu newEventData.Frame > otherParentEvent.EventData.Frame, nó là root của frame mới đó.
	return newEventData.Frame > otherParentEvent.EventData.Frame
}

// consensusLoop là vòng lặp chính thực hiện các bước đồng thuận.
func (mn *ManagedNode) consensusLoop() {
	defer mn.wg.Done()
	// Tần suất thực hiện một vòng đồng thuận nên được cấu hình
	consensusTickInterval := 5 * time.Second // Ví dụ, có thể lấy từ mn.config
	// if mn.config.ConsensusTickInterval > 0 {  // Giả sử bạn thêm ConsensusTickInterval vào NodeConfig
	// 	consensusTickInterval = mn.config.ConsensusTickInterval
	// }
	// logger.Info("ConsensusTickInterval: ", consensusTickInterval)
	ticker := time.NewTicker(consensusTickInterval)
	defer ticker.Stop()

	log.Println("Bắt đầu vòng lặp đồng thuận chính (Kiểm tra Finality -> Nếu không có tiến triển, Tạo Event Mới và Đồng bộ)...")

	for {
		select {
		case <-mn.ctx.Done():
			log.Println("Dừng vòng lặp đồng thuận do context của node đã hủy.")
			return
		case <-ticker.C:
			log.Println("--- Vòng đồng thuận mới ---")
			logger.Info(mn.dagStore.GetAllEventsSnapshot())
			// Bước 1: Cố gắng hoàn tất các event hiện có và xử lý "block tiếp theo"
			log.Printf("CONSENSUS_LOOP: Chạy DecideClotho() để xác định các Clotho events. LastDecidedFrame hiện tại của DagStore: %d", mn.dagStore.GetLastDecidedFrame())
			mn.dagStore.DecideClotho() // Cập nhật trạng thái Clotho cho các Roots [cite:0]
			logger.Info("end DecideClotho")
			currentDagLastDecidedFrame := mn.dagStore.GetLastDecidedFrame()
			log.Printf("CONSENSUS_LOOP: Sau DecideClotho(), LastDecidedFrame của DagStore là: %d. Frame cuối cùng đã xử lý: %d", currentDagLastDecidedFrame, mn.lastProcessedFinalizedFrame)

			var newlyFinalizedRoots []*dag.Event
			if currentDagLastDecidedFrame > mn.lastProcessedFinalizedFrame {
				for frameToProcess := mn.lastProcessedFinalizedFrame + 1; frameToProcess <= currentDagLastDecidedFrame; frameToProcess++ {
					rootsInFrame := mn.dagStore.GetRoots(frameToProcess) // [cite:0]
					for _, rootID := range rootsInFrame {
						rootEvent, exists := mn.dagStore.GetEvent(rootID)                                         // [cite:0]
						if exists && rootEvent.EventData.IsRoot && rootEvent.ClothoStatus == dag.ClothoIsClotho { // [cite:7]
							newlyFinalizedRoots = append(newlyFinalizedRoots, rootEvent)
						}
					}
				}
			}

			if len(newlyFinalizedRoots) > 0 {
				log.Printf("CONSENSUS_LOOP: Tìm thấy %d Root mới được quyết định là Clotho để xử lý.", len(newlyFinalizedRoots))
				sort.Slice(newlyFinalizedRoots, func(i, j int) bool {
					if newlyFinalizedRoots[i].EventData.Frame != newlyFinalizedRoots[j].EventData.Frame {
						return newlyFinalizedRoots[i].EventData.Frame < newlyFinalizedRoots[j].EventData.Frame
					}
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
					if finalizedRoot.EventData.Frame > maxProcessedFrameThisRound {
						maxProcessedFrameThisRound = finalizedRoot.EventData.Frame
					}
				}
				mn.lastProcessedFinalizedFrame = maxProcessedFrameThisRound
				log.Printf("CONSENSUS_LOOP: Đã cập nhật lastProcessedFinalizedFrame thành %d", mn.lastProcessedFinalizedFrame)
			} else {
				log.Printf("CONSENSUS_LOOP: Không có Root nào mới được hoàn tất (Clotho). Tiến hành tạo event mới và đồng bộ.")

				// Bước 2: Tạo event mới (nếu không có tiến triển về hoàn tất)
				reqCtx, cancelReq := context.WithTimeout(mn.ctx, 20*time.Second) // [cite:8]
				requestPayload := []byte(fmt.Sprintf("{\"action\": \"get_pending_transactions\", \"timestamp\": %d, \"request_id\": \"client_periodic_%d\"}", time.Now().Unix(), time.Now().Nanosecond()))

				log.Printf("CONSENSUS_LOOP: Đang gửi TransactionsRequestProtocol tới Master Node để lấy giao dịch (Payload: %s)...", string(requestPayload))
				responseData, err := mn.SendRequestToMasterNode(reqCtx, TransactionsRequestProtocol, requestPayload) // [cite:9]
				cancelReq()                                                                                          // [cite:8]

				var transactionsForNewBlock []byte
				if err != nil {
					log.Printf("CONSENSUS_LOOP: Lỗi khi gửi TransactionsRequestProtocol đến Master Node: %v. Sẽ thử tạo block không có giao dịch mới.", err)
					transactionsForNewBlock = []byte{} // Tạo block rỗng nếu không lấy được TX
				} else {
					log.Printf("CONSENSUS_LOOP: Đã nhận phản hồi giao dịch từ Master Node (%d bytes).", len(responseData))
					var parseErr error
					transactionsForNewBlock, parseErr = parseTransactionsFromResponse(responseData)
					if parseErr != nil {
						log.Printf("CONSENSUS_LOOP: Lỗi khi phân tích cú pháp giao dịch từ Master Node: %v. Sẽ thử tạo block không có giao dịch mới.", parseErr)
						transactionsForNewBlock = []byte{}
					}
				}

				ownPubKeyHex, err := mn.getOwnPublicKeyHex()
				if err != nil {
					log.Printf("CONSENSUS_LOOP: Lỗi nghiêm trọng khi lấy public key của chính node: %v. Bỏ qua tạo event.", err)
					goto endOfConsensusRound
				}
				ownPubKeyBytes, err := getPublicKeyBytesFromHexString(ownPubKeyHex)
				if err != nil {
					log.Printf("CONSENSUS_LOOP: Lỗi nghiêm trọng khi chuyển đổi public key hex sang bytes: %v. Bỏ qua tạo event.", err)
					goto endOfConsensusRound
				}

				latestSelfEventID, selfEventExists := mn.dagStore.GetLatestEventIDByCreatorPubKeyHex(ownPubKeyHex) // [cite:0]
				var selfParentEvent *dag.Event
				var newEventIndex uint64 = 1

				if selfEventExists {
					var ok bool
					selfParentEvent, ok = mn.dagStore.GetEvent(latestSelfEventID) // [cite:0]
					if !ok {
						log.Printf("CONSENSUS_LOOP: Lỗi: latest self event ID %s tồn tại nhưng không tìm thấy event trong store. Bỏ qua tạo event.", latestSelfEventID.String())
						goto endOfConsensusRound
					}
					newEventIndex = selfParentEvent.EventData.Index + 1
				} else {
					log.Printf("CONSENSUS_LOOP: Không tìm thấy event nào trước đó của node %s. Đây sẽ là event đầu tiên (Index %d).", ownPubKeyHex, newEventIndex)
					latestSelfEventID = dag.EventID{} // Zero EventID
				}

				var otherParents []dag.EventID
				var otherParentEventForMeta *dag.Event

				partnerNodeID, partnerPeerID, partnerErr := mn.selectConsensusPartner()
				if partnerErr != nil {
					logger.Error("CONSENSUS_LOOP (pre-create event): ", partnerErr) // [cite:5]
					log.Printf("CONSENSUS_LOOP (pre-create event): Không thể chọn node đối tác: %v. Event sẽ không có otherParent.", partnerErr)
				} else {
					log.Printf("CONSENSUS_LOOP (pre-create event): Đã chọn đối tác: NodeID=%s, PeerID=%s", partnerNodeID, partnerPeerID)
					partnerLatestEventID, partnerLatestExists := mn.dagStore.GetLatestEventIDByCreatorPubKeyHex(string(partnerNodeID)) // [cite:0]
					if partnerLatestExists {
						otherParents = append(otherParents, partnerLatestEventID)
						otherParentEventForMeta, _ = mn.dagStore.GetEvent(partnerLatestEventID) // [cite:0]
						log.Printf("CONSENSUS_LOOP: Sử dụng event %s của partner %s làm otherParent.", partnerLatestEventID.String(), partnerNodeID)
					} else {
						log.Printf("CONSENSUS_LOOP: Partner %s chưa có event nào, event mới sẽ không có otherParent từ partner này.", partnerNodeID)
					}
				}

				// Tính toán Frame và IsRoot cho event mới
				// Frame của event mới = max(frame của self-parent, frame của other-parent) + 1,
				// hoặc 1 nếu không có parent.
				// IsRoot nếu đây là event đầu tiên của node trong frame đó.
				// Logic này cần được xem xét cẩn thận theo Lachesis.
				// Tạm thời:
				currentFrame := uint64(1)
				if selfParentEvent != nil {
					currentFrame = selfParentEvent.EventData.Frame
				}
				if otherParentEventForMeta != nil && otherParentEventForMeta.EventData.Frame > currentFrame {
					currentFrame = otherParentEventForMeta.EventData.Frame
				}
				newEventFrame := currentFrame + 1 // Frame mới luôn lớn hơn frame của parent
				// IsRoot: Theo Lachesis, một event là root nếu nó là event đầu tiên của creator trong frame đó.
				// Hoặc, nếu frame(e) > frame(p) cho tất cả parent p mà e thấy mạnh.
				// Đơn giản hóa: Nếu không có self-parent (event đầu tiên của node này) HOẶC frame mới > frame self-parent.
				// Và (nếu có otherParent) frame mới > frame otherParent.
				// Điều này đảm bảo nó là "root" của frame mới này.
				newEventIsRoot := true // Mặc định là root, sẽ kiểm tra lại
				// Nếu selfParentEvent tồn tại và newEventFrame bằng selfParentEvent.EventData.Frame, thì không phải root mới.
				// Điều này không nên xảy ra nếu newEventFrame = currentFrame + 1.
				// IsRoot thường có nghĩa là event đầu tiên của một node trong một frame.
				// Vì chúng ta đang tạo một event mới, và newEventFrame được tính là frame tiếp theo,
				// nên event này sẽ là root cho newEventFrame đối với creator này.

				newEventData := dag.EventData{
					Transactions: transactionsForNewBlock,
					SelfParent:   latestSelfEventID,
					OtherParents: otherParents,
					Creator:      ownPubKeyBytes,
					Index:        newEventIndex,
					Timestamp:    time.Now().Unix(),
					Frame:        newEventFrame,  // Sử dụng frame đã tính
					IsRoot:       newEventIsRoot, // Mặc định là root cho frame mới này
				}

				signature, signErr := signEventData(newEventData, mn.privKey)
				if signErr != nil {
					log.Printf("CONSENSUS_LOOP: Lỗi khi ký event data: %v. Bỏ qua tạo event.", signErr)
					goto endOfConsensusRound
				}
				newEvent := dag.NewEvent(newEventData, signature) // [cite:7]

				if addEventErr := mn.dagStore.AddEvent(newEvent); addEventErr != nil { // [cite:0]
					log.Printf("CONSENSUS_LOOP: Lỗi khi thêm event block mới %s vào DagStore: %v", newEvent.GetEventId().String(), addEventErr)
					goto endOfConsensusRound
				}
				log.Printf("CONSENSUS_LOOP: Đã tạo và thêm event block local mới: ID %s, Index %d, Frame %d, IsRoot %t, TxLen: %d, OtherParents: %v",
					newEvent.GetEventId().String(), newEvent.EventData.Index, newEvent.EventData.Frame, newEvent.EventData.IsRoot, len(newEvent.EventData.Transactions), newEvent.EventData.OtherParents)

				// Bước 3: Đồng bộ với partner đã chọn (nếu có)
				if partnerErr == nil && partnerPeerID != "" {
					log.Printf("CONSENSUS_LOOP: Bắt đầu đồng bộ với đối tác %s (PeerID: %s)...", partnerNodeID, partnerPeerID)
					mn.requestSyncWithPeer(mn.ctx, partnerPeerID, string(partnerNodeID))
					log.Printf("CONSENSUS_LOOP: Đồng bộ với đối tác %s (PeerID: %s) hoàn tất.", partnerNodeID, partnerPeerID)
				} else {
					log.Printf("CONSENSUS_LOOP: Bỏ qua bước đồng bộ do không chọn được đối tác hoặc có lỗi.")
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
			logger.Error("setupConnectionNotifier - peerID", peerID) // [cite:5]
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
					logger.Info("setupConnectionNotifier - mn.connectedPeerPubKeys", mn.connectedPeerPubKeys) // [cite:5]
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
			mn.updatePeerStatus(peerID, PeerConnected, nil, peerType)                               // [cite:4]
			mn.host.Peerstore().AddAddr(peerID, conn.RemoteMultiaddr(), peerstore.ConnectedAddrTTL) // [cite:4]
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
			mn.updatePeerStatus(peerID, PeerDisconnected, errors.New("đã ngắt kết nối"), peerType) // [cite:4]
			if exists && shouldReconnect(pInfo.Type, mn.config) {                                  // [cite:4]
				log.Printf("Lên lịch kết nối lại cho peer quan trọng %s (Loại: %s)", peerID, pInfo.Type)
				mn.tryReconnectToPeer(peerID, pInfo.Type) // [cite:4]
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

// requestSyncWithPeer initiates a synchronization process with a specific peer.
// This function is called by the current node (Node A - Initiator) to request events
// from a partner node (Node B - Provider).
func (mn *ManagedNode) requestSyncWithPeer(ctx context.Context, partnerPeerID peer.ID, partnerNodeID string) {
	log.Printf("CONSENSUS_LOOP: Đang cố gắng bắt đầu đồng bộ với đối tác %s (PeerID: %s)", partnerNodeID, partnerPeerID)

	ownPubKeyHex, err := mn.getOwnPublicKeyHex()
	if err != nil {
		log.Printf("CONSENSUS_LOOP: Lỗi khi lấy public key của chính node để gửi yêu cầu đồng bộ tới %s: %v", partnerPeerID, err)
		return
	}
	if ownPubKeyHex == "" {
		log.Printf("CONSENSUS_LOOP: Public key của chính node là rỗng, không thể gửi yêu cầu đồng bộ tới %s.", partnerPeerID)
		return
	}
	if partnerPeerID == mn.host.ID() {
		log.Printf("CONSENSUS_LOOP: Bỏ qua việc đồng bộ với chính mình (PeerID: %s)", partnerPeerID)
		return
	}

	// 1. Chuẩn bị payload cho yêu cầu đồng bộ.
	// Payload này sẽ chứa thông tin về các event mới nhất mà node hiện tại (Node A) đã biết.
	knownMaxIndices := make(map[string]uint64)
	latestEventsMap := mn.dagStore.GetLatestEventsMapSnapshot() // map[creatorPubKeyHex]EventID

	for creatorPubKeyHex, latestEventID := range latestEventsMap {
		if latestEventID.IsZero() { // Bỏ qua nếu EventID là zero
			continue
		}
		latestEvent, exists := mn.dagStore.GetEvent(latestEventID)
		if exists {
			knownMaxIndices[creatorPubKeyHex] = latestEvent.EventData.Index
		} else {
			// Trường hợp này không nên xảy ra nếu DagStore nhất quán
			log.Printf("CONSENSUS_LOOP: Cảnh báo - Event mới nhất %s của người tạo %s không tìm thấy trong DagStore khi chuẩn bị KnownMaxIndices cho yêu cầu đồng bộ.", latestEventID.String()[:6], creatorPubKeyHex[:6])
		}
	}
	log.Printf("CONSENSUS_LOOP: Chuẩn bị KnownMaxIndices để gửi tới %s: %d mục", partnerPeerID, len(knownMaxIndices))

	// Sử dụng cấu trúc SyncRequestPayload đã định nghĩa (trong stream_manager.go)
	syncPayload := SyncRequestPayload{
		Action:          "request_events_based_on_indices", // Một action string rõ ràng hơn
		RequesterNodeID: ownPubKeyHex,
		KnownMaxIndices: knownMaxIndices,
	}

	requestPayloadBytes, err := json.Marshal(syncPayload)
	if err != nil {
		log.Printf("CONSENSUS_LOOP: Lỗi khi marshal SyncRequestPayload cho đối tác %s: %v", partnerPeerID, err)
		return
	}

	// 2. Gửi yêu cầu sử dụng mn.SendRequest (từ stream_manager.go)
	reqCtx, cancelReq := context.WithTimeout(ctx, 30*time.Second) // Timeout cho yêu cầu
	defer cancelReq()

	log.Printf("CONSENSUS_LOOP: Đang gửi SyncRequestProtocol tới %s (PeerID: %s). Kích thước payload: %d bytes.", partnerNodeID, partnerPeerID, len(requestPayloadBytes))
	// log.Printf("CONSENSUS_LOOP: Payload gửi đi: %s", string(requestPayloadBytes)) // Log chi tiết payload nếu cần debug

	responseData, err := mn.SendRequest(reqCtx, partnerPeerID, SyncRequestProtocol, requestPayloadBytes)

	if err != nil {
		log.Printf("CONSENSUS_LOOP: Lỗi khi gửi yêu cầu đồng bộ tới đối tác %s (PeerID: %s): %v", partnerNodeID, partnerPeerID, err)
		// Lỗi "failed to negotiate protocol" có thể xảy ra ở đây nếu node đích (partnerPeerID)
		// không hỗ trợ SyncRequestProtocol hoặc handler chưa được thiết lập đúng.
		return
	}

	// 3. Xử lý responseData nhận được từ đối tác.
	log.Printf("CONSENSUS_LOOP: Đã nhận phản hồi đồng bộ từ đối tác %s (PeerID: %s). Kích thước: %d bytes.", partnerNodeID, partnerPeerID, len(responseData))
	// log.Printf("CONSENSUS_LOOP: Dữ liệu phản hồi: %s", string(responseData)) // Log chi tiết nếu cần

	var syncResponse SyncResponsePayload // Cấu trúc này cũng được định nghĩa trong stream_manager.go
	if err := json.Unmarshal(responseData, &syncResponse); err != nil {
		log.Printf("CONSENSUS_LOOP: Lỗi unmarshal phản hồi đồng bộ từ %s: %v", partnerPeerID, err)
		return
	}

	if len(syncResponse.Events) == 0 {
		log.Printf("CONSENSUS_LOOP: Đối tác %s không gửi event mới nào (có thể đã đồng bộ hoặc đối tác không có event mới).", partnerPeerID)
	} else {
		log.Printf("CONSENSUS_LOOP: Đã nhận %d event từ đối tác %s. Đang xử lý...", len(syncResponse.Events), partnerPeerID)
		addedCount := 0
		for i, eventBytes := range syncResponse.Events {
			event, err := dag.Unmarshal(eventBytes) // Sử dụng dag.Unmarshal từ consensus/dag/event.go
			if err != nil {
				log.Printf("CONSENSUS_LOOP: Lỗi unmarshal event thứ %d từ đối tác %s: %v. Bỏ qua event này.", i+1, partnerPeerID, err)
				continue
			}
			// Kiểm tra tính hợp lệ của event trước khi thêm (ví dụ: chữ ký, cấu trúc)
			// (Phần này có thể cần logic kiểm tra chi tiết hơn)

			// Thêm event vào DagStore của node hiện tại
			if err := mn.dagStore.AddEvent(event); err != nil {
				// Lỗi có thể do event không hợp lệ, đã tồn tại, hoặc các vấn đề khác với DagStore
				// Nếu lỗi là "event already exists", có thể bỏ qua.
				// if strings.Contains(err.Error(), "already exists") {
				// 	// log.Printf("CONSENSUS_LOOP: Event %s từ đối tác %s đã tồn tại, bỏ qua.", event.GetEventId().String()[:6], partnerPeerID)
				// } else {
				log.Printf("CONSENSUS_LOOP: Lỗi khi thêm event %s (Index: %d, Creator: %s) từ đối tác %s vào DagStore: %v",
					event.GetEventId().String()[:6], event.EventData.Index, hex.EncodeToString(event.EventData.Creator)[:6], partnerPeerID, err)
				// }
			} else {
				addedCount++
				log.Printf("CONSENSUS_LOOP: Đã thêm thành công event %s (Index: %d, Creator: %s) từ đối tác %s.",
					event.GetEventId().String()[:6], event.EventData.Index, hex.EncodeToString(event.EventData.Creator)[:6], partnerPeerID)
			}
		}
		log.Printf("CONSENSUS_LOOP: Đã thêm %d/%d event nhận được từ đối tác %s vào DagStore.", addedCount, len(syncResponse.Events), partnerPeerID)
	}

	// (Tùy chọn) Xử lý syncResponse.PartnerLatestIndices nếu cần
	if len(syncResponse.PartnerLatestIndices) > 0 {
		log.Printf("CONSENSUS_LOOP: Đối tác %s cũng gửi thông tin về latest indices của họ (%d mục). (Hiện tại chưa xử lý thông tin này)", partnerPeerID, len(syncResponse.PartnerLatestIndices))
		// Logic để cập nhật kiến thức về trạng thái của partner có thể được thêm ở đây.
	}
}
