package consensusnode

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math/rand"
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
	keyValueCache   *lru.Cache[string, []byte]
	transactionChan chan []byte
	fetchingBlocks  sync.Map // Theo dõi các block đang được tìm kiếm (key: uint64, value: bool)
	feeAddresses    []string
	feeAddressesMux sync.RWMutex

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
			// Kiểm tra nếu là khóa Secp256k1 (thường dùng trong Ethereum/Bitcoin)
			if ecdsaPriv, ok := privKey.(*crypto.Secp256k1PrivateKey); ok {
				log.Println("Khóa riêng tư là loại Secp256k1PrivateKey.")
				// Lấy public key đã được ép kiểu (*crypto.Secp256k1PublicKey)
				ecdsaTypedPubKey := ecdsaPriv.GetPublic().(*crypto.Secp256k1PublicKey)
				var ecdsaBytes []byte
				var ecdsaErr error
				// Thử lấy raw bytes từ public key đã ép kiểu này
				ecdsaBytes, ecdsaErr = ecdsaTypedPubKey.Raw() // Sử dụng ecdsaTypedPubKey ở đây
				if ecdsaErr == nil {
					log.Println("Lấy raw public key thành công bằng ecdsaTypedPubKey.Raw().")
					finalPubKeyBytes = ecdsaBytes
					extractionError = nil // Xóa lỗi nếu thành công
				} else {
					log.Printf("Không thể lấy raw public key từ ecdsaTypedPubKey.Raw(): %v", ecdsaErr)
					extractionError = ecdsaErr // Giữ lỗi mới nhất
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

	// Tạo connection manager
	cm, err := connmgr.NewConnManager(
		cfg.MinConnections,
		cfg.MaxConnections,
		connmgr.WithGracePeriod(cfg.ConnectionGracePeriod),
	)
	if err != nil {
		return nil, fmt.Errorf("không thể tạo connection manager: %w", err)
	}

	// Tạo Libp2p Host
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

	// PubSub (GossipSub)
	psOptions := []pubsub.Option{
		pubsub.WithMaxMessageSize(cfg.MaxMessageSize),
	}
	ps, err := pubsub.NewGossipSub(ctx, libp2pHost, psOptions...)
	if err != nil {
		_ = libp2pHost.Close() // Dọn dẹp host nếu pubsub thất bại
		return nil, fmt.Errorf("không thể tạo pubsub: %w", err)
	}

	nodeCtx, cancel := context.WithCancel(ctx)

	// Khởi tạo LRU cache
	cache, err := lru.New[string, []byte](cfg.KeyValueCacheSize)
	if err != nil {
		_ = libp2pHost.Close()
		cancel()
		return nil, fmt.Errorf("không thể tạo LRU cache: %w", err)
	}

	// Khởi tạo DagStore
	initialStakeData := make(map[string]uint64)
	// Đảm bảo NodeConfig có trường InitialStake và nó được đọc từ config file.
	if ownPubKeyHex != "" && cfg.InitialStake > 0 { // [cite: consensusnode/config.go]
		initialStakeData[ownPubKeyHex] = cfg.InitialStake
	}
	// Bạn có thể cần thêm logic để điền stake cho các validator khác ở đây.
	dagStoreInstance := dag.NewDagStore(initialStakeData) // [cite: consensus/dag/dag.go]

	mn := &ManagedNode{
		config:               cfg,
		host:                 libp2pHost,
		privKey:              privKey,
		ownPubKeyHex:         ownPubKeyHex,
		pubsub:               ps,
		dagStore:             dagStoreInstance,
		peers:                make(map[peer.ID]*ManagedPeerInfo),
		ctx:                  nodeCtx,
		cancelFunc:           cancel,
		streamHandlers:       make(map[protocol.ID]network.StreamHandler),
		topicSubscriptions:   make(map[string]*pubsub.Subscription),
		topicHandlers:        make(map[string]func(msg *pubsub.Message)),
		keyValueCache:        cache,
		transactionChan:      make(chan []byte, cfg.TransactionChanBuffer),
		connectedPeerPubKeys: make(map[peer.ID]string), // Khởi tạo map
	}

	// Đăng ký stream handler mặc định
	if mn.config.NodeType == "consensus" { // Hoặc một loại node cụ thể khác
		mn.RegisterStreamHandler(TransactionsRequestProtocol, mn.transactionsRequestHandler) // stream_manager.go [cite: consensusnode/stream_manager.go]
		log.Printf("Đã đăng ký stream handler cho %s (Loại Node: %s)", TransactionsRequestProtocol, mn.config.NodeType)
	}

	// Hiển thị thông tin Node
	mn.displayNodeInfo() // utils.go [cite: consensusnode/utils.go]

	return mn, nil
}

// Start khởi tạo các hoạt động của node.
func (mn *ManagedNode) Start() error {
	// Thiết lập các stream handler đã đăng ký
	for protoID, handler := range mn.streamHandlers {
		mn.host.SetStreamHandler(protoID, handler)
		log.Printf("Đã đăng ký stream handler cho protocol %s", protoID)
	}

	// Kết nối tới các bootstrap peer
	if err := mn.connectToBootstrapPeers(); err != nil { // peer_manager.go [cite: consensusnode/peer_manager.go]
		log.Printf("LƯU Ý: Không thể kết nối tới một số bootstrap peer: %v", err)
		// Không trả về lỗi ở đây để node vẫn có thể khởi động
	}

	// Bắt đầu theo dõi sức khỏe peer
	mn.wg.Add(1)
	go mn.peerHealthMonitor() // peer_manager.go [cite: consensusnode/peer_manager.go]

	// Bắt đầu notifier sự kiện kết nối
	mn.setupConnectionNotifier() // QUAN TRỌNG: Cập nhật connectedPeerPubKeys ở đây

	log.Printf("ManagedNode (%s) đã khởi động thành công.", mn.host.ID())

	// Bắt đầu vòng lặp logic đồng thuận chính (ví dụ)
	if mn.config.NodeType == "consensus" { // Chỉ chạy nếu là node đồng thuận
		mn.wg.Add(1)
		go mn.consensusLoop()
	}
	return nil
}

// Stop tắt ManagedNode một cách an toàn.
func (mn *ManagedNode) Stop() error {
	log.Println("Đang dừng ManagedNode...")
	mn.cancelFunc() // Gửi tín hiệu cho tất cả goroutine dừng lại

	mn.cancelAllReconnects() // peer_manager.go [cite: consensusnode/peer_manager.go]
	mn.reconnectWG.Wait()    // Đợi tất cả các goroutine kết nối lại hoàn thành

	// Đóng host
	if err := mn.host.Close(); err != nil {
		log.Printf("Lỗi khi đóng libp2p host: %v", err)
	}

	mn.wg.Wait() // Đợi tất cả các goroutine chính của ManagedNode hoàn thành
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
		// Thử lấy lại nếu chưa có (dù nên có từ NewManagedNode)
		if mn.privKey != nil {
			pubKey := mn.privKey.GetPublic()
			var tempPubKeyBytes []byte
			var tempErr error
			tempPubKeyBytes, tempErr = pubKey.Raw() // Thử lấy raw bytes trực tiếp
			if tempErr != nil {                     // Nếu thất bại, thử với loại khóa cụ thể
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
			if mn.ownPubKeyHex == "" { // Kiểm tra lại sau khi gán
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
	if ownKeyHex == "" { // Kiểm tra quan trọng
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
		if peerPubKeyHex == ownKeyHex { // Bỏ qua chính mình
			continue
		}
		currentNodeID := GetNodeIDFromString(peerPubKeyHex) // Từ node_selection.go (vẫn dùng GetNodeIDFromString) [cite: consensusnode/node_selection.go]

		candidateNodeIDs = append(candidateNodeIDs, currentNodeID)
		nodeIDToPeerIDMap[currentNodeID] = pID
	}

	if len(candidateNodeIDs) == 0 {
		log.Println("Không có ứng cử viên hợp lệ nào sau khi lọc (ví dụ: tất cả đều là chính mình).")
		return "", "", errors.New("no valid candidates after filtering (e.g., all are self)")
	}

	// Chọn ngẫu nhiên một node từ candidateNodeIDs
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

// Helper function to parse transactions from master node's response.
// This is a placeholder and needs to be implemented based on your actual response format.
func parseTransactionsFromResponse(responseData []byte) ([]byte, error) {
	// Ví dụ: Nếu responseData là một JSON array các transactions, bạn cần unmarshal nó.
	// Hoặc nếu nó đã là một byte slice của các transactions đã được nối lại, bạn có thể trả về trực tiếp.
	// Hiện tại, giả sử responseData đã là byte slice của các transactions.
	if responseData == nil {
		return []byte{}, nil // Trả về slice rỗng nếu không có dữ liệu
	}
	log.Printf("parseTransactionsFromResponse: Received %d bytes of transaction data.", len(responseData))
	return responseData, nil
}

// Helper function to sign EventData.
// This function signs the hash of the EventData, consistent with EventID generation.
func signEventData(eventData dag.EventData, privKey crypto.PrivKey) ([]byte, error) {
	if privKey == nil {
		return nil, errors.New("private key is nil, cannot sign event data")
	}

	// 1. Create a temporary Event to utilize its Hashing mechanism,
	//    which includes preparing (e.g., sorting OtherParents) and Borsh serializing EventData.
	tempEventForHashing := &dag.Event{EventData: eventData} // [cite: consensus/dag/event.go]

	// 2. Get the hash of the EventData. This hash is what will be signed.
	//    The Hash() method internally calls PrepareForHashing and then serializes EventData using Borsh,
	//    and finally computes Keccak256.
	hashToSign, err := tempEventForHashing.Hash() // [cite: consensus/dag/event.go]
	if err != nil {
		return nil, fmt.Errorf("failed to hash event data for signing: %w", err)
	}
	dataBytes := hashToSign.Bytes() // Get the byte representation of the hash

	log.Printf("Signing hash of EventData: %s", hashToSign.String())

	// 3. Sign the hash bytes
	signature, err := privKey.Sign(dataBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to sign event data hash: %w", err)
	}
	log.Printf("Successfully signed event data hash. Signature length: %d", len(signature))
	return signature, nil
}

// Helper function to get public key bytes from hex string.
// This is a placeholder.
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

// Helper function to calculate the next frame number.
// This is a placeholder.
func (mn *ManagedNode) calculateNextFrame(selfParentEvent *dag.Event, otherParentEvent *dag.Event) uint64 {
	// Logic tính frame có thể phức tạp, ví dụ:
	// - Nếu là event đầu tiên của node: frame 1 (hoặc 0).
	// - Dựa trên frame của self-parent và other-parent.
	// - Có thể cần logic để xử lý trường hợp một trong các parent không tồn tại.
	// Ví dụ đơn giản: lấy frame lớn nhất của parent + 1, hoặc frame của self-parent + 1.
	// Hoặc, nếu các root được quyết định theo frame, frame có thể tăng khi một root mới được tạo.

	// Logic ví dụ rất đơn giản:
	if selfParentEvent == nil {
		return 1 // Hoặc 0, tùy theo frame bắt đầu của bạn
	}
	// Logic phức tạp hơn có thể dựa trên frame của selfParent và otherParent
	// và liệu event này có trở thành root hay không.
	// Ví dụ: nếu otherParent là root của frame N, event này có thể là root của frame N+1.
	// Hiện tại, chỉ tăng frame của self-parent.
	return selfParentEvent.EventData.Frame + 1
}

// Helper function to determine if the new event should be a root.
// This is a placeholder.
func (mn *ManagedNode) checkIfRoot(newEventData dag.EventData, otherParentEvent *dag.Event) bool {
	// Logic xác định một event có phải là root hay không.
	// Ví dụ:
	// - Event đầu tiên của một node trong một "round" mới có thể là root.
	// - Nếu otherParent là một root và event này "strongly sees" một quorum các root
	//   trong frame của otherParent, thì event này có thể là root của frame tiếp theo.
	// - Hoặc một cách tiếp cận đơn giản hơn: mọi event thứ N của một node là root,
	//   hoặc mỗi khi nó kết nối với một partner mới.

	// Ví dụ rất đơn giản: nếu không có other parent hoặc other parent là root.
	if otherParentEvent == nil {
		return true // Event đầu tiên của node (hoặc không có other parent) có thể là root
	}
	// Nếu other parent là root, event này có thể là root của frame tiếp theo
	return otherParentEvent.EventData.IsRoot
}

// consensusLoop là vòng lặp chính thực hiện các bước đồng thuận.
func (mn *ManagedNode) consensusLoop() {
	defer mn.wg.Done()
	ticker := time.NewTicker(15 * time.Second) // Tần suất thực hiện một vòng đồng thuận
	defer ticker.Stop()

	log.Println("Bắt đầu vòng lặp đồng thuận chính (Thứ tự mới: Lấy TX -> Tạo Block Local -> Chọn Partner)...")

	for {
		select {
		case <-mn.ctx.Done():
			log.Println("Dừng vòng lặp đồng thuận do context của node đã hủy.")
			return
		case <-ticker.C:
			log.Println("--- Vòng đồng thuận mới ---")

			// --- BEGIN MODIFIED ORDER ---
			// Bước 1: Lấy giao dịch từ Master Node
			reqCtx, cancelReq := context.WithTimeout(mn.ctx, 20*time.Second) // Timeout 20 giây cho request [cite: consensus/cmd/consensus_app/app.go]
			requestPayload := []byte(fmt.Sprintf("{\"action\": \"get_pending_transactions\", \"timestamp\": %d, \"request_id\": \"client_periodic_%d\"}", time.Now().Unix(), time.Now().Nanosecond()))

			log.Printf("CONSENSUS_LOOP: Đang gửi TransactionsRequestProtocol tới Master Node để lấy giao dịch (Payload: %s)...", string(requestPayload))
			responseData, err := mn.SendRequestToMasterNode(reqCtx, TransactionsRequestProtocol, requestPayload) // [cite: consensusnode/application_services.go]
			cancelReq()                                                                                          // [cite: consensus/cmd/consensus_app/app.go]

			var transactionsForNewBlock []byte
			if err != nil {
				log.Printf("CONSENSUS_LOOP: Lỗi khi gửi TransactionsRequestProtocol đến Master Node: %v. Sẽ thử tạo block không có giao dịch mới.", err)
				transactionsForNewBlock = []byte{} // Sử dụng slice byte rỗng nếu không lấy được giao dịch
			} else {
				log.Printf("CONSENSUS_LOOP: Đã nhận phản hồi giao dịch từ Master Node: %s", string(responseData))
				var parseErr error
				transactionsForNewBlock, parseErr = parseTransactionsFromResponse(responseData)
				if parseErr != nil {
					log.Printf("CONSENSUS_LOOP: Lỗi khi phân tích cú pháp giao dịch từ Master Node: %v. Sẽ thử tạo block không có giao dịch mới.", parseErr)
					transactionsForNewBlock = []byte{}
				}
			}

			// Bước 2: Tạo event block local
			// Lấy thông tin cần thiết cho event mới
			ownPubKeyHex, err := mn.getOwnPublicKeyHex()
			if err != nil {
				log.Printf("CONSENSUS_LOOP: Lỗi nghiêm trọng khi lấy public key của chính node: %v. Bỏ qua vòng này.", err)
				continue
			}
			ownPubKeyBytes, err := getPublicKeyBytesFromHexString(ownPubKeyHex)
			if err != nil {
				log.Printf("CONSENSUS_LOOP: Lỗi nghiêm trọng khi chuyển đổi public key hex sang bytes: %v. Bỏ qua vòng này.", err)
				continue
			}

			latestSelfEventID, selfEventExists := mn.dagStore.GetLatestEventIDByCreatorPubKeyHex(ownPubKeyHex) // [cite: consensus/dag/dag.go]
			var selfParentEvent *dag.Event
			var newEventIndex uint64 = 1 // Index bắt đầu từ 1 nếu chưa có event nào

			if selfEventExists {
				var ok bool
				selfParentEvent, ok = mn.dagStore.GetEvent(latestSelfEventID) // [cite: consensus/dag/dag.go]
				if !ok {
					log.Printf("CONSENSUS_LOOP: Lỗi: latest self event ID %s tồn tại nhưng không tìm thấy event trong store. Bỏ qua vòng này.", latestSelfEventID.String())
					continue
				}
				newEventIndex = selfParentEvent.EventData.Index + 1
			} else {
				log.Printf("CONSENSUS_LOOP: Không tìm thấy event nào trước đó của node %s. Đây sẽ là event đầu tiên (Index %d).", ownPubKeyHex, newEventIndex)
				// selfParentEvent sẽ là nil, latestSelfEventID sẽ là zero
				latestSelfEventID = dag.EventID{} // Zero EventID
			}

			// Tạm thời, OtherParents sẽ rỗng vì chúng ta chọn partner SAU KHI tạo block này.
			// Event block này sẽ chỉ có self-parent.
			// Hoặc, bạn có thể quyết định rằng event đầu tiên của một "phiên" không cần other parent.
			// Hoặc, bạn có thể lưu trữ otherParent từ vòng trước và sử dụng nó ở đây.
			// Để đơn giản, OtherParents sẽ là slice rỗng.
			var otherParents []dag.EventID
			var otherParentEventForMeta *dag.Event // Dùng cho tính toán Frame/IsRoot

			// Logic tính Frame và IsRoot (cần được cải thiện)
			// Vì chưa có otherParent thực sự cho event *này*,
			// các hàm calculateNextFrame và checkIfRoot sẽ hoạt động với otherParentEventForMeta là nil.
			newEventFrame := mn.calculateNextFrame(selfParentEvent, otherParentEventForMeta)
			newEventIsRoot := mn.checkIfRoot(dag.EventData{}, otherParentEventForMeta) // Truyền EventData rỗng vì nó chưa được tạo
			logger.Error(transactionsForNewBlock)
			newEventData := dag.EventData{
				Transactions: transactionsForNewBlock, // Sử dụng transactions đã lấy (hoặc rỗng)
				SelfParent:   latestSelfEventID,
				OtherParents: otherParents, // Rỗng cho ví dụ này
				Creator:      ownPubKeyBytes,
				Index:        newEventIndex,
				Timestamp:    time.Now().Unix(),
				Frame:        newEventFrame,
				IsRoot:       newEventIsRoot,
			}

			signature, signErr := signEventData(newEventData, mn.privKey)
			if signErr != nil {
				log.Printf("CONSENSUS_LOOP: Lỗi khi ký event data: %v. Bỏ qua vòng này.", signErr)
				continue
			}

			newEvent := dag.NewEvent(newEventData, signature) // [cite: consensus/dag/event.go]

			logger.Info(newEvent)

			if addEventErr := mn.dagStore.AddEvent(newEvent); addEventErr != nil { // [cite: consensus/dag/dag.go]
				log.Printf("CONSENSUS_LOOP: Lỗi khi thêm event block mới %s vào DagStore: %v", newEvent.GetEventId().String(), addEventErr)
				continue
			}
			log.Printf("CONSENSUS_LOOP: Đã tạo và thêm event block local mới: ID %s, Index %d, Frame %d, IsRoot %t, TxLen: %d",
				newEvent.GetEventId().String(), newEvent.EventData.Index, newEvent.EventData.Frame, newEvent.EventData.IsRoot, len(newEvent.EventData.Transactions))

			// Broadcast event mới này
			marshaledEvent, marshalErr := newEvent.Marshal() // [cite: consensus/dag/event.go]
			if marshalErr != nil {
				log.Printf("CONSENSUS_LOOP: Lỗi khi marshal event mới %s: %v", newEvent.GetEventId().String(), marshalErr)
			} else {
				// TODO: Xác định tên topic phù hợp cho việc broadcast event
				eventTopic := "consensus/events"                                            // Ví dụ
				if pubErr := mn.PublishMessage(eventTopic, marshaledEvent); pubErr != nil { // [cite: consensusnode/pubsub_manager.go]
					log.Printf("CONSENSUS_LOOP: Lỗi khi publish event mới %s lên topic %s: %v", newEvent.GetEventId().String(), eventTopic, pubErr)
				} else {
					log.Printf("CONSENSUS_LOOP: Đã publish event mới %s lên topic %s", newEvent.GetEventId().String(), eventTopic)
				}
			}

			// Bước 3: Tìm kiếm đối tác đồng thuận (cho vòng tiếp theo hoặc đồng bộ)
			partnerNodeID, partnerPeerID, err := mn.selectConsensusPartner()
			if err != nil {
				logger.Error("CONSENSUS_LOOP: ", err) // [cite: consensus/logger/logger.go]
				log.Printf("CONSENSUS_LOOP: Không thể chọn node đối tác trong vòng này: %v", err)
				// Vẫn tiếp tục vì đã tạo block local
			} else {
				log.Printf("CONSENSUS_LOOP: Đã chọn đối tác cho các hoạt động tiếp theo: NodeID=%s, PeerID=%s", partnerNodeID, partnerPeerID)
				// TODO: Sử dụng partnerNodeID và partnerPeerID cho các bước tiếp theo,
				// ví dụ: yêu cầu đồng bộ DAG, hoặc lưu trữ để dùng làm otherParent cho event KẾ TIẾP.
				// Ví dụ: mn.requestSyncWithPeer(partnerPeerID, string(partnerNodeID))
				log.Printf("Bước tiếp theo (chưa triển khai): Yêu cầu đồng bộ với %s...", partnerPeerID)
			}
			// --- END MODIFIED ORDER ---

			// Chạy thuật toán quyết định Clotho/Atropos
			// mn.dagStore.DecideClotho() // [cite: consensus/dag/dag.go]
			// log.Printf("CONSENSUS_LOOP: Đã chạy (hoặc cố gắng chạy) DecideClotho(). LastDecidedFrame: %d", mn.dagStore.GetLastDecidedFrame())

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
			pubKey := conn.RemotePublicKey()                         // Lấy public key từ kết nối
			if pubKey == nil {
				log.Printf("Cảnh báo: Không thể lấy public key cho peer %s từ kết nối.", peerID)
			} else {
				rawPubKey, err := pubKey.Raw() // Lấy raw bytes của public key
				if err != nil {
					log.Printf("Cảnh báo: Không thể lấy raw public key cho peer %s: %v", peerID, err)
				} else {
					pubKeyHex := hex.EncodeToString(rawPubKey)
					mn.peerPubKeyMutex.Lock()
					mn.connectedPeerPubKeys[peerID] = pubKeyHex // Lưu trữ public key hex
					mn.peerPubKeyMutex.Unlock()
					logger.Info("setupConnectionNotifier - mn.connectedPeerPubKeys", mn.connectedPeerPubKeys) // [cite: consensus/logger/logger.go]
					log.Printf("Đã lưu trữ public key hex '%s...' cho peer %s", pubKeyHex[:min(10, len(pubKeyHex))], peerID)
				}
			}

			// Phần còn lại của logic ConnectedF (cập nhật peer status, v.v.)
			mn.peerMutex.RLock()
			pInfo, exists := mn.peers[peerID]
			var peerType string
			if exists {
				peerType = pInfo.Type
			} else {
				peerType = "unknown_inbound"
			}
			mn.peerMutex.RUnlock()
			mn.updatePeerStatus(peerID, PeerConnected, nil, peerType)                               // peer_manager.go [cite: consensusnode/peer_manager.go]
			mn.host.Peerstore().AddAddr(peerID, conn.RemoteMultiaddr(), peerstore.ConnectedAddrTTL) // [cite: consensusnode/peer_manager.go]
		},
		DisconnectedF: func(net network.Network, conn network.Conn) {
			peerID := conn.RemotePeer()
			log.Printf("❌ Đã ngắt kết nối từ peer: %s", peerID)

			// Xóa public key của peer khỏi map khi ngắt kết nối
			mn.peerPubKeyMutex.Lock()
			delete(mn.connectedPeerPubKeys, peerID)
			mn.peerPubKeyMutex.Unlock()
			log.Printf("Đã xóa public key đã lưu cho peer %s do ngắt kết nối.", peerID)

			// Phần còn lại của logic DisconnectedF
			mn.peerMutex.RLock()
			pInfo, exists := mn.peers[peerID]
			var peerType string
			if exists {
				peerType = pInfo.Type
			}
			mn.peerMutex.RUnlock()
			mn.updatePeerStatus(peerID, PeerDisconnected, errors.New("đã ngắt kết nối"), peerType) // peer_manager.go [cite: consensusnode/peer_manager.go]
			if exists && shouldReconnect(pInfo.Type, mn.config) {                                  // peer_manager.go [cite: consensusnode/peer_manager.go]
				log.Printf("Lên lịch kết nối lại cho peer quan trọng %s (Loại: %s)", peerID, pInfo.Type)
				mn.tryReconnectToPeer(peerID, pInfo.Type) // peer_manager.go [cite: consensusnode/peer_manager.go]
			}
		},
	})
}

// min helper (nếu chưa có trong package)
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Make sure to add other necessary methods of ManagedNode if they were omitted for brevity
// e.g., RegisterStreamHandler, SendRequest, etc. from stream_manager.go
// and other helpers from different files if they are called directly or indirectly.
// The provided code focuses on the consensusLoop and its direct dependencies.
// Also, ensure that all imported packages (like "github.com/blockchain/consensus/dag", "github.com/blockchain/consensus/logger")
// are correctly pathed and accessible in your project structure.
// The placeholder functions like loadPrivateKey, RegisterStreamHandler, transactionsRequestHandler,
// displayNodeInfo, connectToBootstrapPeers, peerHealthMonitor, updatePeerStatus, tryReconnectToPeer,
// shouldReconnect, cancelAllReconnects, PublishMessage should be defined elsewhere in your consensusnode package.
