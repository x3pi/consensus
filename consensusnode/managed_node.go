package consensusnode

import (
	"context"
	"math/rand"

	// "crypto/ecdsa" // Không cần trực tiếp nếu dùng crypto.Secp256k1PrivateKey
	"encoding/hex"
	"errors"
	"fmt"
	"log"
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
	"github.com/blockchain/consensus/dag"
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
	if ownPubKeyHex != "" && cfg.InitialStake > 0 {
		initialStakeData[ownPubKeyHex] = cfg.InitialStake
	}
	// Bạn có thể cần thêm logic để điền stake cho các validator khác ở đây.
	dagStoreInstance := dag.NewDagStore(initialStakeData)

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
		mn.RegisterStreamHandler(TransactionsRequestProtocol, mn.transactionsRequestHandler) // stream_manager.go
		log.Printf("Đã đăng ký stream handler cho %s (Loại Node: %s)", TransactionsRequestProtocol, mn.config.NodeType)
	}

	// Hiển thị thông tin Node
	mn.displayNodeInfo() // utils.go

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
	if err := mn.connectToBootstrapPeers(); err != nil { // peer_manager.go
		log.Printf("LƯU Ý: Không thể kết nối tới một số bootstrap peer: %v", err)
		// Không trả về lỗi ở đây để node vẫn có thể khởi động
	}

	// Bắt đầu theo dõi sức khỏe peer
	mn.wg.Add(1)
	go mn.peerHealthMonitor() // peer_manager.go

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

	mn.cancelAllReconnects() // peer_manager.go
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

// // selectConsensusPartner chọn một node đối tác để đồng bộ và tạo event.
// func (mn *ManagedNode) selectConsensusPartner() (NodeID, peer.ID, error) {
// 	currentDagStore := mn.GetDagStore()
// 	if currentDagStore == nil {
// 		return "", "", errors.New("DagStore is not available in ManagedNode")
// 	}

// 	ownKeyHex, err := mn.getOwnPublicKeyHex()
// 	if err != nil {
// 		return "", "", fmt.Errorf("không thể lấy public key của chính node: %w", err)
// 	}
// 	if ownKeyHex == "" { // Kiểm tra quan trọng
// 		return "", "", errors.New("public key của chính node là rỗng, không thể tiếp tục lựa chọn node")
// 	}

// 	connectedPeersMap, err := mn.getConnectedPeerPublicKeys()
// 	if err != nil {
// 		return "", "", fmt.Errorf("không thể lấy public key của các peer đang kết nối: %w", err)
// 	}
// 	logger.Info("connectedPeersMap: ", connectedPeersMap)

// 	if len(connectedPeersMap) == 0 {
// 		log.Println("Không có peer nào đang kết nối với public key đã biết để chọn làm đối tác.")
// 		return "", "", errors.New("no connected peers with known public keys for selection")
// 	}

// 	var candidateNodeIDs []NodeID
// 	nodeHeights := make(map[NodeID]uint64)
// 	nodeInDegrees := make(map[NodeID]uint64)
// 	nodeIDToPeerIDMap := make(map[NodeID]peer.ID)

// 	for pID, peerPubKeyHex := range connectedPeersMap {
// 		if peerPubKeyHex == ownKeyHex { // Bỏ qua chính mình
// 			continue
// 		}
// 		currentNodeID := GetNodeIDFromString(peerPubKeyHex) // Từ node_selection.go

// 		h, hOk := currentDagStore.GetHeightForNode(peerPubKeyHex)   // Truyền chuỗi hex
// 		i, iOk := currentDagStore.GetInDegreeForNode(peerPubKeyHex) // Truyền chuỗi hex

// 		if hOk && iOk {
// 			logger.Info("currentNodeID", currentNodeID)
// 			candidateNodeIDs = append(candidateNodeIDs, currentNodeID)
// 			nodeHeights[currentNodeID] = h
// 			nodeInDegrees[currentNodeID] = i
// 			nodeIDToPeerIDMap[currentNodeID] = pID
// 		} else {
// 			logger.Error("candidateNodeIDs error")
// 			// log.Printf("Bỏ qua ứng cử viên %s (PeerID: %s) do thiếu dữ liệu height (ok: %t) hoặc in-degree (ok: %t).",
// 			//	currentNodeID, pID, hOk, iOk)
// 		}
// 	}

// 	if len(candidateNodeIDs) == 0 {
// 		log.Println("Không có ứng cử viên hợp lệ nào sau khi lọc (ví dụ: tất cả đều là chính mình hoặc thiếu dữ liệu H/I).")
// 		return "", "", errors.New("no valid candidates after filtering (e.g., all are self or missing H/I data)")
// 	}

// 	// Gọi thuật toán lựa chọn node
// 	selectedNodeID, err := SelectReferenceNode(nodeHeights, nodeInDegrees, candidateNodeIDs)
// 	if err != nil {
// 		log.Printf("Lỗi khi chọn node tham chiếu: %v", err)
// 		return "", "", err
// 	}

// 	selectedPID, pidExists := nodeIDToPeerIDMap[selectedNodeID]
// 	if !pidExists {
// 		log.Printf("Lỗi nghiêm trọng: Không tìm thấy PeerID cho NodeID đã chọn %s", selectedNodeID)
// 		return "", "", fmt.Errorf("internal error: could not map selected NodeID %s back to PeerID", selectedNodeID)
// 	}

// 	log.Printf("Node tham chiếu được chọn cho đồng thuận: %s (PeerID: %s)", selectedNodeID, selectedPID)
// 	return selectedNodeID, selectedPID, nil
// }

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
	logger.Info("connectedPeersMap: ", connectedPeersMap)

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
		currentNodeID := GetNodeIDFromString(peerPubKeyHex) // Từ node_selection.go (vẫn dùng GetNodeIDFromString)

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

// consensusLoop là vòng lặp chính thực hiện các bước đồng thuận (ví dụ đơn giản hóa).
func (mn *ManagedNode) consensusLoop() {
	defer mn.wg.Done()
	// Tần suất thực hiện một vòng đồng thuận
	ticker := time.NewTicker(15 * time.Second) // Ví dụ: 15 giây
	defer ticker.Stop()

	log.Println("Bắt đầu vòng lặp đồng thuận chính...")

	for {
		select {
		case <-mn.ctx.Done():
			log.Println("Dừng vòng lặp đồng thuận do context của node đã hủy.")
			return
		case <-ticker.C:
			log.Println("--- Vòng đồng thuận mới ---")

			partnerNodeID, partnerPeerID, err := mn.selectConsensusPartner()
			if err != nil {
				logger.Error(err)
				log.Printf("Không thể chọn node đối tác trong vòng này: %v", err)
				continue // Chuyển sang vòng lặp tiếp theo
			}
			log.Printf("Đã chọn đối tác: NodeID=%s, PeerID=%s", partnerNodeID, partnerPeerID)

			// TODO: Bước 2: Yêu cầu đồng bộ (Request Sync) với partnerPeerID
			// Ví dụ: mn.requestSyncWithPeer(partnerPeerID, string(partnerNodeID))
			log.Printf("Bước tiếp theo (chưa triển khai): Yêu cầu đồng bộ với %s...", partnerPeerID)

			// TODO: Bước 3: Đồng bộ event (Sync all known events)
			// Xử lý phản hồi từ sync request, thêm event vào DagStore.

			// TODO: Bước 4: Tạo Event Block mới (Event block creation)
			// Tham chiếu self-parent và other-parent (từ partnerNodeID).
			// Thêm event mới vào DagStore, broadcast.
			// Ví dụ: mn.createNewEventBlock(string(partnerNodeID))
			log.Printf("Bước tiếp theo (chưa triển khai): Tạo event block mới...")

			// TODO: Bước 5: Chạy Decima selection và Atropos time consensus (nếu cần)
			// Ví dụ: mn.dagStore.DecideClotho()
			log.Printf("Bước tiếp theo (chưa triển khai): Chạy thuật toán quyết định Clotho/Atropos...")

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
			logger.Error("peerID", peerID)
			pubKey := conn.RemotePublicKey() // Lấy public key từ kết nối
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
					logger.Info(mn.connectedPeerPubKeys)
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
			mn.updatePeerStatus(peerID, PeerConnected, nil, peerType) // peer_manager.go
			mn.host.Peerstore().AddAddr(peerID, conn.RemoteMultiaddr(), peerstore.ConnectedAddrTTL)
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
			mn.updatePeerStatus(peerID, PeerDisconnected, errors.New("đã ngắt kết nối"), peerType) // peer_manager.go
			if exists && shouldReconnect(pInfo.Type, mn.config) {                                  // peer_manager.go
				log.Printf("Lên lịch kết nối lại cho peer quan trọng %s (Loại: %s)", peerID, pInfo.Type)
				mn.tryReconnectToPeer(peerID, pInfo.Type) // peer_manager.go
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
