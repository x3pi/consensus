package consensusnode

import (
	"context"
	"fmt"
	"log" // Sử dụng log tiêu chuẩn, thay thế bằng logger của bạn nếu cần
	"sync"

	// "time" // Not directly used in this specific file after splitting, but might be needed if more logic is added back

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub" // Correct import for pubsub

	// "github.com/libp2p/go-libp2p/core/crypto" // crypto is used in utils.go
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	// "github.com/meta-node-blockchain/meta-node/pkg/logger" // Placeholder
	// "github.com/meta-node-blockchain/meta-node/pkg/storage" // Placeholder
)

// --- Định nghĩa Interface (nếu cần thiết cho các package khác tham chiếu) ---

// YourStorageInterface là một placeholder cho interface storage của bạn.
// Bạn cần định nghĩa interface này dựa trên package `storage` của bạn.
type YourStorageInterface interface {
	Get(key []byte) ([]byte, error)
	Put(key []byte, value []byte) error
	Delete(key []byte) error
	// Thêm các phương thức khác nếu cần
}

// --- Đối tượng Node Cốt lõi ---

// ManagedNode là đối tượng trung tâm cho các hoạt động mạng và đồng thuận.
type ManagedNode struct {
	config NodeConfig
	host   host.Host
	pubsub *pubsub.PubSub

	// TopicStorageMap quản lý các storage instance cho từng topic
	topicStorageMap sync.Map // Key: string (tên topic), Value: YourStorageInterface

	peers       map[peer.ID]*ManagedPeerInfo
	peerMutex   sync.RWMutex
	reconnectWG sync.WaitGroup // WaitGroup cho các goroutine kết nối lại

	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup // Để quản lý các goroutine

	// Stream Handlers
	streamHandlers map[protocol.ID]network.StreamHandler

	// Topic Subscriptions
	topicSubscriptions map[string]*pubsub.Subscription
	topicHandlers      map[string]func(msg *pubsub.Message) // Các handler cho message từ các topic cụ thể

	// Trạng thái nội bộ khác
	keyValueCache   *lru.Cache[string, []byte]
	transactionChan chan []byte
	fetchingBlocks  sync.Map // Theo dõi các block đang được tìm kiếm (key: uint64, value: bool)
	feeAddresses    []string
	feeAddressesMux sync.RWMutex
}

// NewManagedNode tạo và khởi tạo một ManagedNode mới.
func NewManagedNode(ctx context.Context, cfg NodeConfig) (*ManagedNode, error) {
	privKey, err := loadPrivateKey(cfg.PrivateKey) // utils.go
	if err != nil {
		return nil, fmt.Errorf("không thể tải khóa riêng tư: %w", err)
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
	// Sử dụng context của node cho PubSub để nó có thể được hủy cùng với node
	ps, err := pubsub.NewGossipSub(ctx, libp2pHost, psOptions...)
	if err != nil {
		_ = libp2pHost.Close() // Dọn dẹp host nếu pubsub thất bại
		return nil, fmt.Errorf("không thể tạo pubsub: %w", err)
	}

	nodeCtx, cancel := context.WithCancel(ctx)

	// Khởi tạo LRU cache
	cache, err := lru.New[string, []byte](cfg.KeyValueCacheSize)
	if err != nil {
		// Không gọi ps.Shutdown() vì không có phương thức này.
		// PubSub sẽ được dọn dẹp khi context của nó (ctx) bị hủy hoặc host đóng.
		_ = libp2pHost.Close() // Dọn dẹp host, điều này cũng sẽ ảnh hưởng đến PubSub
		cancel()               // Hủy context của node
		return nil, fmt.Errorf("không thể tạo LRU cache: %w", err)
	}

	mn := &ManagedNode{
		config:             cfg,
		host:               libp2pHost,
		pubsub:             ps,
		peers:              make(map[peer.ID]*ManagedPeerInfo),
		ctx:                nodeCtx,
		cancelFunc:         cancel,
		streamHandlers:     make(map[protocol.ID]network.StreamHandler),
		topicSubscriptions: make(map[string]*pubsub.Subscription),
		topicHandlers:      make(map[string]func(msg *pubsub.Message)),
		keyValueCache:      cache,
		transactionChan:    make(chan []byte, cfg.TransactionChanBuffer),
	}

	// Đăng ký stream handler mặc định nếu node type là "sub"
	// Giả sử BlockRequestProtocol được định nghĩa trong utils.go hoặc một file hằng số chung

	if mn.config.NodeType == "validator" {
		mn.RegisterStreamHandler(TransactionStreamProtocol, mn.transactionStreamHandler) // stream_manager.go
		log.Printf("Đã đăng ký stream handler cho %s (Loại Node: %s)", BlockRequestProtocol, mn.config.NodeType)
	}

	// Hiển thị thông tin Node
	mn.displayNodeInfo() // utils.go

	return mn, nil
}

// --- Phương thức Vòng đời ---

// Start khởi tạo các hoạt động của node, như kết nối tới peer và bắt đầu các dịch vụ.
func (mn *ManagedNode) Start() error {
	// Thiết lập các stream handler đã đăng ký
	for protoID, handler := range mn.streamHandlers {
		mn.host.SetStreamHandler(protoID, handler)
		log.Printf("Đã đăng ký stream handler cho protocol %s", protoID)
	}

	// Kết nối tới các bootstrap peer (peer_manager.go)
	if err := mn.connectToBootstrapPeers(); err != nil {
		log.Printf("LƯU Ý: Không thể kết nối tới một số bootstrap peer: %v", err)
	}

	// Bắt đầu theo dõi sức khỏe peer (peer_manager.go)
	mn.wg.Add(1)
	go mn.peerHealthMonitor()

	// Bắt đầu notifier sự kiện kết nối (peer_manager.go)
	mn.setupConnectionNotifier()

	// Việc đăng ký topic handler (SubscribeAndHandle) nên được thực hiện bởi
	// code sử dụng ManagedNode sau khi nó được tạo và trước khi Start được gọi,
	// hoặc thông qua một phương thức cấu hình riêng biệt trên ManagedNode.
	// Ví dụ:
	// for topicName, handler := range mn.topicHandlers { // Giả sử topicHandlers được điền trước khi Start
	// 	if err := mn.SubscribeAndHandle(topicName, handler); err != nil { // pubsub_manager.go
	// 		log.Printf("Lỗi khi đăng ký topic %s trong Start: %v", topicName, err)
	// 	}
	// }

	log.Printf("ManagedNode (%s) đã khởi động thành công.", mn.host.ID())
	return nil
}

// Stop tắt ManagedNode một cách an toàn.
func (mn *ManagedNode) Stop() error {
	log.Println("Đang dừng ManagedNode...")
	mn.cancelFunc() // Gửi tín hiệu cho tất cả goroutine dừng lại thông qua context của node

	// Hủy tất cả các goroutine kết nối lại đang chờ (peer_manager.go)
	mn.cancelAllReconnects()
	mn.reconnectWG.Wait() // Đợi tất cả các goroutine kết nối lại hoàn thành

	// Đóng host (sẽ đóng các kết nối, stream, và cũng sẽ ảnh hưởng đến PubSub)
	if err := mn.host.Close(); err != nil {
		log.Printf("Lỗi khi đóng libp2p host: %v", err)
		// Không cần gọi mn.pubsub.Shutdown() vì không có phương thức đó.
		// PubSub được gắn với vòng đời của host và context của nó.
	}

	mn.wg.Wait() // Đợi tất cả các goroutine chính của ManagedNode hoàn thành
	log.Println("ManagedNode đã dừng.")
	return nil
}

// --- Getters cơ bản ---

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
// Trả về bản sao để đảm bảo tính bất biến nếu cần.
func (mn *ManagedNode) Config() NodeConfig {
	return mn.config
}
