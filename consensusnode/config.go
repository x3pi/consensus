package consensusnode

import (
	"time"

	"github.com/libp2p/go-libp2p/core/protocol"
)

// NodeConfig chứa tất cả các tham số cấu hình cho node.
type NodeConfig struct {
	ListenAddress         string        `yaml:"listenAddress"`
	PrivateKey            string        `yaml:"privateKey"`
	BootstrapPeers        []string      `yaml:"bootstrapPeers"`
	NodeType              string        `yaml:"nodeType"`
	SupportedProtocols    []protocol.ID `yaml:"supportedProtocols"` // Lưu ý: protocol.ID có thể cần custom unmarshaler
	RootPath              string        `yaml:"rootPath"`
	TransactionChanBuffer int           `yaml:"transactionChanBuffer"`
	MasterNodeAddress     string        `yaml:"masterNodeAddress"`

	// Quản lý kết nối
	MinConnections        int           `yaml:"minConnections"`
	MaxConnections        int           `yaml:"maxConnections"`
	ConnectionGracePeriod time.Duration `yaml:"connectionGracePeriod"`
	InitialReconnectDelay time.Duration `yaml:"initialReconnectDelay"`
	MaxReconnectDelay     time.Duration `yaml:"maxReconnectDelay"`
	MaxReconnectAttempts  int           `yaml:"maxReconnectAttempts"`
	PingInterval          time.Duration `yaml:"pingInterval"`
	PingTimeout           time.Duration `yaml:"pingTimeout"`

	// Cài đặt PubSub
	MaxMessageSize int `yaml:"maxMessageSize"`

	// Cài đặt Cache
	KeyValueCacheSize int `yaml:"keyValueCacheSize"`

	// *** THÊM TRƯỜNG NÀY ĐỂ LƯU STAKE BAN ĐẦU CỦA NODE ***
	InitialStake uint64 `yaml:"initialStake,omitempty"` // Stake ban đầu của node này nếu nó là validator.
	// `omitempty` nghĩa là nếu giá trị này không có trong YAML,
	// nó sẽ không gây lỗi và giá trị mặc định (0) sẽ được sử dụng.
}

// DefaultNodeConfig cung cấp các giá trị mặc định hợp lý.
func DefaultNodeConfig() NodeConfig {
	// Giả sử TransactionsRequestProtocol đã được định nghĩa ở đâu đó, ví dụ trong stream_manager.go
	// const TransactionsRequestProtocol protocol.ID = "/meta-node/transactions-request/1.0.0"
	// Nếu chưa, bạn cần định nghĩa nó hoặc xóa khỏi SupportedProtocols mặc định.
	// Để mã này biên dịch được, tôi sẽ tạm comment dòng sử dụng nó nếu nó chưa được định nghĩa.
	var defaultSupportedProtocols []protocol.ID
	// if TransactionsRequestProtocol != "" { // Kiểm tra xem hằng số có tồn tại không (cách này không chuẩn)
	// defaultSupportedProtocols = []protocol.ID{TransactionsRequestProtocol}
	// }

	return NodeConfig{
		ListenAddress:         "/ip4/0.0.0.0/tcp/0", // Tự động chọn port
		NodeType:              "generic",
		MinConnections:        5,
		MaxConnections:        20,
		ConnectionGracePeriod: 10 * time.Second,
		InitialReconnectDelay: 2 * time.Second,
		MaxReconnectDelay:     30 * time.Second,
		MaxReconnectAttempts:  5,
		PingInterval:          30 * time.Second,
		PingTimeout:           5 * time.Second,
		MaxMessageSize:        10 * 1024 * 1024, // 10MB
		RootPath:              "./node_data",
		TransactionChanBuffer: 1000,
		KeyValueCacheSize:     1000,
		SupportedProtocols:    defaultSupportedProtocols, // Sử dụng slice đã kiểm tra

		// *** GÁN GIÁ TRỊ MẶC ĐỊNH CHO TRƯỜNG MỚI ***
		InitialStake: 0, // Hoặc một giá trị mặc định khác nếu bạn muốn, ví dụ: 100
	}
}
