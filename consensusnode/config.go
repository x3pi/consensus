package consensusnode

import (
	"time"

	"github.com/libp2p/go-libp2p/core/protocol"
)

// --- Cấu hình ---

// NodeConfig chứa tất cả các tham số cấu hình cho node.
// Đã thêm các `yaml` tags để ánh xạ chính xác với tệp node_config.yaml
type NodeConfig struct {
	ListenAddress         string        `yaml:"listenAddress"`
	PrivateKey            string        `yaml:"privateKey"`
	BootstrapPeers        []string      `yaml:"bootstrapPeers"`
	NodeType              string        `yaml:"nodeType"`
	SupportedProtocols    []protocol.ID `yaml:"supportedProtocols"` // Lưu ý: protocol.ID có thể cần custom unmarshaler nếu không hoạt động trực tiếp
	RootPath              string        `yaml:"rootPath"`
	TransactionChanBuffer int           `yaml:"transactionChanBuffer"`
	MasterNodeAddress     string        `yaml:"masterNodeAddress"` // Thêm địa chỉ Master Node

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
}

// DefaultNodeConfig cung cấp các giá trị mặc định hợp lý.
func DefaultNodeConfig() NodeConfig {
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
		SupportedProtocols:    []protocol.ID{BlockRequestProtocol},
	}
}
