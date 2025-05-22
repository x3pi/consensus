package consensusnode

import (
	"time"

	"github.com/libp2p/go-libp2p/core/protocol"
)

// NodeConfig holds all configuration parameters for a ManagedNode.
type NodeConfig struct {
	// ListenAddress is the multiaddress string the node will listen on.
	// Example: "/ip4/0.0.0.0/tcp/9001"
	ListenAddress string `yaml:"listenAddress"`
	// PrivateKey is the base64 encoded private key for the node.
	// If empty, a new key will be generated.
	PrivateKey string `yaml:"privateKey"`
	// BootstrapPeers is a list of multiaddresses for bootstrap peer nodes.
	BootstrapPeers []string `yaml:"bootstrapPeers"`
	// NodeType specifies the type of the node (e.g., "master", "validator").
	NodeType string `yaml:"nodeType"`
	// SupportedProtocols is a list of custom protocol.IDs supported by this node.
	// Note: protocol.ID might require a custom unmarshaler if complex types are directly used in YAML.
	SupportedProtocols []protocol.ID `yaml:"supportedProtocols"`
	// RootPath is the root directory path for node data storage (e.g., databases, logs).
	RootPath string `yaml:"rootPath"`
	// TransactionChanBuffer is the buffer size for the internal transaction channel.
	TransactionChanBuffer int `yaml:"transactionChanBuffer"`
	// MasterNodeAddress is the multiaddress of a designated master node, if applicable.
	MasterNodeAddress string `yaml:"masterNodeAddress"`

	// Connection Management settings
	// MinConnections is the minimum number of peer connections the node will attempt to maintain.
	MinConnections int `yaml:"minConnections"`
	// MaxConnections is the maximum number of peer connections allowed.
	MaxConnections int `yaml:"maxConnections"`
	// ConnectionGracePeriod is the duration for which new connections are immune to pruning by the connection manager.
	ConnectionGracePeriod time.Duration `yaml:"connectionGracePeriod"`
	// InitialReconnectDelay is the initial delay before attempting to reconnect to a lost peer.
	InitialReconnectDelay time.Duration `yaml:"initialReconnectDelay"`
	// MaxReconnectDelay is the maximum delay between reconnect attempts.
	MaxReconnectDelay time.Duration `yaml:"maxReconnectDelay"`
	// MaxReconnectAttempts is the maximum number of times to attempt reconnection before giving up.
	// 0 or a negative value means infinite attempts.
	MaxReconnectAttempts int `yaml:"maxReconnectAttempts"`
	// PingInterval is the interval at which to send pings to connected peers to check liveness.
	PingInterval time.Duration `yaml:"pingInterval"`
	// PingTimeout is the maximum time to wait for a ping response.
	PingTimeout time.Duration `yaml:"pingTimeout"`

	// PubSub Settings
	// MaxMessageSize is the maximum allowed size for PubSub messages in bytes.
	MaxMessageSize int `yaml:"maxMessageSize"`

	// Cache Settings
	// KeyValueCacheSize is the maximum number of items in the key-value LRU cache.
	KeyValueCacheSize int `yaml:"keyValueCacheSize"`
	// ConsensusTickInterval is the frequency at which the consensus loop runs.
	ConsensusTickInterval time.Duration `yaml:"consensusTickInterval"`

	// InitialStake is the initial stake amount for this node if it acts as a validator.
	// The `omitempty` tag means if this field is not in the YAML,
	// it will not cause an error, and its zero value (0) will be used.
	InitialStake uint64 `yaml:"initialStake,omitempty"` //

	AllStakers []StakerInfo `yaml:"allStakers,omitempty"` // MỚI: Danh sách tất cả staker trong mạng

}

// DefaultNodeConfig provides sensible default values for NodeConfig.
func DefaultNodeConfig() NodeConfig {
	// By default, SupportedProtocols is empty. Specific protocols should be
	// added by the application based on its requirements.
	// For example, if TransactionsRequestProtocol is defined:
	// defaultSupportedProtocols = []protocol.ID{TransactionsRequestProtocol}
	var defaultSupportedProtocols []protocol.ID

	return NodeConfig{
		ListenAddress: "/ip4/0.0.0.0/tcp/0", // Automatically select a port.
		NodeType:      "generic",

		MinConnections:        5,
		MaxConnections:        20,
		ConnectionGracePeriod: 10 * time.Second,
		InitialReconnectDelay: 2 * time.Second,
		MaxReconnectDelay:     30 * time.Second,
		MaxReconnectAttempts:  5,
		PingInterval:          30 * time.Second,
		PingTimeout:           5 * time.Second,

		MaxMessageSize: 10 * 1024 * 1024, // 10MB

		RootPath:              "./node_data",
		TransactionChanBuffer: 1000,
		KeyValueCacheSize:     1000,
		SupportedProtocols:    defaultSupportedProtocols,
		ConsensusTickInterval: 10 * time.Second, // Default consensus tick.

		InitialStake: 0, // Default initial stake. Can be overridden by config.
	}
}

// MỚI: Cấu trúc thông tin staker
type StakerInfo struct {
	PubKeyHex string `yaml:"pubKeyHex"` // Public key dạng hex string của staker
	Stake     uint64 `yaml:"stake"`     // Lượng stake của staker
}
