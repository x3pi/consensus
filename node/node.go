package node

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Network struct {
		ListenAddress  string   `yaml:"listen_address"`
		BootstrapPeers []string `yaml:"bootstrap_peers"`
	} `yaml:"network"`
	Libp2p struct {
		PrivateKey         string   `yaml:"private_key"`
		SupportedProtocols []string `yaml:"supported_protocols"`
	} `yaml:"libp2p"`
	Consensus struct {
		Algorithm      string      `yaml:"algorithm"`
		RaftConfig     interface{} `yaml:"raft_config"` // Thay bằng struct cụ thể nếu dùng Raft
		StateStorePath string      `yaml:"state_store_path"`
	} `yaml:"consensus"`
	Logging struct {
		Level string `yaml:"level"`
	} `yaml:"logging"`
}

func LoadConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var cfg Config
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&cfg)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}

func StartNode(configPath string) error {
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}
	// 1. Khởi tạo libp2p host
	listenAddr, err := multiaddr.NewMultiaddr(cfg.Network.ListenAddress)
	if err != nil {
		return fmt.Errorf("failed to parse listen address: %w", err)
	}
	// Load private key từ cấu hình và tạo identity (sử dụng base64)
	keyStr := cfg.Libp2p.PrivateKey
	keyBytes, err := base64.StdEncoding.DecodeString(keyStr)
	if err != nil {
		return fmt.Errorf("failed to decode private key from base64: %w", err)
	}

	privKey, err := crypto.UnmarshalPrivateKey(keyBytes)
	if err != nil {
		return fmt.Errorf("failed to unmarshal private key: %w", err)
	}

	h, err := libp2p.New(
		libp2p.ListenAddrs(listenAddr),
		libp2p.Identity(privKey), // Sử dụng private key để thiết lập identity
	)
	if err != nil {
		return fmt.Errorf("failed to create libp2p host: %w", err)
	}
	fmt.Println("Libp2p host started. Peer ID:", h.ID())
	fmt.Println("Listening on:", h.Addrs())

	// 2. Implement logic đồng thuận và thiết lập handler
	fmt.Println("Consensus algorithm:", cfg.Consensus.Algorithm)
	// TODO: Khởi tạo và cấu hình cơ chế đồng thuận dựa trên cfg.Consensus

	for _, protocolStr := range cfg.Libp2p.SupportedProtocols {
		protocol := protocol.ID(protocolStr) // Ép kiểu string sang protocol.ID
		fmt.Println("Supporting protocol:", protocol)
		h.SetStreamHandler(protocol, func(s network.Stream) {
			fmt.Println("Received new stream:", s.Conn().RemotePeer())
			// TODO: Xử lý stream cho giao thức đồng thuận
		})
	}

	// 3. Kết nối đến các peer ban đầu
	for _, peerAddrStr := range cfg.Network.BootstrapPeers {
		peerAddr, err := multiaddr.NewMultiaddr(peerAddrStr)
		if err != nil {
			fmt.Printf("failed to parse bootstrap peer address '%s': %v\n", peerAddrStr, err) // Đã sửa thành %v vì không còn dùng %w
			continue
		}
		peerInfo, err := peer.AddrInfoFromP2pAddr(peerAddr)
		if err != nil {
			fmt.Printf("failed to get peer info from address '%s': %v\n", peerAddrStr, err) // Đã sửa thành %v vì không còn dùng %w
			continue
		}

		err = h.Connect(context.Background(), *peerInfo)
		if err != nil {
			fmt.Printf("failed to connect to bootstrap peer '%s': %v\n", peerAddrStr, err) // Đã sửa thành %v vì không còn dùng %w
			continue
		}
		fmt.Println("Connected to bootstrap peer:", peerInfo.ID)
	}

	select {} // Giữ cho chương trình chạy vô thời hạn
}
