package consensusnode

import (
	"encoding/base64"
	"fmt"
	"log"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// BlockRequestProtocol định nghĩa ID protocol cho yêu cầu block.
// Đảm bảo hằng số này nhất quán với định nghĩa ở các file khác nếu có.
// const BlockRequestProtocol protocol.ID = "/meta-node/block-request/1.0.0" // Đã định nghĩa ở managed_node.go
const BlockRequestProtocol protocol.ID = "/meta-node/block-request/1.0.0"

// loadPrivateKey là một helper để tải hoặc tạo khóa riêng tư.
// Sử dụng base64 decoding chuẩn.
func loadPrivateKey(keyStr string) (crypto.PrivKey, error) {
	if keyStr == "" {
		log.Println("Không tìm thấy khóa riêng tư trong cấu hình, đang tạo khóa mới...")
		// Sử dụng Ed25519 làm mặc định vì nó hiệu quả và an toàn
		priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
		if err != nil {
			return nil, fmt.Errorf("không thể tạo cặp khóa Ed25519: %w", err)
		}
		// Log khóa mới được tạo (chỉ cho mục đích dev/test, không bao giờ log private key trong production)
		// encoded, _ := crypto.MarshalPrivateKey(priv)
		// log.Printf("Khóa riêng tư mới (base64): %s", base64.StdEncoding.EncodeToString(encoded))
		return priv, nil
	}
	keyBytes, err := base64.StdEncoding.DecodeString(keyStr)
	if err != nil {
		return nil, fmt.Errorf("không thể giải mã base64 cho khóa riêng tư: %w", err)
	}
	privKey, err := crypto.UnmarshalPrivateKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("không thể unmarshal khóa riêng tư: %w", err)
	}
	log.Println("Đã tải khóa riêng tư từ cấu hình thành công.")
	return privKey, nil
}

// displayNodeInfo hiển thị thông tin của node.
// Đây là một phương thức của ManagedNode để truy cập host và config.
func (mn *ManagedNode) displayNodeInfo() {
	if mn.host == nil {
		log.Println("Host chưa được khởi tạo, không thể hiển thị thông tin node.")
		return
	}
	log.Printf("===== Thông tin Node =====")
	log.Printf("Node ID: %s", mn.host.ID())
	log.Printf("Loại Node: %s", mn.config.NodeType)
	log.Println("Địa chỉ lắng nghe:")
	if len(mn.host.Addrs()) == 0 {
		log.Println("  (Không có địa chỉ lắng nghe nào được thiết lập hoặc host chưa sẵn sàng)")
	}
	for _, addr := range mn.host.Addrs() {
		log.Printf("  %s/p2p/%s", addr, mn.host.ID())
	}
	log.Printf("==========================")
}
