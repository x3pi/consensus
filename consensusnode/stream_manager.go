package consensusnode

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// --- Protocol IDs ---
const BlockRequestProtocol protocol.ID = "/meta-node/block-request/1.0.0"
const TransactionStreamProtocol protocol.ID = "/meta-node/transaction-stream/1.0.0"

// --- Stream Handlers ---
// transactionStreamHandler xử lý các stream đến mang theo mảng giao dịch.
// Hàm này chỉ chấp nhận stream từ Master Node đã được cấu hình.
func (mn *ManagedNode) transactionStreamHandler(stream network.Stream) {
	remotePeerID := stream.Conn().RemotePeer()
	log.Printf("Đã nhận transaction stream từ peer: %s trên protocol %s", remotePeerID, stream.Protocol())

	// --- KIỂM TRA MASTER NODE ---
	var configuredMasterPeerID peer.ID
	if mn.config.MasterNodeAddress != "" {
		masterAddrInfo, err := peer.AddrInfoFromString(mn.config.MasterNodeAddress)
		if err != nil {
			log.Printf("❌ Lỗi nghiêm trọng: Không thể phân tích MasterNodeAddress từ cấu hình '%s': %v. Từ chối stream từ %s.", mn.config.MasterNodeAddress, err, remotePeerID)
			_ = stream.Reset() // Reset stream vì cấu hình master node có vấn đề
			return
		}
		configuredMasterPeerID = masterAddrInfo.ID
	} else {
		log.Printf("⚠️ Cảnh báo: MasterNodeAddress chưa được cấu hình. Không thể xác thực nguồn gốc transaction stream. Từ chối stream từ %s.", remotePeerID)
		_ = stream.Reset() // Reset stream vì không có master node nào được cấu hình để so sánh
		return
	}

	if remotePeerID != configuredMasterPeerID {
		log.Printf("🚫 CẢNH BÁO: Đã nhận transaction stream từ một peer KHÔNG PHẢI là Master Node đã cấu hình (%s). Peer gửi: %s. Stream sẽ bị từ chối.", configuredMasterPeerID, remotePeerID)
		_ = stream.Reset() // Reset stream vì không phải từ Master Node
		return
	}
	// --- KẾT THÚC KIỂM TRA MASTER NODE ---

	log.Printf("✅ Transaction stream từ Master Node %s được chấp nhận. Tiếp tục xử lý...", remotePeerID)

	defer func() {
		if errClose := stream.Close(); errClose != nil {
			log.Printf("Lỗi khi đóng transaction stream từ Master Node %s: %v", remotePeerID, errClose)
		} else {
			log.Printf("Đã đóng transaction stream từ Master Node %s", remotePeerID)
		}
	}()
	log.Printf("TXS: 1 %v", mn.config.MaxMessageSize)

	limitedReader := io.LimitReader(stream, int64(mn.config.MaxMessageSize))
	rawData, err := io.ReadAll(limitedReader)
	log.Printf("TXS: 1.0")

	if err != nil {
		log.Printf("❌ Lỗi đọc dữ liệu từ transaction stream (Master Node %s): %v", remotePeerID, err)
		_ = stream.Reset()
		log.Printf("TXS: 1.1")

		return
	}
	log.Printf("TXS: 2")

	if len(rawData) == 0 {
		log.Printf("⚠️ Transaction stream từ Master Node %s không có dữ liệu.", remotePeerID)
		return
	}
	// Chỉ in dữ liệu nhận được dưới dạng hex
	log.Println(string(rawData))
	log.Printf("TXS: 3")

	// Dòng log chi tiết về số byte nhận được và "Đang giải mã..." đã được loại bỏ để đơn giản hóa.
	// Nếu bạn vẫn muốn giữ thông tin về số byte, bạn có thể thêm lại một dòng log tối giản hơn ở đây.
}

// blockRequestHandler là một ví dụ về stream handler.
func (mn *ManagedNode) blockRequestHandler(stream network.Stream) {
	peerID := stream.Conn().RemotePeer()
	log.Printf("Đã nhận block request từ peer: %s trên protocol %s (Cần triển khai logic chi tiết)", peerID, stream.Protocol())
	defer stream.Close()
	// ... (logic xử lý block request)
}

// --- Quản lý Stream Handler ---
func (mn *ManagedNode) RegisterStreamHandler(protoID protocol.ID, handler network.StreamHandler) {
	if handler == nil {
		log.Printf("Cảnh báo: Đã cố gắng đăng ký stream handler nil cho protocol %s", protoID)
		return
	}
	mn.streamHandlers[protoID] = handler
	if mn.host != nil {
		mn.host.SetStreamHandler(protoID, handler)
	} else {
		log.Printf("Host chưa được khởi tạo, stream handler cho %s sẽ được thiết lập khi Start()", protoID)
	}
}

// --- Gửi Dữ liệu qua Stream ---
func (mn *ManagedNode) SendRequest(ctx context.Context, targetPeerID peer.ID, protoID protocol.ID, message []byte) ([]byte, error) {
	if targetPeerID == mn.host.ID() {
		return nil, fmt.Errorf("không thể gửi request tới chính mình")
	}
	if mn.host.Network().Connectedness(targetPeerID) != network.Connected {
		return nil, fmt.Errorf("không kết nối tới peer %s để gửi request qua protocol %s", targetPeerID, protoID)
	}

	log.Printf("Đang mở stream tới %s cho protocol %s", targetPeerID, protoID)
	stream, err := mn.host.NewStream(ctx, targetPeerID, protoID)
	if err != nil {
		return nil, fmt.Errorf("không thể mở stream tới %s cho protocol %s: %w", targetPeerID, protoID, err)
	}
	defer func() {
		if errClose := stream.Close(); errClose != nil {
			log.Printf("Lỗi khi đóng stream (SendRequest) tới %s: %v", targetPeerID, errClose)
		}
	}()

	log.Printf("Đang gửi yêu cầu tới %s qua %s (%d bytes)", targetPeerID, protoID, len(message))
	_, err = stream.Write(message)
	if err != nil {
		_ = stream.Reset()
		return nil, fmt.Errorf("không thể ghi vào stream: %w", err)
	}

	err = stream.CloseWrite()
	if err != nil {
		_ = stream.Reset()
		return nil, fmt.Errorf("không thể đóng phía ghi của stream: %w", err)
	}

	reader := bufio.NewReader(stream)
	var responseBuffer []byte
	responseBuffer, err = io.ReadAll(reader)
	if err != nil && err != io.EOF {
		return responseBuffer, fmt.Errorf("không thể đọc toàn bộ phản hồi từ stream: %w (đã đọc %d bytes)", err, len(responseBuffer))
	}

	log.Printf("Đã nhận phản hồi từ %s (%d bytes) cho protocol %s", targetPeerID, len(responseBuffer), protoID)
	return responseBuffer, nil
}

// --- Helper Functions ---
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
