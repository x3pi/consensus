package consensusnode

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// BlockRequestProtocol định nghĩa ID protocol cho yêu cầu block.
// Đảm bảo hằng số này nhất quán với định nghĩa ở các file khác nếu có.
// const BlockRequestProtocol protocol.ID = "/meta-node/block-request/1.0.0" // Đã định nghĩa ở managed_node.go

// --- Xử lý Stream ---

// RegisterStreamHandler đăng ký một handler cho một protocol ID cụ thể.
func (mn *ManagedNode) RegisterStreamHandler(protoID protocol.ID, handler network.StreamHandler) {
	if handler == nil {
		log.Printf("Cảnh báo: Đã cố gắng đăng ký stream handler nil cho protocol %s", protoID)
		return
	}
	mn.streamHandlers[protoID] = handler
	if mn.host != nil { // Nếu host đã được khởi tạo, thiết lập trực tiếp
		mn.host.SetStreamHandler(protoID, handler)
		log.Printf("Đã đăng ký stream handler cho protocol %s (từ RegisterStreamHandler)", protoID)
	} else {
		log.Printf("Host chưa được khởi tạo, stream handler cho %s sẽ được thiết lập khi Start()", protoID)
	}
}

// SendRequest mở một stream mới tới một peer và gửi một message.
// Đây là một cơ chế request/response chung.
func (mn *ManagedNode) SendRequest(ctx context.Context, targetPeerID peer.ID, protoID protocol.ID, message []byte) ([]byte, error) {
	if targetPeerID == mn.host.ID() {
		return nil, errors.New("không thể gửi request tới chính mình")
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
			log.Printf("Lỗi khi đóng stream tới %s: %v", targetPeerID, errClose)
		}
	}()

	log.Printf("Đang gửi yêu cầu tới %s qua %s (%d bytes)", targetPeerID, protoID, len(message))
	_, err = stream.Write(message)
	if err != nil {
		_ = stream.Reset() // Cố gắng reset stream nếu ghi lỗi
		return nil, fmt.Errorf("không thể ghi vào stream: %w", err)
	}

	err = stream.CloseWrite()
	if err != nil {
		_ = stream.Reset()
		return nil, fmt.Errorf("không thể đóng phía ghi của stream: %w", err)
	}

	reader := bufio.NewReader(stream)
	var responseBuffer bytes.Buffer
	// Đọc từng phần với buffer để tránh cấp phát bộ nhớ lớn không cần thiết
	readBuffer := make([]byte, 4096) // Kích thước buffer đọc
	for {
		n, errRead := reader.Read(readBuffer)
		if n > 0 {
			responseBuffer.Write(readBuffer[:n])
		}
		if errRead != nil {
			if errRead == io.EOF {
				break
			}
			// Không reset stream ở đây vì có thể đã nhận được một phần dữ liệu
			return responseBuffer.Bytes(), fmt.Errorf("không thể đọc toàn bộ phản hồi từ stream: %w (đã đọc %d bytes)", errRead, responseBuffer.Len())
		}
	}

	log.Printf("Đã nhận phản hồi từ %s (%d bytes) cho protocol %s", targetPeerID, responseBuffer.Len(), protoID)
	return responseBuffer.Bytes(), nil
}

// blockRequestHandler là một ví dụ về stream handler.
// Cần được điều chỉnh cho logic xử lý block request cụ thể của bạn.
func (mn *ManagedNode) blockRequestHandler(stream network.Stream) {
	peerID := stream.Conn().RemotePeer()
	log.Printf("Đã nhận block request từ peer: %s trên protocol %s", peerID, stream.Protocol())
	defer func() {
		if errClose := stream.Close(); errClose != nil {
			log.Printf("Lỗi khi đóng stream (blockRequestHandler) từ %s: %v", peerID, errClose)
		}
	}()

	// Ví dụ: Đọc yêu cầu tên file
	reader := bufio.NewReader(stream)
	fileName, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("❌ Lỗi đọc dữ liệu từ stream (blockRequestHandler) từ %s: %v", peerID, err)
		_ = stream.Reset() // Cố gắng reset stream
		return
	}
	fileName = strings.TrimSpace(fileName)
	log.Printf("📥 Yêu cầu nhận (blockRequestHandler) từ %s: %s", peerID, fileName)

	// Xử lý yêu cầu:
	// 1. Kiểm tra xem file/block có tồn tại không (dựa trên `fileName`).
	// 2. Nếu có, đọc nội dung và gửi lại.
	// 3. Nếu không, gửi thông báo lỗi.

	// Ví dụ: Giả sử file "example_block.data" tồn tại và chứa "Đây là dữ liệu block mẫu."
	if fileName == "example_block.data" {
		blockData := []byte("Đây là dữ liệu block mẫu cho " + fileName + "\n")
		_, err = stream.Write(blockData)
		if err != nil {
			log.Printf("❌ Lỗi gửi dữ liệu block (blockRequestHandler) tới %s: %v", peerID, err)
			_ = stream.Reset()
			return
		}
		log.Printf("✅ Đã gửi dữ liệu block '%s' tới %s", fileName, peerID)
	} else {
		errorMessage := []byte("Lỗi: File hoặc block '" + fileName + "' không tìm thấy.\n")
		_, err = stream.Write(errorMessage)
		if err != nil {
			log.Printf("❌ Lỗi gửi thông báo lỗi (blockRequestHandler) tới %s: %v", peerID, err)
			_ = stream.Reset()
			return
		}
		log.Printf("ℹ️ Đã gửi thông báo lỗi 'không tìm thấy file' cho '%s' tới %s", fileName, peerID)
	}
}
