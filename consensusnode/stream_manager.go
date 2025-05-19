package consensusnode

import (
	"context"
	"fmt"
	"io"
	"log"

	// "encoding/json" // Bỏ comment nếu bạn muốn xử lý JSON

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// --- Protocol IDs ---
const TransactionsRequestProtocol protocol.ID = "/meta-node/transactions-request/1.0.0"
const TransactionStreamProtocol protocol.ID = "/meta-node/transaction-stream/1.0.0"

// transactionsRequestHandler xử lý các yêu cầu đến cho TransactionsRequestProtocol.
// Hàm này sẽ đọc yêu cầu, xử lý (ví dụ), và gửi lại phản hồi.
func (mn *ManagedNode) transactionsRequestHandler(stream network.Stream) {
	remotePeerID := stream.Conn().RemotePeer()
	log.Printf("Đã nhận TransactionsRequestProtocol từ peer: %s", remotePeerID)

	defer func() {
		if errClose := stream.Close(); errClose != nil {
			log.Printf("Lỗi khi đóng stream (transactionsRequestHandler) từ %s: %v", remotePeerID, errClose)
		} else {
			log.Printf("Đã đóng stream (transactionsRequestHandler) từ %s", remotePeerID)
		}
	}()

	// Đọc dữ liệu yêu cầu từ stream
	// Giới hạn kích thước đọc để tránh tấn công DoS hoặc lỗi do message quá lớn
	limitedReader := io.LimitReader(stream, int64(mn.config.MaxMessageSize)) // Sử dụng MaxMessageSize từ config
	requestBytes, err := io.ReadAll(limitedReader)
	if err != nil {
		log.Printf("Lỗi đọc dữ liệu từ stream (transactionsRequestHandler) của peer %s: %v", remotePeerID, err)
		_ = stream.Reset() // Reset stream nếu có lỗi đọc
		return
	}

	if len(requestBytes) == 0 {
		log.Printf("Yêu cầu từ peer %s (transactionsRequestHandler) không có dữ liệu.", remotePeerID)
		// Quyết định cách xử lý: gửi lỗi lại hoặc chỉ đóng stream
		responsePayload := []byte("{\"error\": \"empty request payload\"}")
		_, writeErr := stream.Write(responsePayload)
		if writeErr != nil {
			log.Printf("Lỗi ghi phản hồi lỗi (empty request) vào stream cho peer %s: %v", remotePeerID, writeErr)
		}
		_ = stream.Reset() // Reset sau khi gửi lỗi hoặc nếu không gửi gì
		return
	}

	log.Printf("Đã nhận yêu cầu (%d bytes) từ %s (TransactionsRequestProtocol): %s", len(requestBytes), remotePeerID, string(requestBytes))

	// --- XỬ LÝ YÊU CẦU THỰC TẾ TẠI ĐÂY ---
	// Dựa vào `requestBytes`, bạn sẽ thực hiện logic nghiệp vụ.
	// Ví dụ: nếu requestBytes là một JSON {"action": "get_pending_count"},
	// bạn sẽ lấy số lượng giao dịch đang chờ và trả về.

	var responsePayload []byte
	// Ví dụ xử lý đơn giản:
	// if string(requestBytes) == "GET_TRANSACTION_COUNT" {
	// 	// count := mn.ledger.GetPendingTransactionCount() // Hàm giả định
	// 	// responsePayload = []byte(fmt.Sprintf("{\"count\": %d}", count))
	// 	responsePayload = []byte(fmt.Sprintf("{\"count\": %d}", 42)) // Giả sử có 42 giao dịch
	// } else if strings.HasPrefix(string(requestBytes), "GET_TRANSACTION_DETAILS:") {
	// 	// txId := strings.TrimPrefix(string(requestBytes), "GET_TRANSACTION_DETAILS:")
	// 	// details := mn.ledger.GetTransactionDetails(txId) // Hàm giả định
	// 	// responsePayload, _ = json.Marshal(details)
	// 	responsePayload = []byte(fmt.Sprintf("{\"details_for\": \"%s\", \"data\": \"some details\"}", "txId_placeholder"))
	// } else {
	// 	responsePayload = []byte("{\"error\": \"unknown request command\"}")
	// }

	// Để minh họa, Master sẽ gửi lại một thông báo xác nhận và echo lại một phần yêu cầu.
	responsePayload = []byte(fmt.Sprintf("Master node đã nhận và xử lý yêu cầu của bạn cho TransactionsRequestProtocol. Dữ liệu nhận được: %s", string(requestBytes)))
	log.Printf("Chuẩn bị gửi phản hồi: %s", string(responsePayload))

	// --- GỬI PHẢN HỒI TRỞ LẠI STREAM ---
	if responsePayload != nil {
		bytesWritten, writeErr := stream.Write(responsePayload)
		if writeErr != nil {
			log.Printf("Lỗi ghi phản hồi vào stream (transactionsRequestHandler) cho peer %s: %v", remotePeerID, writeErr)
			_ = stream.Reset() // Reset stream nếu có lỗi ghi
			return
		}
		log.Printf("Đã gửi phản hồi (%d bytes) cho peer %s (TransactionsRequestProtocol)", bytesWritten, remotePeerID)
	} else {
		log.Printf("Không có phản hồi nào được chuẩn bị để gửi cho peer %s (TransactionsRequestProtocol)", remotePeerID)
		// Nếu không có phản hồi, client có thể bị timeout. Cân nhắc gửi một phản hồi trống hoặc lỗi.
		// Hoặc nếu thiết kế cho phép không phản hồi, đảm bảo client xử lý được.
	}

	// stream.Close() đã được defer ở trên.
	// Sau khi server ghi phản hồi, client có thể đọc.
	// Client (trong SendRequest) sẽ gọi stream.CloseWrite() sau khi gửi yêu cầu,
	// điều này báo cho server biết client đã gửi xong.
	// Server, sau khi ghi phản hồi, stream sẽ được đóng bởi defer.
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
		log.Printf("Stream handler cho %s đã được thiết lập trên host.", protoID)
	} else {
		log.Printf("Host chưa được khởi tạo, stream handler cho %s sẽ được thiết lập khi Start()", protoID)
	}
}

// --- Gửi Dữ liệu qua Stream ---
// SendRequest mở một stream mới, gửi message, và đọc phản hồi.
func (mn *ManagedNode) SendRequest(ctx context.Context, targetPeerID peer.ID, protoID protocol.ID, message []byte) ([]byte, error) {
	if targetPeerID == mn.host.ID() {
		return nil, fmt.Errorf("không thể gửi request tới chính mình")
	}
	// Kiểm tra kết nối trước khi mở stream
	if mn.host.Network().Connectedness(targetPeerID) != network.Connected {
		// Cố gắng kết nối nếu chưa kết nối và có thông tin địa chỉ
		// Điều này hữu ích nếu targetPeerID là master node đã biết nhưng tạm thời mất kết nối.
		// Tuy nhiên, SendRequestToMasterNode đã có logic kết nối rồi.
		log.Printf("Cảnh báo: Không kết nối tới peer %s để gửi request qua protocol %s. Thử kết nối nếu là master node đã biết...", targetPeerID, protoID)
		// Nếu đây là master node từ config, SendRequestToMasterNode đã cố gắng kết nối.
		// Nếu là peer khác, có thể cần thêm logic kết nối ở đây hoặc đảm bảo kết nối trước khi gọi.
		// Hiện tại, sẽ trả lỗi nếu không kết nối.
		return nil, fmt.Errorf("không kết nối tới peer %s để gửi request qua protocol %s", targetPeerID, protoID)
	}

	log.Printf("Đang mở stream tới %s cho protocol %s", targetPeerID, protoID)
	stream, err := mn.host.NewStream(ctx, targetPeerID, protoID)
	if err != nil {
		return nil, fmt.Errorf("không thể mở stream tới %s cho protocol %s: %w", targetPeerID, protoID, err)
	}
	// Đảm bảo stream được đóng hoàn toàn sau khi hàm kết thúc, dù thành công hay thất bại.
	defer func() {
		if errClose := stream.Close(); errClose != nil {
			// Log lỗi nếu stream.Reset() chưa được gọi và Close() thất bại
			// Nếu stream.Reset() đã được gọi, Close() có thể trả về lỗi nhưng đó là bình thường.
			// Kiểm tra xem stream có bị reset không có thể phức tạp.
			// Đơn giản là log lỗi nếu có.
			log.Printf("Lỗi khi đóng stream (SendRequest defer) tới %s: %v", targetPeerID, errClose)
		}
	}()

	log.Printf("Đang gửi yêu cầu tới %s qua %s (%d bytes)", targetPeerID, protoID, len(message))
	_, err = stream.Write(message)
	if err != nil {
		_ = stream.Reset() // Nếu ghi lỗi, reset stream để giải phóng tài nguyên và báo lỗi cho phía bên kia.
		return nil, fmt.Errorf("không thể ghi vào stream tới %s: %w", targetPeerID, err)
	}

	// Rất quan trọng: Đóng phía ghi của stream để báo cho server biết client đã gửi xong.
	// Server sẽ không thể đọc được EOF nếu phía ghi không được đóng.
	err = stream.CloseWrite()
	if err != nil {
		_ = stream.Reset()
		return nil, fmt.Errorf("không thể đóng phía ghi của stream tới %s: %w", targetPeerID, err)
	}

	// Đọc phản hồi từ server
	log.Printf("Đang chờ phản hồi từ %s cho protocol %s...", targetPeerID, protoID)

	// Sử dụng bufio.NewReader để có thể đọc hiệu quả hơn, nhưng io.ReadAll cũng hoạt động.
	// Giới hạn kích thước đọc phản hồi để tránh client bị treo hoặc tiêu thụ quá nhiều bộ nhớ nếu server gửi dữ liệu lớn không mong muốn.
	// Cần một config cho kích thước phản hồi tối đa, tương tự MaxMessageSize.
	// Tạm thời dùng MaxMessageSize.
	limitedResponseReader := io.LimitReader(stream, int64(mn.config.MaxMessageSize))
	responseBuffer, err := io.ReadAll(limitedResponseReader)

	// Xử lý lỗi đọc phản hồi
	// io.EOF không phải là lỗi nếu đã đọc được một số dữ liệu, hoặc nếu server đóng stream sau khi gửi.
	// Tuy nhiên, nếu ReadAll trả về io.EOF và không có byte nào được đọc, đó có thể là server đóng stream mà không gửi gì.
	if err != nil && err != io.EOF { // Nếu lỗi khác EOF
		// Nếu lỗi xảy ra trong khi đọc, stream có thể đã bị reset bởi server hoặc có vấn đề mạng.
		// stream.Reset() ở đây có thể không cần thiết nếu lỗi là do server đã reset.
		// _ = stream.Reset() // Cân nhắc việc reset ở đây
		return responseBuffer, fmt.Errorf("lỗi khi đọc phản hồi từ stream của %s: %w (đã đọc %d bytes)", targetPeerID, err, len(responseBuffer))
	}
	// Nếu err là io.EOF, có nghĩa là server đã đóng stream sau khi gửi xong dữ liệu của nó. Đây là trường hợp bình thường.
	// Nếu responseBuffer rỗng và err là io.EOF, server có thể đã không gửi gì.

	log.Printf("Đã nhận phản hồi từ %s (%d bytes) cho protocol %s", targetPeerID, len(responseBuffer), protoID)
	return responseBuffer, nil // Trả về nil error nếu đọc thành công (kể cả khi err là io.EOF sau khi đã đọc)
}

// --- Helper Functions ---
// func min(a, b int) int { // Hàm này không được sử dụng, có thể xóa
// 	if a < b {
// 		return a
// 	}
// 	return b
// }
