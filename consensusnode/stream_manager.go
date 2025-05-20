// In consensusnode/stream_manager.go

package consensusnode

import (
	"context"
	"encoding/hex" // Thêm import này
	"encoding/json"
	"fmt"
	"io"
	"log"

	// "time" // Bỏ comment nếu cần

	"github.com/blockchain/consensus/dag" // Đảm bảo đường dẫn này chính xác
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// --- Protocol IDs ---
const TransactionsRequestProtocol protocol.ID = "/meta-node/transactions-request/1.0.0"
const TransactionStreamProtocol protocol.ID = "/meta-node/transaction-stream/1.0.0" // Giả sử bạn có thể cần protocol này
const SyncRequestProtocol protocol.ID = "/meta-node/sync-request/1.0.0"             // Protocol cho sync requests

// transactionsRequestHandler xử lý các yêu cầu đến cho TransactionsRequestProtocol.
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

	limitedReader := io.LimitReader(stream, int64(mn.config.MaxMessageSize))
	requestBytes, err := io.ReadAll(limitedReader)
	if err != nil {
		log.Printf("Lỗi đọc dữ liệu từ stream (transactionsRequestHandler) của peer %s: %v", remotePeerID, err)
		_ = stream.Reset()
		return
	}

	if len(requestBytes) == 0 {
		log.Printf("Yêu cầu từ peer %s (transactionsRequestHandler) không có dữ liệu.", remotePeerID)
		responsePayload := []byte("{\"error\": \"empty request payload\"}")
		_, writeErr := stream.Write(responsePayload)
		if writeErr != nil {
			log.Printf("Lỗi ghi phản hồi lỗi (empty request) vào stream cho peer %s: %v", remotePeerID, writeErr)
		}
		_ = stream.Reset()
		return
	}

	log.Printf("Đã nhận yêu cầu (%d bytes) từ %s (TransactionsRequestProtocol): %s", len(requestBytes), remotePeerID, string(requestBytes))

	// --- XỬ LÝ YÊU CẦU THỰC TẾ TẠI ĐÂY ---
	// Dựa vào `requestBytes`, bạn sẽ thực hiện logic nghiệp vụ.
	// Ví dụ: nếu requestBytes là một JSON {"action": "get_pending_count"},
	// bạn sẽ lấy số lượng giao dịch đang chờ và trả về.

	// Để minh họa, Master sẽ gửi lại một thông báo xác nhận và echo lại một phần yêu cầu.
	responsePayload := []byte(fmt.Sprintf("Master node đã nhận và xử lý yêu cầu của bạn cho TransactionsRequestProtocol. Dữ liệu nhận được: %s", string(requestBytes)))
	log.Printf("Chuẩn bị gửi phản hồi: %s", string(responsePayload))

	// --- GỬI PHẢN HỒI TRỞ LẠI STREAM ---
	if responsePayload != nil {
		bytesWritten, writeErr := stream.Write(responsePayload)
		if writeErr != nil {
			log.Printf("Lỗi ghi phản hồi vào stream (transactionsRequestHandler) cho peer %s: %v", remotePeerID, writeErr)
			_ = stream.Reset()
			return
		}
		log.Printf("Đã gửi phản hồi (%d bytes) cho peer %s (TransactionsRequestProtocol)", bytesWritten, remotePeerID)
	} else {
		log.Printf("Không có phản hồi nào được chuẩn bị để gửi cho peer %s (TransactionsRequestProtocol)", remotePeerID)
	}
}

// SyncRequestPayload định nghĩa cấu trúc cho yêu cầu đồng bộ dựa trên index.
type SyncRequestPayload struct {
	Action          string            `json:"action"`
	RequesterNodeID string            `json:"requester_node_id"` // PubKeyHex của node yêu cầu
	KnownMaxIndices map[string]uint64 `json:"known_max_indices"` // Key: Creator PubKeyHex, Value: Max event index đã biết
}

// SyncResponsePayload định nghĩa cấu trúc cho phản hồi đồng bộ.
type SyncResponsePayload struct {
	Events               [][]byte          `json:"events"`                 // Slice các dag.Event đã được marshal
	PartnerLatestIndices map[string]uint64 `json:"partner_latest_indices"` // (Tùy chọn) Index mới nhất của partner
}

// syncRequestHandler xử lý các yêu cầu đồng bộ đến (phiên bản dựa trên index).
func (mn *ManagedNode) syncRequestHandler(stream network.Stream) {
	remotePeerID := stream.Conn().RemotePeer()
	log.Printf("SYNC_HANDLER: Đã nhận SyncRequestProtocol từ peer: %s", remotePeerID)

	defer func() {
		if errClose := stream.Close(); errClose != nil {
			log.Printf("SYNC_HANDLER: Lỗi khi đóng stream từ %s: %v", remotePeerID, errClose)
		} else {
			log.Printf("SYNC_HANDLER: Đã đóng stream từ %s", remotePeerID)
		}
	}()

	limitedReader := io.LimitReader(stream, int64(mn.config.MaxMessageSize))
	requestBytes, err := io.ReadAll(limitedReader)
	if err != nil {
		log.Printf("SYNC_HANDLER: Lỗi đọc dữ liệu yêu cầu đồng bộ từ peer %s: %v", remotePeerID, err)
		_ = stream.Reset()
		return
	}

	if len(requestBytes) == 0 {
		log.Printf("SYNC_HANDLER: Yêu cầu đồng bộ từ peer %s không có dữ liệu.", remotePeerID)
		_ = stream.Reset()
		return
	}

	log.Printf("SYNC_HANDLER: Đã nhận yêu cầu đồng bộ (%d bytes) từ %s: %s", len(requestBytes), remotePeerID, string(requestBytes))

	var reqPayload SyncRequestPayload
	if err := json.Unmarshal(requestBytes, &reqPayload); err != nil {
		log.Printf("SYNC_HANDLER: Lỗi unmarshal yêu cầu đồng bộ từ %s: %v. Payload: %s", remotePeerID, err, string(requestBytes))
		// Gửi lại lỗi cho client nếu cần
		errorResponse := []byte("{\"error\": \"invalid sync request payload format\"}")
		_, _ = stream.Write(errorResponse)
		_ = stream.Reset()
		return
	}

	log.Printf("SYNC_HANDLER: Đã phân tích yêu cầu đồng bộ từ Node ID %s. Action: %s. KnownMaxIndices count: %d",
		reqPayload.RequesterNodeID, reqPayload.Action, len(reqPayload.KnownMaxIndices))

	var eventsToSend []*dag.Event
	var marshaledEventsData [][]byte

	// Lấy tất cả các event từ DagStore của node hiện tại (Node B - provider).
	allEventsInStore := mn.dagStore.GetAllEventsSnapshot() // Cần một hàm như thế này trong DagStore

	for _, event := range allEventsInStore {
		creatorHex := hex.EncodeToString(event.EventData.Creator)
		knownMaxIndex, requesterKnowsThisCreator := reqPayload.KnownMaxIndices[creatorHex]

		if !requesterKnowsThisCreator {
			// Node A không biết về creator này, gửi tất cả event của creator này (hoặc từ event đầu tiên)
			// Để đơn giản, gửi event hiện tại nếu Node A không biết creator.
			// Một logic tốt hơn là gửi tất cả các event của creator này mà Node B có.
			// Hoặc, gửi các event từ index 1 của creator đó.
			eventsToSend = append(eventsToSend, event)
		} else {
			// Node A biết creator này, chỉ gửi event nếu index của nó lớn hơn index Node A đã biết.
			if event.EventData.Index > knownMaxIndex {
				eventsToSend = append(eventsToSend, event)
			}
		}
	}

	log.Printf("SYNC_HANDLER: Chuẩn bị gửi %d event cho %s.", len(eventsToSend), reqPayload.RequesterNodeID)

	for _, event := range eventsToSend {
		eventBytes, err := event.Marshal() // Sử dụng Marshal từ dag/event.go
		if err != nil {
			log.Printf("SYNC_HANDLER: Lỗi marshal event %s cho phản hồi đồng bộ: %v", event.GetEventId().String(), err)
			continue // Bỏ qua event này nếu không marshal được
		}
		marshaledEventsData = append(marshaledEventsData, eventBytes)
	}

	// (Tùy chọn) Chuẩn bị PartnerLatestIndices
	partnerIndices := make(map[string]uint64)
	// Lấy latest indices từ DagStore của Node B
	// Ví dụ:
	// latestEventsMap := mn.dagStore.GetLatestEventsMapSnapshot() // Cần hàm GetLatestEventsMapSnapshot
	// for creatorPubKeyHex, latestEventID := range latestEventsMap {
	// 	   latestEvent, exists := mn.dagStore.GetEvent(latestEventID)
	// 	   if exists {
	// 		   partnerIndices[creatorPubKeyHex] = latestEvent.EventData.Index
	// 	   }
	// }

	respPayload := SyncResponsePayload{
		Events:               marshaledEventsData,
		PartnerLatestIndices: partnerIndices, // Sẽ rỗng nếu không triển khai phần trên
	}

	responsePayloadBytes, err := json.Marshal(respPayload)
	if err != nil {
		log.Printf("SYNC_HANDLER: Lỗi marshal phản hồi đồng bộ cho %s: %v", remotePeerID, err)
		errorResponse := []byte("{\"error\": \"internal server error during sync response generation\"}")
		_, _ = stream.Write(errorResponse)
		_ = stream.Reset()
		return
	}

	if len(responsePayloadBytes) > 0 { // Kiểm tra lại điều kiện này, nên là len(marshaledEventsData) > 0 hoặc luôn gửi phản hồi
		bytesWritten, writeErr := stream.Write(responsePayloadBytes)
		if writeErr != nil {
			log.Printf("SYNC_HANDLER: Lỗi ghi phản hồi đồng bộ vào stream cho peer %s: %v", remotePeerID, writeErr)
			_ = stream.Reset()
			return
		}
		log.Printf("SYNC_HANDLER: Đã gửi phản hồi đồng bộ (%d bytes, %d events) cho peer %s", bytesWritten, len(eventsToSend), remotePeerID)
	} else { // Trường hợp không có event nào để gửi (len(eventsToSend) == 0)
		log.Printf("SYNC_HANDLER: Không có event mới nào để gửi cho peer %s. Gửi phản hồi rỗng.", remotePeerID)
		// Vẫn gửi một phản hồi rỗng hợp lệ
		emptyRespBytes, _ := json.Marshal(SyncResponsePayload{Events: [][]byte{}, PartnerLatestIndices: partnerIndices})
		_, writeErr := stream.Write(emptyRespBytes)
		if writeErr != nil {
			log.Printf("SYNC_HANDLER: Lỗi ghi phản hồi đồng bộ rỗng vào stream cho peer %s: %v", remotePeerID, writeErr)
			_ = stream.Reset() // Reset stream nếu có lỗi ghi
		} else {
			log.Printf("SYNC_HANDLER: Đã gửi phản hồi đồng bộ rỗng (không có event mới) cho peer %s", remotePeerID)
		}
	}
}

// --- Quản lý Stream Handler ---
// RegisterStreamHandler đăng ký một handler cho một protocol ID cụ thể.
func (mn *ManagedNode) RegisterStreamHandler(protoID protocol.ID, handler network.StreamHandler) {
	if handler == nil {
		log.Printf("Cảnh báo: Đã cố gắng đăng ký stream handler nil cho protocol %s", protoID)
		return
	}
	mn.streamHandlers[protoID] = handler // Lưu trữ handler nội bộ
	// Nếu host đã được khởi tạo, thiết lập handler ngay lập tức
	if mn.host != nil {
		mn.host.SetStreamHandler(protoID, handler)
		log.Printf("Stream handler cho %s đã được thiết lập trên host.", protoID)
	} else {
		// Nếu host chưa được khởi tạo, handler sẽ được thiết lập khi node Start()
		log.Printf("Host chưa được khởi tạo, stream handler cho %s sẽ được thiết lập khi Start()", protoID)
	}
}

// --- Gửi Dữ liệu qua Stream ---
// SendRequest mở một stream mới, gửi message, và đọc phản hồi.
func (mn *ManagedNode) SendRequest(ctx context.Context, targetPeerID peer.ID, protoID protocol.ID, message []byte) ([]byte, error) {
	// Kiểm tra host đã được khởi tạo chưa
	if mn.host == nil {
		return nil, fmt.Errorf("host chưa được khởi tạo, không thể gửi request")
	}
	// Không gửi request cho chính mình
	if targetPeerID == mn.host.ID() {
		return nil, fmt.Errorf("không thể gửi request tới chính mình")
	}

	// Kiểm tra kết nối hiện tại, nhưng không nhất thiết phải lỗi ngay nếu chưa kết nối,
	// vì NewStream có thể tự động thử kết nối.
	if mn.host.Network().Connectedness(targetPeerID) != network.Connected {
		log.Printf("Cảnh báo: Chưa kết nối tới peer %s để gửi request qua protocol %s. NewStream sẽ thử kết nối.", targetPeerID, protoID)
	}

	log.Printf("Đang mở stream tới %s cho protocol %s", targetPeerID, protoID)
	stream, err := mn.host.NewStream(ctx, targetPeerID, protoID)
	if err != nil {
		return nil, fmt.Errorf("không thể mở stream tới %s cho protocol %s: %w", targetPeerID, protoID, err)
	}
	// Đảm bảo stream được đóng hoàn toàn sau khi hàm kết thúc
	defer func() {
		if errClose := stream.Close(); errClose != nil {
			log.Printf("Lỗi khi đóng stream (SendRequest defer) tới %s: %v", targetPeerID, errClose)
		}
	}()

	log.Printf("Đang gửi yêu cầu tới %s qua %s (%d bytes)", targetPeerID, protoID, len(message))
	_, err = stream.Write(message)
	if err != nil {
		_ = stream.Reset() // Reset stream nếu ghi lỗi
		return nil, fmt.Errorf("không thể ghi vào stream tới %s: %w", targetPeerID, err)
	}

	// Đóng phía ghi của stream để báo cho server biết client đã gửi xong.
	err = stream.CloseWrite()
	if err != nil {
		_ = stream.Reset() // Reset stream nếu không đóng được phía ghi
		return nil, fmt.Errorf("không thể đóng phía ghi của stream tới %s: %w", targetPeerID, err)
	}

	log.Printf("Đang chờ phản hồi từ %s cho protocol %s...", targetPeerID, protoID)

	// Đọc phản hồi từ server, giới hạn kích thước
	limitedResponseReader := io.LimitReader(stream, int64(mn.config.MaxMessageSize))
	responseBuffer, err := io.ReadAll(limitedResponseReader)

	// Xử lý lỗi đọc phản hồi
	if err != nil && err != io.EOF { // Nếu lỗi khác EOF
		return responseBuffer, fmt.Errorf("lỗi khi đọc phản hồi từ stream của %s: %w (đã đọc %d bytes)", targetPeerID, err, len(responseBuffer))
	}
	// io.EOF là bình thường nếu server đã đóng stream sau khi gửi xong.

	log.Printf("Đã nhận phản hồi từ %s (%d bytes) cho protocol %s", targetPeerID, len(responseBuffer), protoID)
	return responseBuffer, nil
}

/*
// Cần thêm các hàm này vào consensusnode/managed_node.go hoặc một file utils chung nếu chưa có
// Ví dụ về cách ManagedNode có thể truy cập DagStore (giả sử mn.dagStore là một con trỏ *dag.DagStore)

func (mn *ManagedNode) GetDagStore() *dag.DagStore {
    return mn.dagStore
}
*/

/*
// Cần thêm hàm GetAllEventsSnapshot() và GetLatestEventsMapSnapshot() vào DagStore
// Ví dụ trong consensus/dag/dag.go:

func (ds *DagStore) GetAllEventsSnapshot() []*Event {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	events := make([]*Event, 0, len(ds.events))
	for _, event := range ds.events {
		events = append(events, event)
	}
	// Có thể cần sắp xếp các event ở đây nếu thứ tự quan trọng cho logic đồng bộ
	// Ví dụ: sort.Slice(events, func(i, j int) bool {
	//     if events[i].EventData.Frame != events[j].EventData.Frame {
	//         return events[i].EventData.Frame < events[j].EventData.Frame
	//     }
	//     return events[i].EventData.Timestamp < events[j].EventData.Timestamp
	// })
	return events
}

func (ds *DagStore) GetLatestEventsMapSnapshot() map[string]EventID {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	latestCopy := make(map[string]EventID, len(ds.latestEvents))
	for k, v := range ds.latestEvents {
		latestCopy[k] = v
	}
	return latestCopy
}
*/
