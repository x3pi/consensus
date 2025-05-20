package consensusnode

import (
	"context"
	"errors"
	"log"

	"github.com/libp2p/go-libp2p/core/network" // Thêm import này
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore" // Thêm import này
	"github.com/libp2p/go-libp2p/core/protocol"
)

// --- Logic Dành riêng cho Ứng dụng ---

// SendRequestToMasterNode tìm một peer loại "master" và gửi một yêu cầu.
// Nó sẽ sử dụng địa chỉ MasterNodeAddress từ cấu hình nếu có,
// nếu không sẽ tìm một peer "master" đang kết nối.
// Hàm này sẽ chờ nhận phản hồi và in ra phản hồi đó.
func (mn *ManagedNode) SendRequestToMasterNode(ctx context.Context, protoID protocol.ID, message []byte) ([]byte, error) {
	var targetPeerID peer.ID
	var masterAddrInfo *peer.AddrInfo // Khai báo ở đây để có thể sử dụng lại

	// Ưu tiên sử dụng MasterNodeAddress từ cấu hình
	if mn.config.MasterNodeAddress != "" {
		addrInfo, err := peer.AddrInfoFromString(mn.config.MasterNodeAddress)
		if err != nil {
			log.Printf("Lỗi: Không thể phân tích MasterNodeAddress từ cấu hình '%s': %v", mn.config.MasterNodeAddress, err)
			// Không trả về lỗi ngay, thử tìm trong danh sách peer kết nối
		} else {
			masterAddrInfo = addrInfo // Gán cho biến đã khai báo
			targetPeerID = masterAddrInfo.ID
			log.Printf("Sử dụng MasterNodeAddress từ cấu hình: %s", targetPeerID)

			// **ĐÁNH DẤU PEER NÀY LÀ "MASTER" TRONG mn.peers**
			// Gọi AddKnownPeer để thêm hoặc cập nhật thông tin của peer này với Type là "master".
			// AddKnownPeer nằm trong peer_manager.go và là một method của ManagedNode.
			// Điều này đảm bảo rằng selectConsensusPartner sẽ biết type của peer này.
			if addPeerErr := mn.AddKnownPeer(mn.config.MasterNodeAddress, "master"); addPeerErr != nil {
				log.Printf("Cảnh báo: Không thể thêm/cập nhật MasterNodeAddress '%s' với type 'master' vào danh sách known peers: %v", mn.config.MasterNodeAddress, addPeerErr)
				// Tiếp tục thực hiện, nhưng có thể selectConsensusPartner không loại bỏ được peer này nếu nó chưa từng được biết đến.
			} else {
				log.Printf("Đã đảm bảo MasterNodeAddress '%s' (PeerID: %s) được đánh dấu là 'master' trong danh sách known peers.", mn.config.MasterNodeAddress, targetPeerID)
			}

			// Kiểm tra xem có đang kết nối tới master node đã cấu hình không
			// và thử kết nối nếu chưa.
			if mn.host.Network().Connectedness(targetPeerID) != network.Connected {
				log.Printf("Cảnh báo: Không kết nối tới master node đã cấu hình (%s). Thử kết nối...", targetPeerID)
				// Thêm địa chỉ vào peerstore để libp2p biết cách kết nối
				mn.host.Peerstore().AddAddrs(masterAddrInfo.ID, masterAddrInfo.Addrs, peerstore.PermanentAddrTTL)
				if err := mn.host.Connect(ctx, *masterAddrInfo); err != nil {
					log.Printf("Không thể kết nối tới master node đã cấu hình %s: %v", targetPeerID, err)
					// Nếu không kết nối được, thử tìm trong các peer đang kết nối
					targetPeerID = "" // Reset để tìm trong danh sách peer
				} else {
					log.Printf("Đã kết nối thành công tới master node đã cấu hình: %s", targetPeerID)
					// Sau khi kết nối thành công, ConnectedF trong setupConnectionNotifier sẽ được gọi.
					// Vì chúng ta đã gọi AddKnownPeer ở trên với type "master",
					// updatePeerStatus trong ConnectedF nên tôn trọng hoặc sử dụng type này.
					// Để chắc chắn, có thể cập nhật lại type ở đây nếu cần, nhưng AddKnownPeer là ưu tiên.
					// mn.updatePeerStatus(targetPeerID, PeerConnected, nil, "master")
				}
			}
			// Nếu đã kết nối, kiểm tra và đảm bảo type là "master" một lần nữa (có thể thừa nếu AddKnownPeer hoạt động tốt)
			// Đoạn này có thể được xem xét lại tùy thuộc vào cách updatePeerStatus xử lý type.
			// mn.peerMutex.Lock()
			// if pInfo, exists := mn.peers[targetPeerID]; exists && pInfo.Type != "master" {
			// 	log.Printf("SendRequestToMasterNode: Cập nhật lại Type của master peer %s thành 'master' sau khi kiểm tra kết nối.", targetPeerID)
			// 	pInfo.Type = "master"
			// }
			// mn.peerMutex.Unlock()
		}
	}

	// Nếu không có MasterNodeAddress trong cấu hình hoặc không kết nối được,
	// tìm một master peer đang kết nối (đã được đánh dấu type "master" từ trước)
	if targetPeerID == "" {
		log.Printf("MasterNodeAddress không được cấu hình hoặc không kết nối được. Đang tìm master peer trong danh sách kết nối...")
		mn.peerMutex.RLock()
		for pid, pInfo := range mn.peers {
			if pInfo.Type == "master" && pInfo.Status == PeerConnected {
				targetPeerID = pid
				// Cập nhật masterAddrInfo nếu tìm thấy master theo cách này và chưa có từ config
				if masterAddrInfo == nil && len(pInfo.Addresses) > 0 {
					// Lấy địa chỉ đầu tiên làm ví dụ, có thể cần logic chọn địa chỉ tốt hơn
					addrCopy := pInfo.Addresses[0]
					masterAddrInfo = &addrCopy
				}
				log.Printf("Tìm thấy master peer đang kết nối: %s (Loại: %s)", targetPeerID, pInfo.Type)
				break
			}
		}
		mn.peerMutex.RUnlock()
	}

	if targetPeerID == "" {
		return nil, errors.New("không tìm thấy master peer nào (từ cấu hình hoặc đang kết nối) để gửi yêu cầu")
	}
	if masterAddrInfo == nil {
		// Điều này không nên xảy ra nếu targetPeerID được tìm thấy, nhưng để phòng ngừa
		return nil, errors.New("không có thông tin địa chỉ cho master peer đã chọn")
	}

	log.Printf("Đang gửi yêu cầu tới master peer %s qua protocol %s", targetPeerID, protoID)

	// Gọi mn.SendRequest để gửi yêu cầu và chờ nhận phản hồi
	responseData, err := mn.SendRequest(ctx, targetPeerID, protoID, message) // stream_manager.go

	if err != nil {
		log.Printf("Lỗi khi gửi yêu cầu hoặc nhận phản hồi từ master node %s: %v", targetPeerID, err)
		return nil, err
	}

	if responseData != nil {
		log.Printf("Đã nhận phản hồi từ master node %s (%d bytes): %s", targetPeerID, len(responseData), string(responseData))
	} else {
		log.Printf("Đã nhận phản hồi nil từ master node %s (không có lỗi)", targetPeerID)
	}

	return responseData, nil
}

// --- Quản lý Fee Addresses ---

// SetFeeAddresses cập nhật danh sách địa chỉ phí một cách an toàn.
func (mn *ManagedNode) SetFeeAddresses(addresses []string) {
	mn.feeAddressesMux.Lock()
	defer mn.feeAddressesMux.Unlock()

	mn.feeAddresses = make([]string, len(addresses))
	copy(mn.feeAddresses, addresses)
	log.Printf("Đã cập nhật FeeAddresses: %v", mn.feeAddresses)
}

// GetFeeAddresses trả về bản sao của danh sách địa chỉ phí hiện tại.
func (mn *ManagedNode) GetFeeAddresses() []string {
	mn.feeAddressesMux.RLock()
	defer mn.feeAddressesMux.RUnlock()

	addressesCopy := make([]string, len(mn.feeAddresses))
	copy(addressesCopy, mn.feeAddresses)
	return addressesCopy
}

// --- Kênh Giao dịch và Trạng thái Tìm nạp Block ---

// GetTransactionChan trả về transaction channel (chỉ đọc).
// Điều này ngăn chặn việc đóng channel từ bên ngoài.
func (mn *ManagedNode) GetTransactionChan() <-chan []byte {
	return mn.transactionChan
}

// SendToTransactionChan gửi dữ liệu vào transaction channel một cách an toàn.
func (mn *ManagedNode) SendToTransactionChan(data []byte) bool {
	select {
	case mn.transactionChan <- data:
		return true
	case <-mn.ctx.Done(): // Ngăn chặn block nếu node đang dừng
		log.Println("Không thể gửi tới transaction channel: node đang dừng.")
		return false
	default:
		// Channel đầy, có thể log hoặc xử lý khác
		log.Println("Cảnh báo: Transaction channel đầy, bỏ qua message.")
		return false
	}
}

// IsFetchingBlock kiểm tra xem một block có đang được tìm nạp không.
func (mn *ManagedNode) IsFetchingBlock(blockNumber uint64) bool {
	_, ok := mn.fetchingBlocks.Load(blockNumber)
	return ok
}

// SetFetchingBlock đánh dấu một block là đang được tìm nạp hoặc đã hoàn thành.
func (mn *ManagedNode) SetFetchingBlock(blockNumber uint64, status bool) {
	if status {
		mn.fetchingBlocks.Store(blockNumber, true)
	} else {
		mn.fetchingBlocks.Delete(blockNumber)
	}
}
