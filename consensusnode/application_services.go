package consensusnode

import (
	"context"
	"errors"
	"log"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// --- Logic Dành riêng cho Ứng dụng ---

// SendRequestToMasterNode tìm một peer loại "master" và gửi một yêu cầu.
func (mn *ManagedNode) SendRequestToMasterNode(ctx context.Context, protoID protocol.ID, message []byte) ([]byte, error) {
	mn.peerMutex.RLock()
	var masterPeerID peer.ID
	// Tìm master peer đầu tiên đang kết nối
	for pid, pInfo := range mn.peers {
		if pInfo.Type == "master" && pInfo.Status == PeerConnected {
			masterPeerID = pid
			break
		}
	}
	mn.peerMutex.RUnlock()

	if masterPeerID == "" {
		return nil, errors.New("không tìm thấy master peer nào đang kết nối")
	}

	log.Printf("Đang gửi yêu cầu tới master peer %s qua protocol %s", masterPeerID, protoID)
	return mn.SendRequest(ctx, masterPeerID, protoID, message) // stream_manager.go
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
