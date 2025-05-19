package consensusnode

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
)

// --- Thông tin và Trạng thái Peer ---

// PeerStatus đại diện cho trạng thái kết nối với một peer.
type PeerStatus int

const (
	PeerDisconnected PeerStatus = iota
	PeerConnecting
	PeerConnected
	PeerFailed
)

// String trả về biểu diễn chuỗi của PeerStatus.
func (s PeerStatus) String() string {
	switch s {
	case PeerDisconnected:
		return "Đã ngắt kết nối"
	case PeerConnecting:
		return "Đang kết nối"
	case PeerConnected:
		return "Đã kết nối"
	case PeerFailed:
		return "Thất bại"
	default:
		return "Không xác định"
	}
}

// ManagedPeerInfo chứa thông tin về một peer đã kết nối hoặc đã biết.
type ManagedPeerInfo struct {
	ID                peer.ID
	Addresses         []peer.AddrInfo // Sử dụng AddrInfo để có thể chứa nhiều địa chỉ
	Type              string          // "master", "validator", v.v.
	Status            PeerStatus
	LastSeen          time.Time
	LastError         error
	ReconnectAttempts int
	cancelReconnect   context.CancelFunc // Hàm để hủy bỏ goroutine kết nối lại cho peer này
}

// --- Quản lý Peer ---

// AddKnownPeer thêm một peer vào danh sách quản lý một cách tường minh, thường trước khi thử kết nối.
func (mn *ManagedNode) AddKnownPeer(peerAddrStr string, peerType string) error {
	addr, err := peer.AddrInfoFromString(peerAddrStr)
	if err != nil {
		return fmt.Errorf("địa chỉ peer không hợp lệ %s: %w", peerAddrStr, err)
	}

	mn.peerMutex.Lock()
	defer mn.peerMutex.Unlock()

	if pInfo, exists := mn.peers[addr.ID]; exists {
		// Peer đã tồn tại, cập nhật thông tin nếu cần (ví dụ: loại, địa chỉ)
		pInfo.Type = peerType
		// Logic để hợp nhất địa chỉ hoặc thay thế bằng địa chỉ mới nhất
		pInfo.Addresses = []peer.AddrInfo{*addr} // Đơn giản là thay thế
		log.Printf("Đã cập nhật thông tin cho peer đã biết %s (Loại: %s, Địa chỉ: %s)", addr.ID, peerType, addr.Addrs)
	} else {
		mn.peers[addr.ID] = &ManagedPeerInfo{
			ID:        addr.ID,
			Addresses: []peer.AddrInfo{*addr},
			Type:      peerType,
			Status:    PeerDisconnected, // Trạng thái ban đầu
		}
		log.Printf("Đã thêm peer đã biết %s (Loại: %s, Địa chỉ: %s)", addr.ID, peerType, addr.Addrs)
	}
	return nil
}

// connectToBootstrapPeers kết nối tới các bootstrap peer.
func (mn *ManagedNode) connectToBootstrapPeers() error {
	var connectWg sync.WaitGroup
	bootstrapPeers := mn.config.BootstrapPeers
	if len(bootstrapPeers) == 0 {
		log.Println("Không có bootstrap peer nào được cấu hình.")
		return nil
	}

	log.Printf("Đang kết nối tới %d bootstrap peer(s)...", len(bootstrapPeers))
	for _, peerAddrStr := range bootstrapPeers {
		if peerAddrStr == "" {
			continue
		}
		addr, err := peer.AddrInfoFromString(peerAddrStr)
		if err != nil {
			log.Printf("Địa chỉ bootstrap peer không hợp lệ %s: %v", peerAddrStr, err)
			continue
		}
		connectWg.Add(1)
		go func(addrInfo peer.AddrInfo) {
			defer connectWg.Done()
			if err := mn.ConnectToPeer(addrInfo, "bootstrap"); err != nil { // Mặc định loại là "bootstrap"
				log.Printf("Không thể kết nối tới bootstrap peer %s: %v", addrInfo.ID, err)
			}
		}(*addr)
	}
	connectWg.Wait()
	log.Println("Hoàn tất quá trình kết nối bootstrap peer.")
	return nil
}

// ConnectToPeer thử kết nối tới một peer cụ thể.
func (mn *ManagedNode) ConnectToPeer(peerInfo peer.AddrInfo, peerType string) error {
	if peerInfo.ID == mn.host.ID() {
		log.Printf("Bỏ qua việc kết nối tới chính mình (%s)", peerInfo.ID)
		return nil
	}

	mn.peerMutex.Lock()
	if p, exists := mn.peers[peerInfo.ID]; exists {
		if p.Status == PeerConnecting || p.Status == PeerConnected {
			mn.peerMutex.Unlock()
			// log.Printf("Đã kết nối hoặc đang kết nối tới peer %s", peerInfo.ID)
			return nil
		}
		p.Type = peerType
		p.Addresses = []peer.AddrInfo{peerInfo}
		p.Status = PeerConnecting
		p.ReconnectAttempts = 0
	} else {
		mn.peers[peerInfo.ID] = &ManagedPeerInfo{
			ID:        peerInfo.ID,
			Addresses: []peer.AddrInfo{peerInfo},
			Type:      peerType,
			Status:    PeerConnecting,
		}
	}
	mn.peerMutex.Unlock()

	log.Printf("Đang thử kết nối tới peer %s (Loại: %s, Địa chỉ: %s)", peerInfo.ID, peerType, peerInfo.Addrs)
	mn.host.Peerstore().AddAddrs(peerInfo.ID, peerInfo.Addrs, peerstore.PermanentAddrTTL)

	err := mn.host.Connect(mn.ctx, peerInfo)
	if err != nil {
		// "" cho peerType vì updatePeerStatus sẽ lấy từ map
		mn.updatePeerStatus(peerInfo.ID, PeerFailed, err, "")
		log.Printf("Không thể kết nối tới peer %s: %v", peerInfo.ID, err)
		mn.tryReconnectToPeer(peerInfo.ID, peerType)
		return err
	}
	// Trạng thái sẽ được cập nhật thành PeerConnected bởi connection notifier
	log.Printf("Yêu cầu kết nối tới peer %s đã được gửi.", peerInfo.ID)
	return nil
}

// updatePeerStatus cập nhật trạng thái của một peer.
func (mn *ManagedNode) updatePeerStatus(peerID peer.ID, status PeerStatus, err error, peerTypeIfNew string) {
	mn.peerMutex.Lock()
	defer mn.peerMutex.Unlock()

	pInfo, exists := mn.peers[peerID]
	if !exists {
		if status == PeerConnected {
			log.Printf("Peer mới %s đã kết nối.", peerID)
			var addrs []peer.AddrInfo
			conns := mn.host.Network().ConnsToPeer(peerID) // Sửa lỗi chính tả từ ConnsToPeer
			if len(conns) > 0 {
				remoteMultiaddr := conns[0].RemoteMultiaddr()
				addrInfo, _ := peer.AddrInfoFromP2pAddr(remoteMultiaddr)
				if addrInfo != nil {
					addrs = append(addrs, *addrInfo)
				}
			}
			mn.peers[peerID] = &ManagedPeerInfo{
				ID:        peerID,
				Addresses: addrs,
				Type:      peerTypeIfNew,
				Status:    status,
				LastSeen:  time.Now(),
			}
		} else {
			// log.Printf("Cập nhật trạng thái cho peer không xác định %s bị bỏ qua (trừ khi là Connected)", peerID)
		}
		return
	}

	// Chỉ cập nhật nếu trạng thái thay đổi hoặc có lỗi mới
	if pInfo.Status == status && err == pInfo.LastError {
		if status == PeerConnected { // Cập nhật LastSeen ngay cả khi trạng thái không đổi
			pInfo.LastSeen = time.Now()
		}
		return
	}

	oldStatus := pInfo.Status
	pInfo.Status = status
	pInfo.LastError = err

	if status == PeerConnected {
		pInfo.LastSeen = time.Now()
		pInfo.ReconnectAttempts = 0
		if pInfo.cancelReconnect != nil {
			pInfo.cancelReconnect()
			pInfo.cancelReconnect = nil
		}
		log.Printf("Peer %s: %s -> %s", peerID, oldStatus, status)
	} else if status == PeerFailed {
		// pInfo.ReconnectAttempts++ // Số lần thử lại được tăng trong tryReconnectToPeer
		log.Printf("Peer %s: %s -> %s (Lỗi: %v, Thử lại hiện tại: %d)", peerID, oldStatus, status, err, pInfo.ReconnectAttempts)
	} else if status == PeerConnecting {
		// Reset số lần thử lại khi bắt đầu một chu kỳ kết nối mới một cách chủ động
		if oldStatus == PeerDisconnected || oldStatus == PeerFailed {
			pInfo.ReconnectAttempts = 0
		}
		log.Printf("Peer %s: %s -> %s", peerID, oldStatus, status)
	} else {
		log.Printf("Peer %s: %s -> %s", peerID, oldStatus, status)
	}
}

// tryReconnectToPeer bắt đầu quá trình kết nối lại cho một peer trong một goroutine mới.
func (mn *ManagedNode) tryReconnectToPeer(peerID peer.ID, peerType string) {
	mn.peerMutex.Lock()
	pInfo, exists := mn.peers[peerID]
	if !exists {
		mn.peerMutex.Unlock()
		log.Printf("Peer %s không tìm thấy để kết nối lại.", peerID)
		return
	}
	if pInfo.cancelReconnect != nil {
		mn.peerMutex.Unlock()
		// log.Printf("Đã có một tiến trình kết nối lại cho peer %s.", peerID)
		return
	}
	if pInfo.Status == PeerConnected { // Không kết nối lại nếu đã kết nối
		mn.peerMutex.Unlock()
		return
	}

	reconnectCtx, cancel := context.WithCancel(mn.ctx)
	pInfo.cancelReconnect = cancel
	// Không đặt pInfo.Status = PeerConnecting ở đây, để cho goroutine tự cập nhật
	// để tránh tình trạng trạng thái bị kẹt là Connecting nếu goroutine không bao giờ chạy.
	mn.peerMutex.Unlock()

	mn.reconnectWG.Add(1)
	go func(pID peer.ID, pType string, currentReconnectCtx context.Context) {
		defer mn.reconnectWG.Done()
		defer func() {
			mn.peerMutex.Lock()
			if pi, ok := mn.peers[pID]; ok {
				pi.cancelReconnect = nil
				if pi.Status == PeerConnecting { // Nếu vẫn đang connecting khi goroutine kết thúc, nghĩa là thất bại
					pi.Status = PeerDisconnected
					log.Printf("Peer %s vẫn ở trạng thái Connecting sau khi goroutine kết nối lại kết thúc, đặt thành Disconnected.", pID)
				}
			}
			mn.peerMutex.Unlock()
		}()

		// Cập nhật trạng thái thành Connecting khi goroutine bắt đầu
		mn.updatePeerStatus(pID, PeerConnecting, nil, pType)

		delay := mn.config.InitialReconnectDelay
		maxAttempts := mn.config.MaxReconnectAttempts
		if maxAttempts <= 0 { // Nếu cấu hình là 0 hoặc âm, thử vô hạn (hoặc một số lần rất lớn)
			maxAttempts = 1000 // Hoặc math.MaxInt32, nhưng cẩn thận
			log.Printf("Peer %s: MaxReconnectAttempts được đặt thành %d (thử gần như vô hạn).", pID, maxAttempts)
		}

		currentAttempt := 0
		mn.peerMutex.RLock()
		if pi, ok := mn.peers[pID]; ok {
			currentAttempt = pi.ReconnectAttempts
		}
		mn.peerMutex.RUnlock()

		for attempt := currentAttempt; attempt < maxAttempts; attempt++ {
			select {
			case <-currentReconnectCtx.Done():
				log.Printf("Tiến trình kết nối lại cho peer %s đã bị hủy.", pID)
				return
			default:
			}

			mn.peerMutex.Lock()
			if pi, ok := mn.peers[pID]; ok {
				pi.ReconnectAttempts = attempt + 1 // Cập nhật số lần thử thực tế
			}
			mn.peerMutex.Unlock()

			log.Printf("Đang thử kết nối lại tới peer %s (Loại: %s, Lần thử: %d/%d, Độ trễ: %s)", pID, pType, attempt+1, maxAttempts, delay)

			mn.peerMutex.RLock()
			var targetAddrInfo *peer.AddrInfo
			if pi, ok := mn.peers[pID]; ok && len(pi.Addresses) > 0 {
				addrCopy := pi.Addresses[0]
				targetAddrInfo = &addrCopy
			}
			mn.peerMutex.RUnlock()

			if targetAddrInfo == nil {
				log.Printf("Không có thông tin địa chỉ để kết nối lại tới peer %s.", pID)
				mn.updatePeerStatus(pID, PeerFailed, errors.New("không có địa chỉ để kết nối lại"), pType)
				return
			}

			mn.host.Peerstore().AddAddrs(targetAddrInfo.ID, targetAddrInfo.Addrs, peerstore.PermanentAddrTTL)

			connectCtx, connectCancel := context.WithTimeout(currentReconnectCtx, mn.config.PingTimeout*3) // Tăng timeout cho connect
			err := mn.host.Connect(connectCtx, *targetAddrInfo)
			connectCancel()

			if err == nil {
				log.Printf("Kết nối lại thành công tới peer %s.", pID)
				// Connection notifier sẽ cập nhật trạng thái và hủy cancelReconnect
				return
			}

			log.Printf("Kết nối lại tới peer %s thất bại: %v", pID, err)
			mn.updatePeerStatus(pID, PeerFailed, err, pType)

			select {
			case <-currentReconnectCtx.Done():
				log.Printf("Tiến trình kết nối lại cho peer %s đã bị hủy trong khi chờ.", pID)
				return
			case <-time.After(delay):
				jitter := time.Duration(rand.Int63n(int64(delay) / 5))                                             // Jitter lên đến 20%
				delay = time.Duration(math.Min(float64(mn.config.MaxReconnectDelay), float64(delay)*1.8)) + jitter // Tăng backoff mạnh hơn một chút
			}
		}
		log.Printf("Từ bỏ kết nối lại tới peer %s sau %d lần thử.", pID, maxAttempts)
		mn.updatePeerStatus(pID, PeerDisconnected, fmt.Errorf("từ bỏ sau %d lần thử", maxAttempts), pType)
	}(peerID, peerType, reconnectCtx)
}

// peerHealthMonitor định kỳ kiểm tra kết nối peer.
func (mn *ManagedNode) peerHealthMonitor() {
	defer mn.wg.Done()
	if mn.config.PingInterval <= 0 {
		log.Println("Theo dõi sức khỏe peer bị tắt do PingInterval <= 0.")
		return
	}
	ticker := time.NewTicker(mn.config.PingInterval)
	defer ticker.Stop()

	log.Println("Theo dõi sức khỏe peer đã bắt đầu.")
	for {
		select {
		case <-mn.ctx.Done():
			log.Println("Theo dõi sức khỏe peer đang dừng.")
			return
		case <-ticker.C:
			mn.checkAllPeerConnections()
		}
	}
}

// checkAllPeerConnections kiểm tra tất cả các kết nối peer.
func (mn *ManagedNode) checkAllPeerConnections() {
	mn.peerMutex.RLock()
	var peersToCheck []struct {
		id         peer.ID
		ty         string
		stat       PeerStatus
		cancelFunc context.CancelFunc
	}
	for pid, pinfo := range mn.peers {
		peersToCheck = append(peersToCheck, struct {
			id         peer.ID
			ty         string
			stat       PeerStatus
			cancelFunc context.CancelFunc
		}{pid, pinfo.Type, pinfo.Status, pinfo.cancelReconnect})
	}
	mn.peerMutex.RUnlock()

	// log.Printf("Đang kiểm tra sức khỏe của %d peer...", len(peersToCheck))
	connectedCount := 0
	for _, p := range peersToCheck {
		if mn.host.Network().Connectedness(p.id) == network.Connected {
			if p.stat != PeerConnected {
				mn.updatePeerStatus(p.id, PeerConnected, nil, p.ty)
			}
			connectedCount++
		} else {
			// Chỉ thử kết nối lại nếu không đang trong quá trình kết nối lại (p.cancelFunc == nil)
			// và không phải vừa mới thất bại (tránh gọi tryReconnectToPeer liên tục)
			if p.stat != PeerConnecting && p.cancelFunc == nil {
				log.Printf("Peer %s được phát hiện đã ngắt kết nối (%s). Thử kết nối lại.", p.id, p.stat)
				mn.updatePeerStatus(p.id, PeerDisconnected, errors.New("ngắt kết nối khi kiểm tra sức khỏe"), p.ty)
				mn.tryReconnectToPeer(p.id, p.ty)
			}
		}
	}
	// log.Printf("Kiểm tra sức khỏe peer hoàn tất. Số peer đang kết nối: %d/%d", connectedCount, len(peersToCheck))
}

// setupConnectionNotifier thiết lập notifier cho các sự kiện kết nối.
func (mn *ManagedNode) setupConnectionNotifier() {
	mn.host.Network().Notify(&network.NotifyBundle{
		ConnectedF: func(net network.Network, conn network.Conn) {
			peerID := conn.RemotePeer()
			log.Printf("✅ Đã kết nối tới peer: %s (Địa chỉ: %s)", peerID, conn.RemoteMultiaddr())

			mn.peerMutex.RLock() // Dùng RLock vì chỉ đọc pInfo.Type
			pInfo, exists := mn.peers[peerID]
			var peerType string
			if exists {
				peerType = pInfo.Type
			} else {
				peerType = "unknown_inbound"
			}
			mn.peerMutex.RUnlock()

			mn.updatePeerStatus(peerID, PeerConnected, nil, peerType)
			mn.host.Peerstore().AddAddr(peerID, conn.RemoteMultiaddr(), peerstore.ConnectedAddrTTL)
		},
		DisconnectedF: func(net network.Network, conn network.Conn) {
			peerID := conn.RemotePeer()
			log.Printf("❌ Đã ngắt kết nối từ peer: %s", peerID)

			mn.peerMutex.RLock()
			pInfo, exists := mn.peers[peerID]
			var peerType string
			if exists {
				peerType = pInfo.Type
			}
			mn.peerMutex.RUnlock()

			mn.updatePeerStatus(peerID, PeerDisconnected, errors.New("đã ngắt kết nối"), peerType)

			// Quyết định xem có cần kết nối lại không
			if exists && shouldReconnect(pInfo.Type, mn.config) {
				log.Printf("Lên lịch kết nối lại cho peer quan trọng %s (Loại: %s)", peerID, pInfo.Type)
				mn.tryReconnectToPeer(peerID, pInfo.Type)
			}
		},
	})
}

// shouldReconnect kiểm tra xem có nên kết nối lại với một loại peer cụ thể không.
func shouldReconnect(peerType string, config NodeConfig) bool {
	// Ví dụ: luôn kết nối lại với master, bootstrap, validator
	// Hoặc dựa trên một danh sách cấu hình các loại peer cần duy trì kết nối
	switch peerType {
	case "master", "bootstrap", "validator":
		return true
	default:
		return false // Không tự động kết nối lại với các loại peer khác
	}
}

// GetPeersInfo trả về ảnh chụp nhanh thông tin peer hiện tại.
func (mn *ManagedNode) GetPeersInfo() map[peer.ID]ManagedPeerInfo {
	mn.peerMutex.RLock()
	defer mn.peerMutex.RUnlock()

	peersCopy := make(map[peer.ID]ManagedPeerInfo, len(mn.peers))
	for id, infoPtr := range mn.peers {
		infoCopy := *infoPtr
		if len(infoPtr.Addresses) > 0 {
			infoCopy.Addresses = make([]peer.AddrInfo, len(infoPtr.Addresses))
			copy(infoCopy.Addresses, infoPtr.Addresses)
		}
		peersCopy[id] = infoCopy
	}
	return peersCopy
}

// cancelAllReconnects hủy tất cả các goroutine kết nối lại đang hoạt động.
func (mn *ManagedNode) cancelAllReconnects() {
	mn.peerMutex.Lock()
	defer mn.peerMutex.Unlock()
	log.Println("Đang hủy tất cả các tiến trình kết nối lại đang chờ...")
	for _, pInfo := range mn.peers {
		if pInfo.cancelReconnect != nil {
			pInfo.cancelReconnect()
			// pInfo.cancelReconnect = nil // Sẽ được đặt thành nil bởi defer trong goroutine
		}
	}
}
