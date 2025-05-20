package dag

import (
	"encoding/hex"
	"errors"
	"fmt"
	"log" // Giữ lại log chuẩn cho các hàm khác nếu cần
	"sort"
	"sync"

	"github.com/blockchain/consensus/logger" // Import logger tùy chỉnh của bạn
	// Các import khác có thể cần thiết nếu bạn mở rộng DagStore
)

// QUORUM là ngưỡng bỏ phiếu (theo stake) cần thiết để quyết định trạng thái Clotho.
// Ví dụ: 2/3 tổng Stake + 1.
// Giá trị này cần được đặt dựa trên tổng stake của mạng lưới.
const QUORUM uint64 = 2 // Ví dụ: giả sử tổng stake của các validator là 100, quorum > 2/3*100
// DagStore là cấu trúc quản lý các Event Block trong DAG và logic Clotho Selection.
type DagStore struct {
	events           map[EventID]*Event   // Lưu trữ tất cả các event theo EventID (hash)
	latestEvents     map[string]EventID   // Key: chuỗi hex public key của creator -> EventID mới nhất
	rootsByFrame     map[uint64][]EventID // Key: frame number -> slice các EventID của Root trong frame đó
	lastDecidedFrame uint64               // Frame cuối cùng mà Clotho đã được quyết định cho tất cả các root
	mu               sync.RWMutex         // Mutex để đồng bộ hóa truy cập vào các map và trường của DagStore
	stake            map[string]uint64    // Key: chuỗi hex public key của creator (validator) -> stake amount
}

// NewDagStore tạo một instance mới của DagStore.
// initialStake: map từ chuỗi hex public key của validator sang stake amount.
func NewDagStore(initialStake map[string]uint64) *DagStore {
	// Sao chép initialStake để tránh sửa đổi từ bên ngoài
	stakeMap := make(map[string]uint64)
	for pubKeyHex, s := range initialStake {
		stakeMap[pubKeyHex] = s
	}

	return &DagStore{
		events:           make(map[EventID]*Event),
		latestEvents:     make(map[string]EventID),
		rootsByFrame:     make(map[uint64][]EventID),
		lastDecidedFrame: 0, // Hoặc 1 tùy theo quy ước frame bắt đầu
		mu:               sync.RWMutex{},
		stake:            stakeMap,
	}
}

// AddEvent thêm một event mới vào DagStore.
func (ds *DagStore) AddEvent(event *Event) error {
	if event == nil {
		return errors.New("cannot add nil event")
	}

	eventID := event.GetEventId()
	if eventID.IsZero() {
		return errors.New("event hash is zero, cannot add to DagStore")
	}

	ds.mu.Lock()
	defer ds.mu.Unlock()

	if _, exists := ds.events[eventID]; exists {
		return nil
	}

	creatorKey := hex.EncodeToString(event.EventData.Creator)

	currentLatestEventID, creatorHasPrevious := ds.latestEvents[creatorKey]
	if creatorHasPrevious {
		if event.EventData.SelfParent.IsZero() && event.EventData.Index > 1 {
			return fmt.Errorf("event %s by %s (Index %d) has zero self parent but creator has previous events (latest: %s, Index %d) and index > 1",
				eventID.String()[:6], creatorKey, event.EventData.Index, currentLatestEventID.String()[:6], ds.events[currentLatestEventID].EventData.Index)
		}
		if event.EventData.Index > 1 && event.EventData.SelfParent != currentLatestEventID {
			var expectedIndex uint64
			if selfParentEvent, ok := ds.events[currentLatestEventID]; ok {
				expectedIndex = selfParentEvent.EventData.Index
			}
			return fmt.Errorf("invalid self parent for event %s by %s (Index %d): expected %s (Index %d), got %s (Index %d)",
				eventID.String()[:6], creatorKey, event.EventData.Index,
				currentLatestEventID.String()[:6], expectedIndex,
				event.EventData.SelfParent.String()[:6], event.EventData.Index-1)
		}
		if prevEvent, ok := ds.events[currentLatestEventID]; ok {
			if event.EventData.Index != prevEvent.EventData.Index+1 {
				return fmt.Errorf("invalid index for event %s by %s: expected %d, got %d",
					eventID.String()[:6], creatorKey, prevEvent.EventData.Index+1, event.EventData.Index)
			}
		}

	} else {
		if !event.EventData.SelfParent.IsZero() {
			return fmt.Errorf("first event %s of creator %s must have zero self parent, got %s", eventID.String()[:6], creatorKey, event.EventData.SelfParent.String()[:6])
		}
		if event.EventData.Index != 1 {
			return fmt.Errorf("first event %s of creator %s must have index 1, got %d", eventID.String()[:6], creatorKey, event.EventData.Index)
		}
		if _, exists := ds.stake[creatorKey]; !exists {
			log.Printf("DagStore.AddEvent: Warning - Creator %s (%s) adding first event but has no registered stake.", creatorKey, eventID.String()[:6])
		}
	}

	for _, otherParentID := range event.EventData.OtherParents {
		if otherParentID.IsZero() {
			continue
		}
		if _, exists := ds.events[otherParentID]; !exists {
			log.Printf("DagStore.AddEvent: Warning - Other parent %s for event %s does not exist in DagStore. Event might be an orphan temporarily.", otherParentID.String(), eventID.String())
		}
	}

	ds.events[eventID] = event

	if !creatorHasPrevious || (ds.events[currentLatestEventID] != nil && event.EventData.Index > ds.events[currentLatestEventID].EventData.Index) {
		ds.latestEvents[creatorKey] = eventID
	}

	if event.EventData.IsRoot {
		isAlreadyRoot := false
		for _, existingRootID := range ds.rootsByFrame[event.EventData.Frame] {
			if existingRootID == eventID {
				isAlreadyRoot = true
				break
			}
		}
		if !isAlreadyRoot {
			ds.rootsByFrame[event.EventData.Frame] = append(ds.rootsByFrame[event.EventData.Frame], eventID)
			sort.Slice(ds.rootsByFrame[event.EventData.Frame], func(i, j int) bool {
				return ds.rootsByFrame[event.EventData.Frame][i].String() < ds.rootsByFrame[event.EventData.Frame][j].String()
			})
		}
	}
	log.Printf("DagStore.AddEvent: Successfully added event: %s (Creator: %s, Index: %d, Frame: %d, IsRoot: %t)",
		eventID.String()[:6], creatorKey, event.EventData.Index, event.EventData.Frame, event.EventData.IsRoot)
	return nil
}

// GetEvent lấy một event từ store bằng EventID của nó.
func (ds *DagStore) GetEvent(id EventID) (*Event, bool) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	event, exists := ds.events[id]
	return event, exists
}

// EventExists kiểm tra xem một event có tồn tại trong store không bằng EventID của nó.
func (ds *DagStore) EventExists(id EventID) bool {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	_, exists := ds.events[id]
	return exists
}

// GetLatestEventIDByCreatorPubKeyHex trả về EventID mới nhất được tạo bởi một creator cụ thể.
func (ds *DagStore) GetLatestEventIDByCreatorPubKeyHex(creatorPubKeyHex string) (EventID, bool) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	eventID, exists := ds.latestEvents[creatorPubKeyHex]
	return eventID, exists
}

// GetRoots trả về slice chứa EventID của tất cả các root trong một frame cụ thể.
func (ds *DagStore) GetRoots(frame uint64) []EventID {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	roots, exists := ds.rootsByFrame[frame]
	if !exists || len(roots) == 0 {
		return []EventID{}
	}
	rootsCopy := make([]EventID, len(roots))
	copy(rootsCopy, roots)
	return rootsCopy
}

// GetLastDecidedFrame trả về frame cuối cùng mà Clotho đã được quyết định.
func (ds *DagStore) GetLastDecidedFrame() uint64 {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.lastDecidedFrame
}

// getStakeLocked lấy stake của một validator dựa trên chuỗi hex public key.
func (ds *DagStore) getStakeLocked(creatorPubKeyHex string) uint64 {
	stake, exists := ds.stake[creatorPubKeyHex]
	if !exists {
		return 0
	}
	return stake
}

// isAncestorLocked kiểm tra xem event ancestorID có phải là tổ tiên của event descendantID không.
func (ds *DagStore) isAncestorLocked(ancestorID, descendantID EventID) bool {
	if ancestorID == descendantID {
		return true
	}
	if ancestorID.IsZero() {
		return false
	}
	if descendantID.IsZero() {
		return false
	}

	queue := []EventID{descendantID}
	visited := make(map[EventID]bool)
	visited[descendantID] = true

	for len(queue) > 0 {
		currentID := queue[0]
		queue = queue[1:]

		currentEvent, exists := ds.events[currentID]
		if !exists {
			logger.Warn(fmt.Sprintf("DagStore.isAncestorLocked: Warning - Event %s not found in store during check for descendant %s, ancestor %s",
				currentID.String()[:6], descendantID.String()[:6], ancestorID.String()[:6]))
			continue
		}

		var parents []EventID
		if !currentEvent.EventData.SelfParent.IsZero() {
			parents = append(parents, currentEvent.EventData.SelfParent)
		}
		if len(currentEvent.EventData.OtherParents) > 0 {
			parents = append(parents, currentEvent.EventData.OtherParents...)
		}

		for _, parentID := range parents {
			if parentID.IsZero() {
				continue
			}
			if parentID == ancestorID {
				return true
			}
			if !visited[parentID] {
				visited[parentID] = true
				queue = append(queue, parentID)
			}
		}
	}
	return false
}

// IsAncestor là phiên bản public của isAncestorLocked, quản lý lock.
func (ds *DagStore) IsAncestor(ancestorID, descendantID EventID) bool {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.isAncestorLocked(ancestorID, descendantID)
}

// forklessCause kiểm tra xem event x có "forkless cause" event y không.
func (ds *DagStore) forklessCause(x, y *Event) bool {
	if x == nil || y == nil {
		return false
	}
	return ds.isAncestorLocked(x.GetEventId(), y.GetEventId())
}

// DecideClotho thực hiện thuật toán lựa chọn Clotho.
func (ds *DagStore) DecideClotho() {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	logger.Info("DecideClotho: Bắt đầu")

	startFrame := ds.lastDecidedFrame + 1
	var maxFrame uint64 = 0
	for frame := range ds.rootsByFrame {
		if frame > maxFrame {
			maxFrame = frame
		}
	}
	logger.Info(fmt.Sprintf("DecideClotho: startFrame=%d, maxFrame=%d, lastDecidedFrame=%d", startFrame, maxFrame, ds.lastDecidedFrame))

	if startFrame > maxFrame {
		logger.Info("DecideClotho: Không có frame mới để xử lý.")
		return
	}

	for xFrame := startFrame; xFrame <= maxFrame; xFrame++ {
		logger.Info(fmt.Sprintf("DecideClotho: Đang xử lý xFrame = %d", xFrame))
		xRootsIDsInCurrentXFrame := ds.rootsByFrame[xFrame]
		if len(xRootsIDsInCurrentXFrame) == 0 {
			logger.Info(fmt.Sprintf("DecideClotho: Không có root nào trong xFrame %d.", xFrame))
		}

		xEventsToProcess := make([]*Event, 0, len(xRootsIDsInCurrentXFrame))
		for _, xID := range xRootsIDsInCurrentXFrame {
			xEvent, exists := ds.events[xID]
			if exists && xEvent.EventData.IsRoot && xEvent.ClothoStatus == ClothoUndecided {
				xEventsToProcess = append(xEventsToProcess, xEvent)
			}
		}

		if len(xEventsToProcess) == 0 {
			if len(xRootsIDsInCurrentXFrame) > 0 {
				logger.Info(fmt.Sprintf("DecideClotho: Tất cả các root trong xFrame %d đã được quyết định trước đó.", xFrame))
			}
		} else {
			logger.Info(fmt.Sprintf("DecideClotho: Tìm thấy %d root chưa quyết định trong xFrame %d để xử lý.", len(xEventsToProcess), xFrame))
		}

	next_x_event_processing_loop:
		for _, xEvent := range xEventsToProcess {
			xID := xEvent.GetEventId()
			logger.Info(fmt.Sprintf("DecideClotho: Đang xử lý xEvent %s (Frame %d)", xID.String()[:6], xFrame))

			for yFrame := xFrame + 1; yFrame <= maxFrame; yFrame++ {
				yRootsIDsInCurrentYFrame := ds.rootsByFrame[yFrame]
				if len(yRootsIDsInCurrentYFrame) == 0 {
					continue
				}

				for _, yID := range yRootsIDsInCurrentYFrame {
					yEvent, yExists := ds.events[yID]
					if !yExists || !yEvent.EventData.IsRoot {
						continue
					}

					round := yEvent.EventData.Frame - xEvent.EventData.Frame

					if round == 1 {
						vote := ds.forklessCause(xEvent, yEvent)
						yEvent.SetVote(xID, vote)
					} else if round >= 2 {
						prevVotersFrame := yFrame - 1
						prevVotersIDs := ds.rootsByFrame[prevVotersFrame]

						yesVotesStake := uint64(0)
						noVotesStake := uint64(0)

						for _, prevRootID := range prevVotersIDs {
							prevRootEvent, prevRootExists := ds.events[prevRootID]
							if !prevRootExists || !prevRootEvent.EventData.IsRoot {
								continue
							}

							if ds.forklessCause(prevRootEvent, yEvent) {
								prevVote, voteExists := prevRootEvent.GetVote(xID)
								if voteExists {
									prevVoterStake := ds.getStakeLocked(hex.EncodeToString(prevRootEvent.EventData.Creator))
									if prevVote {
										yesVotesStake += prevVoterStake
									} else {
										noVotesStake += prevVoterStake
									}
								}
							}
						}

						yFinalVote := (yesVotesStake >= noVotesStake)
						yEvent.SetVote(xID, yFinalVote)

						if yesVotesStake >= QUORUM {
							xEvent.SetCandidate(true)
							xEvent.SetClothoStatus(ClothoIsClotho)
							logger.Info(fmt.Sprintf("DecideClotho: QUYẾT ĐỊNH - Root %s (F%d) LÀ CLOTHO. Quyết định bởi phiếu bầu tổng hợp tại yEvent %s (F%d). YesStake: %d >= Quorum: %d",
								xID.String()[:6], xFrame, yID.String()[:6], yFrame, yesVotesStake, QUORUM))
							continue next_x_event_processing_loop
						}
						if noVotesStake >= QUORUM {
							xEvent.SetCandidate(false)
							xEvent.SetClothoStatus(ClothoIsNotClotho)
							logger.Info(fmt.Sprintf("DecideClotho: QUYẾT ĐỊNH - Root %s (F%d) KHÔNG PHẢI CLOTHO. Quyết định bởi phiếu bầu tổng hợp tại yEvent %s (F%d). NoStake: %d >= Quorum: %d",
								xID.String()[:6], xFrame, yID.String()[:6], yFrame, noVotesStake, QUORUM))
							continue next_x_event_processing_loop
						}
					}
				}
			}

			if xEvent.ClothoStatus == ClothoUndecided {
				logger.Info(fmt.Sprintf("DecideClotho: Root %s (F%d) vẫn UNDECIDED sau khi kiểm tra tất cả y-frames đến %d", xID.String()[:6], xFrame, maxFrame))
			}
		}
		logger.Info(fmt.Sprintf("DecideClotho: Hoàn tất xử lý các xEvent cho xFrame %d", xFrame))

		allRootsInXFrameDecided := true
		if len(xRootsIDsInCurrentXFrame) > 0 {
			for _, rootID := range xRootsIDsInCurrentXFrame {
				rootEvent, exists := ds.events[rootID]
				if !exists || !rootEvent.EventData.IsRoot || rootEvent.ClothoStatus == ClothoUndecided {
					allRootsInXFrameDecided = false
					logger.Info(fmt.Sprintf("DecideClotho: xFrame %d chưa được quyết định hoàn toàn. Root %s còn UNDECIDED (exists: %t, isRoot: %t).", xFrame, rootID.String()[:6], exists, exists && rootEvent.EventData.IsRoot))
					break
				}
			}
		} else {
			allRootsInXFrameDecided = true
			logger.Info(fmt.Sprintf("DecideClotho: xFrame %d rỗng, coi như đã quyết định.", xFrame))
		}

		logger.Info(fmt.Sprintf("DecideClotho: Kết thúc xFrame %d. allRootsInXFrameDecided = %t", xFrame, allRootsInXFrameDecided))

		if allRootsInXFrameDecided {
			ds.lastDecidedFrame = xFrame
			logger.Info(fmt.Sprintf("DecideClotho: Cập nhật lastDecidedFrame thành %d.", ds.lastDecidedFrame))
		} else {
			logger.Info(fmt.Sprintf("DecideClotho: xFrame %d chưa được quyết định hoàn toàn. Dừng DecideClotho cho các frame sau. lastDecidedFrame hiện tại: %d.", xFrame, ds.lastDecidedFrame))
			break
		}
	}
	logger.Info(fmt.Sprintf("DecideClotho: Kết thúc. lastDecidedFrame cuối cùng: %d", ds.lastDecidedFrame))
}

// PruneOldEvents loại bỏ các event từ các frame cũ hơn oldestFrameToKeep.
// oldestFrameToKeep là frame CŨ NHẤT mà bạn muốn GIỮ LẠI.
// Các event thuộc frame < oldestFrameToKeep sẽ bị xóa.
func (ds *DagStore) PruneOldEvents(oldestFrameToKeep uint64) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	logger.Info(fmt.Sprintf("PruneOldEvents: Bắt đầu dọn dẹp các event thuộc frame cũ hơn %d. lastDecidedFrame hiện tại: %d", oldestFrameToKeep, ds.lastDecidedFrame))

	if oldestFrameToKeep == 0 { // Không cho phép xóa frame 0 nếu frame 0 có ý nghĩa đặc biệt hoặc frame bắt đầu từ 1
		logger.Warn("PruneOldEvents: oldestFrameToKeep là 0, không thực hiện dọn dẹp.")
		return
	}

	// Điều kiện an toàn: Chỉ cho phép pruning nếu oldestFrameToKeep <= ds.lastDecidedFrame.
	// Điều này đảm bảo rằng frame cuối cùng đã được quyết định (ds.lastDecidedFrame) sẽ không bị xóa,
	// mà chỉ các frame cũ hơn nó một cách nghiêm ngặt mới được xem xét để xóa.
	// oldestFrameToKeep là frame CŨ NHẤT mà chúng ta muốn GIỮ LẠI.
	// Các frame F < oldestFrameToKeep sẽ bị xóa.
	// Để frame ds.lastDecidedFrame không bị xóa, thì ds.lastDecidedFrame phải >= oldestFrameToKeep.
	// Hay nói cách khác, oldestFrameToKeep phải <= ds.lastDecidedFrame.
	// Nếu oldestFrameToKeep > ds.lastDecidedFrame, việc pruning sẽ bị bỏ qua.
	if oldestFrameToKeep > ds.lastDecidedFrame {
		logger.Info(fmt.Sprintf("PruneOldEvents: Yêu cầu giữ lại các frame từ %d (oldestFrameToKeep). Frame này lớn hơn frame đã quyết định cuối cùng (%d). Không thực hiện dọn dẹp để đảm bảo an toàn, vì điều này có nghĩa là không có frame nào cũ hơn %d để xóa hoặc frame %d là frame hiện tại/tương lai.", oldestFrameToKeep, ds.lastDecidedFrame, oldestFrameToKeep, oldestFrameToKeep))
		return
	}

	eventsToDelete := make(map[EventID]struct{}) // Sử dụng map để tránh trùng lặp và xóa hiệu quả

	// 1. Xác định các event cần xóa từ ds.events
	for id, event := range ds.events {
		if event.EventData.Frame < oldestFrameToKeep {
			eventsToDelete[id] = struct{}{}
		}
	}

	if len(eventsToDelete) == 0 {
		logger.Info(fmt.Sprintf("PruneOldEvents: Không có event nào cũ hơn frame %d để xóa.", oldestFrameToKeep))
		return
	}

	logger.Info(fmt.Sprintf("PruneOldEvents: Sẽ xóa %d event.", len(eventsToDelete)))

	// 2. Xóa các event đã xác định khỏi ds.events
	for id := range eventsToDelete {
		delete(ds.events, id)
	}

	// 3. Dọn dẹp ds.rootsByFrame
	// Xóa toàn bộ các entry frame cũ hơn oldestFrameToKeep
	framesProcessedForRoots := make(map[uint64]bool) // Để tránh log lặp lại cho cùng một frame
	for frame := range ds.rootsByFrame {
		if frame < oldestFrameToKeep {
			if !framesProcessedForRoots[frame] { // Log một lần cho mỗi frame bị xóa khỏi rootsByFrame
				logger.Debug(fmt.Sprintf("PruneOldEvents: Xóa entry cho frame %d khỏi rootsByFrame.", frame))
				framesProcessedForRoots[frame] = true
			}
			delete(ds.rootsByFrame, frame)
		} else {
			// Đối với các frame còn lại, lọc ra các rootID đã bị xóa
			var validRoots []EventID
			initialRootCount := len(ds.rootsByFrame[frame])
			for _, rootID := range ds.rootsByFrame[frame] {
				if _, deleted := eventsToDelete[rootID]; !deleted { // Check if the root event itself was deleted
					validRoots = append(validRoots, rootID)
				}
			}

			if len(validRoots) < initialRootCount && initialRootCount > 0 { // Log nếu có root bị xóa khỏi frame này
				logger.Debug(fmt.Sprintf("PruneOldEvents: Đã xóa %d root(s) khỏi frame %d trong rootsByFrame. Còn lại %d root(s).", initialRootCount-len(validRoots), frame, len(validRoots)))
			}

			if len(validRoots) == 0 && initialRootCount > 0 { // Nếu frame từng có root nhưng giờ không còn root nào hợp lệ
				delete(ds.rootsByFrame, frame)
			} else if len(validRoots) > 0 { // Only update if there are valid roots left
				ds.rootsByFrame[frame] = validRoots
			} else if initialRootCount == 0 { // Frame này vốn đã rỗng, không cần làm gì
				// Có thể xóa nếu nó là key rỗng, nhưng để an toàn, chỉ xóa nếu nó có entry và giờ rỗng
				// delete(ds.rootsByFrame, frame) // Tùy chọn: xóa key nếu giá trị là slice rỗng
			}
		}
	}

	// Hàm helper để cắt ngắn chuỗi hex một cách an toàn
	min := func(a, b int) int {
		if a < b {
			return a
		}
		return b
	}

	// 4. Dọn dẹp ds.latestEvents
	// Nếu latestEvent của một creator đã bị xóa, tìm và cập nhật lại latest event mới nhất của họ từ các event còn lại.
	creatorsToRecheck := make([]string, 0, len(ds.latestEvents))
	for creatorKey := range ds.latestEvents {
		creatorsToRecheck = append(creatorsToRecheck, creatorKey)
	}

	for _, creatorKey := range creatorsToRecheck {
		currentLatestID, existsInMap := ds.latestEvents[creatorKey]
		if !existsInMap {
			continue
		}

		if _, wasDeleted := eventsToDelete[currentLatestID]; wasDeleted {
			logger.Info(fmt.Sprintf("PruneOldEvents: Latest event %s của creator %s đã bị xóa. Tìm kiếm latest event mới...", currentLatestID.String()[:6], creatorKey[:min(6, len(creatorKey))]))

			delete(ds.latestEvents, creatorKey)

			var newLatestEventForCreator *Event = nil
			for _, event := range ds.events { // ds.events giờ chỉ chứa các event không bị prune
				if hex.EncodeToString(event.EventData.Creator) == creatorKey {
					if newLatestEventForCreator == nil || event.EventData.Index > newLatestEventForCreator.EventData.Index {
						newLatestEventForCreator = event
					}
				}
			}

			if newLatestEventForCreator != nil {
				ds.latestEvents[creatorKey] = newLatestEventForCreator.GetEventId()
				logger.Info(fmt.Sprintf("PruneOldEvents: Đã cập nhật latest event cho creator %s thành %s (Index: %d, Frame: %d)",
					creatorKey[:min(6, len(creatorKey))],
					newLatestEventForCreator.GetEventId().String()[:6],
					newLatestEventForCreator.EventData.Index,
					newLatestEventForCreator.EventData.Frame))
			} else {
				logger.Info(fmt.Sprintf("PruneOldEvents: Không tìm thấy event nào còn lại cho creator %s sau khi pruning. Entry trong latestEvents vẫn bị xóa.", creatorKey[:min(6, len(creatorKey))]))
			}
		}
	}

	logger.Info(fmt.Sprintf("PruneOldEvents: Hoàn tất. Đã xóa %d event. Số event còn lại: %d. Số frame trong rootsByFrame: %d. Số entry trong latestEvents: %d",
		len(eventsToDelete), len(ds.events), len(ds.rootsByFrame), len(ds.latestEvents)))
}

// GetDecidedRoots trả về một map các EventID tới *Event cho các root đã có quyết định Clotho.
func (ds *DagStore) GetDecidedRoots() map[EventID]*Event {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	decided := make(map[EventID]*Event)
	for id, event := range ds.events {
		if event.EventData.IsRoot && event.ClothoStatus != ClothoUndecided {
			decided[id] = event
		}
	}
	return decided
}

// GetRootStatus trả về trạng thái Clotho của một Root.
func (ds *DagStore) GetRootStatus(rootID EventID) (ClothoStatus, bool) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	event, exists := ds.events[rootID]
	if !exists || !event.EventData.IsRoot {
		return ClothoUndecided, false
	}
	return event.ClothoStatus, true
}

// GetHeightForNode trả về chỉ số (index) của event mới nhất được tạo bởi node có NodeID đã cho.
func (ds *DagStore) GetHeightForNode(nodeCreatorPubKeyHex string) (uint64, bool) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	latestEventID, exists := ds.latestEvents[nodeCreatorPubKeyHex]
	if !exists {
		return 0, false
	}

	event, eventExists := ds.events[latestEventID]
	if !eventExists {
		log.Printf("DagStore.GetHeightForNode: Error - Latest event %s for creator %s not found in events map.", latestEventID.String(), nodeCreatorPubKeyHex)
		return 0, false
	}
	return event.EventData.Index, true
}

// GetInDegreeForNode trả về "in-degree" cho event mới nhất của node có NodeID đã cho.
func (ds *DagStore) GetInDegreeForNode(nodeCreatorPubKeyHex string) (uint64, bool) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	latestEventID, exists := ds.latestEvents[nodeCreatorPubKeyHex]
	if !exists {
		return 0, false
	}

	event, eventExists := ds.events[latestEventID]
	if !eventExists {
		log.Printf("DagStore.GetInDegreeForNode: Error - Latest event %s for creator %s not found in DagStore for in-degree.", latestEventID.String(), nodeCreatorPubKeyHex)
		return 0, false
	}

	if len(event.EventData.OtherParents) == 0 {
		return 0, true
	}

	distinctCreators := make(map[string]struct{})
	for _, parentEventID := range event.EventData.OtherParents {
		if parentEventID.IsZero() {
			continue
		}
		parentEvent, parentExists := ds.events[parentEventID]
		if !parentExists {
			log.Printf("DagStore.GetInDegreeForNode: Warning - Parent event %s not found for in-degree of node %s's event %s", parentEventID.String(), nodeCreatorPubKeyHex, latestEventID.String())
			continue
		}
		parentCreatorHex := hex.EncodeToString(parentEvent.EventData.Creator)
		distinctCreators[parentCreatorHex] = struct{}{}
	}
	return uint64(len(distinctCreators)), true
}

// GetAllEventsSnapshot trả về một slice chứa bản sao của tất cả các event trong store.
func (ds *DagStore) GetAllEventsSnapshot() []*Event {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	events := make([]*Event, 0, len(ds.events))
	for _, event := range ds.events {
		events = append(events, event)
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].EventData.Frame != events[j].EventData.Frame {
			return events[i].EventData.Frame < events[j].EventData.Frame
		}
		if events[i].EventData.Timestamp != events[j].EventData.Timestamp {
			return events[i].EventData.Timestamp < events[j].EventData.Timestamp
		}
		return events[i].GetEventId().String() < events[j].GetEventId().String()
	})
	return events
}

// GetLatestEventsMapSnapshot trả về một bản sao của map latestEvents.
func (ds *DagStore) GetLatestEventsMapSnapshot() map[string]EventID {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	latestCopy := make(map[string]EventID, len(ds.latestEvents))
	for k, v := range ds.latestEvents {
		latestCopy[k] = v
	}
	return latestCopy
}

// GetEventsByCreatorSinceIndex trả về các event của một creator cụ thể
// bắt đầu từ một index cho trước (không bao gồm index đó).
func (ds *DagStore) GetEventsByCreatorSinceIndex(creatorPubKeyHex string, startIndex uint64) []*Event {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	var result []*Event
	creatorBytes, err := hex.DecodeString(creatorPubKeyHex)
	if err != nil {
		// Nên sử dụng logger của bạn ở đây thay vì log.Printf
		logger.Error(fmt.Sprintf("DagStore.GetEventsByCreatorSinceIndex: Invalid creatorPubKeyHex %s: %v", creatorPubKeyHex, err))
		return result
	}

	var tempEvents []*Event
	for _, event := range ds.events {
		isSameCreator := true
		if len(event.EventData.Creator) != len(creatorBytes) {
			isSameCreator = false
		} else {
			for i := range event.EventData.Creator {
				if event.EventData.Creator[i] != creatorBytes[i] {
					isSameCreator = false
					break
				}
			}
		}

		if isSameCreator && event.EventData.Index > startIndex {
			tempEvents = append(tempEvents, event)
		}
	}

	sort.Slice(tempEvents, func(i, j int) bool {
		return tempEvents[i].EventData.Index < tempEvents[j].EventData.Index
	})
	return tempEvents
}
