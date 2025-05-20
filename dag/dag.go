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
const QUORUM uint64 = 67 // Ví dụ: giả sử tổng stake của các validator là 100, quorum > 2/3*100

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
	// Sao chép initialStake để tránh sửa đổi từ bên ngoài (nếu cần)
	stakeMap := make(map[string]uint64)
	if initialStake != nil {
		for pubKeyHex, s := range initialStake {
			stakeMap[pubKeyHex] = s
		}
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
// Thực hiện các kiểm tra cơ bản, cập nhật latest event và thêm vào rootsByFrame nếu là root.
func (ds *DagStore) AddEvent(event *Event) error {
	if event == nil {
		return errors.New("cannot add nil event")
	}

	eventID := event.GetEventId() // Lấy EventID đã được cache hoặc tính toán
	if eventID.IsZero() {
		// Hash() đã được gọi trong NewEvent, nếu vẫn zero thì có vấn đề
		return errors.New("event hash is zero, cannot add to DagStore")
	}

	ds.mu.Lock() // Khóa ghi khi thêm/cập nhật dữ liệu
	defer ds.mu.Unlock()

	// 1. Kiểm tra xem event đã tồn tại chưa
	if _, exists := ds.events[eventID]; exists {
		// Event đã tồn tại, không coi là lỗi nghiêm trọng, chỉ log và bỏ qua
		// log.Printf("DagStore.AddEvent: Event with ID %s already exists, skipping.", eventID.String())
		return nil // Không trả về lỗi để quá trình đồng bộ có thể tiếp tục với các event khác
	}

	// Lấy creator public key dạng hex string làm key cho map latestEvents và stake
	creatorKey := hex.EncodeToString(event.EventData.Creator)

	// 2. Kiểm tra tính hợp lệ cơ bản của SelfParent
	currentLatestEventID, creatorHasPrevious := ds.latestEvents[creatorKey]
	if creatorHasPrevious {
		if event.EventData.SelfParent.IsZero() && event.EventData.Index > 1 { // Chỉ event đầu tiên (index 1) mới có self-parent zero
			return fmt.Errorf("event %s by %s (Index %d) has zero self parent but creator has previous events (latest: %s, Index %d) and index > 1",
				eventID.String()[:6], creatorKey, event.EventData.Index, currentLatestEventID.String()[:6], ds.events[currentLatestEventID].EventData.Index)
		}
		// Chỉ kiểm tra self-parent nếu event.Index > 1
		if event.EventData.Index > 1 && event.EventData.SelfParent != currentLatestEventID {
			// Lấy index của self-parent để so sánh
			var expectedIndex uint64
			if selfParentEvent, ok := ds.events[currentLatestEventID]; ok {
				expectedIndex = selfParentEvent.EventData.Index
			}
			return fmt.Errorf("invalid self parent for event %s by %s (Index %d): expected %s (Index %d), got %s (Index %d)",
				eventID.String()[:6], creatorKey, event.EventData.Index,
				currentLatestEventID.String()[:6], expectedIndex,
				event.EventData.SelfParent.String()[:6], event.EventData.Index-1)
		}
		// Kiểm tra index phải tăng dần
		if prevEvent, ok := ds.events[currentLatestEventID]; ok {
			if event.EventData.Index != prevEvent.EventData.Index+1 {
				return fmt.Errorf("invalid index for event %s by %s: expected %d, got %d",
					eventID.String()[:6], creatorKey, prevEvent.EventData.Index+1, event.EventData.Index)
			}
		}

	} else { // Event đầu tiên của creator
		if !event.EventData.SelfParent.IsZero() {
			return fmt.Errorf("first event %s of creator %s must have zero self parent, got %s", eventID.String()[:6], creatorKey, event.EventData.SelfParent.String()[:6])
		}
		if event.EventData.Index != 1 {
			return fmt.Errorf("first event %s of creator %s must have index 1, got %d", eventID.String()[:6], creatorKey, event.EventData.Index)
		}
		// Kiểm tra xem creator có stake không (quan trọng nếu là validator)
		if _, exists := ds.stake[creatorKey]; !exists {
			log.Printf("DagStore.AddEvent: Warning - Creator %s (%s) adding first event but has no registered stake.", creatorKey, eventID.String()[:6])
		}
	}

	// 3. Kiểm tra xem OtherParents có tồn tại trong store không
	for _, otherParentID := range event.EventData.OtherParents {
		if otherParentID.IsZero() {
			continue
		}
		if _, exists := ds.events[otherParentID]; !exists {
			log.Printf("DagStore.AddEvent: Warning - Other parent %s for event %s does not exist in DagStore. Event might be an orphan temporarily.", otherParentID.String(), eventID.String())
		}
	}

	// 4. Lưu event vào store
	ds.events[eventID] = event

	// 5. Cập nhật latest event cho creator này, chỉ nếu event mới này có index lớn hơn
	if !creatorHasPrevious || (ds.events[currentLatestEventID] != nil && event.EventData.Index > ds.events[currentLatestEventID].EventData.Index) {
		ds.latestEvents[creatorKey] = eventID
	}

	// 6. Nếu event là Root, thêm vào danh sách rootsByFrame
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
	ds.mu.RLock() // Khóa đọc
	defer ds.mu.RUnlock()
	event, exists := ds.events[id]
	return event, exists
}

// EventExists kiểm tra xem một event có tồn tại trong store không bằng EventID của nó.
func (ds *DagStore) EventExists(id EventID) bool {
	ds.mu.RLock() // Khóa đọc
	defer ds.mu.RUnlock()
	_, exists := ds.events[id]
	return exists
}

// GetLatestEventIDByCreatorPubKeyHex trả về EventID mới nhất được tạo bởi một creator cụ thể.
func (ds *DagStore) GetLatestEventIDByCreatorPubKeyHex(creatorPubKeyHex string) (EventID, bool) {
	ds.mu.RLock() // Khóa đọc
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

// getStake lấy stake của một validator dựa trên chuỗi hex public key.
// Hàm này giả định lock đã được giữ bởi hàm gọi nó nếu cần.
func (ds *DagStore) getStakeLocked(creatorPubKeyHex string) uint64 {
	// Không RLock ở đây vì hàm này được thiết kế để gọi khi lock đã được giữ
	stake, exists := ds.stake[creatorPubKeyHex]
	if !exists {
		return 0
	}
	return stake
}

// isAncestorLocked kiểm tra xem event ancestorID có phải là tổ tiên của event descendantID không.
// Hàm này KHÔNG tự quản lý lock, nó giả định lock đã được giữ bởi hàm gọi nó.
// Sử dụng BFS để duyệt ngược từ descendantID.
func (ds *DagStore) isAncestorLocked(ancestorID, descendantID EventID) bool {
	// KHÔNG CÓ ds.mu.RLock() ở đây

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

		currentEvent, exists := ds.events[currentID] // Truy cập trực tiếp ds.events
		if !exists {
			// Sử dụng logger.Warn hoặc log.Printf tùy theo chuẩn của bạn
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
// Hàm này giả định lock đã được giữ bởi hàm gọi nó (ví dụ: DecideClotho).
func (ds *DagStore) forklessCause(x, y *Event) bool {
	if x == nil || y == nil {
		return false
	}
	// Gọi isAncestorLocked vì DecideClotho đã giữ ds.mu.Lock()
	return ds.isAncestorLocked(x.GetEventId(), y.GetEventId())
}

// DecideClotho thực hiện thuật toán lựa chọn Clotho.
// Nó duyệt qua các Root và tính toán phiếu bầu để quyết định trạng thái Clotho.
func (ds *DagStore) DecideClotho() {
	ds.mu.Lock() // Khóa ghi cho toàn bộ quá trình quyết định
	defer ds.mu.Unlock()
	logger.Info("DecideClotho: Bắt đầu") // Sử dụng logger của bạn

	startFrame := ds.lastDecidedFrame + 1
	var maxFrame uint64 = 0 // Khởi tạo maxFrame
	// Tìm frame cao nhất hiện có các roots để biết giới hạn duyệt
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

	// Duyệt qua các Root (x) từ frame sau frame cuối cùng đã quyết định
	for xFrame := startFrame; xFrame <= maxFrame; xFrame++ {
		logger.Info(fmt.Sprintf("DecideClotho: Đang xử lý xFrame = %d", xFrame))
		xRootsIDsInCurrentXFrame := ds.rootsByFrame[xFrame]
		if len(xRootsIDsInCurrentXFrame) == 0 {
			logger.Info(fmt.Sprintf("DecideClotho: Không có root nào trong xFrame %d.", xFrame))
			// Nếu frame này rỗng, nó sẽ được coi là "quyết định xong" ở phần kiểm tra allRootsInXFrameDecided
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
			} else {
				// Đã log ở trên là không có root
			}
		} else {
			logger.Info(fmt.Sprintf("DecideClotho: Tìm thấy %d root chưa quyết định trong xFrame %d để xử lý.", len(xEventsToProcess), xFrame))
		}

	next_x_event_processing_loop:
		for _, xEvent := range xEventsToProcess {
			xID := xEvent.GetEventId()
			logger.Info(fmt.Sprintf("DecideClotho: Đang xử lý xEvent %s (Frame %d)", xID.String()[:6], xFrame))

			for yFrame := xFrame + 1; yFrame <= maxFrame; yFrame++ {
				// logger.Info(fmt.Sprintf("DecideClotho:  -> Đang xử lý yFrame = %d cho xEvent %s", yFrame, xID.String()[:6]))
				yRootsIDsInCurrentYFrame := ds.rootsByFrame[yFrame]
				if len(yRootsIDsInCurrentYFrame) == 0 {
					// logger.Info(fmt.Sprintf("DecideClotho:  -> Không có root nào trong yFrame %d", yFrame))
					continue
				}

				for _, yID := range yRootsIDsInCurrentYFrame {
					yEvent, yExists := ds.events[yID]
					if !yExists || !yEvent.EventData.IsRoot {
						continue
					}

					round := yEvent.EventData.Frame - xEvent.EventData.Frame
					// logger.Info(fmt.Sprintf("DecideClotho:  -> yEvent %s (Frame %d), round %d", yID.String()[:6], yFrame, round))

					if round == 1 {
						vote := ds.forklessCause(xEvent, yEvent) // forklessCause gọi isAncestorLocked
						yEvent.SetVote(xID, vote)
						// logger.Info(fmt.Sprintf("DecideClotho:  R1: Root %s (F%d) votes %t cho Root %s (F%d)", yID.String()[:6], yFrame, vote, xID.String()[:6], xFrame))
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

							if ds.forklessCause(prevRootEvent, yEvent) { // forklessCause gọi isAncestorLocked
								prevVote, voteExists := prevRootEvent.GetVote(xID)
								if voteExists {
									prevVoterStake := ds.getStakeLocked(hex.EncodeToString(prevRootEvent.EventData.Creator)) // Sử dụng getStakeLocked
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
						// logger.Info(fmt.Sprintf("DecideClotho:  R%d: Root %s (F%d) votes %t cho %s (F%d) (YesS: %d, NoS: %d)",
						// round, yID.String()[:6], yFrame, yFinalVote, xID.String()[:6], xFrame, yesVotesStake, noVotesStake))

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
				} // Kết thúc vòng lặp qua yID
			} // Kết thúc vòng lặp qua yFrame

			if xEvent.ClothoStatus == ClothoUndecided {
				logger.Info(fmt.Sprintf("DecideClotho: Root %s (F%d) vẫn UNDECIDED sau khi kiểm tra tất cả y-frames đến %d", xID.String()[:6], xFrame, maxFrame))
			}
		} // Kết thúc vòng lặp qua xEvent (next_x_event_processing_loop)
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
			allRootsInXFrameDecided = true // Frame rỗng được coi là đã quyết định
			logger.Info(fmt.Sprintf("DecideClotho: xFrame %d rỗng, coi như đã quyết định.", xFrame))
		}

		logger.Info(fmt.Sprintf("DecideClotho: Kết thúc xFrame %d. allRootsInXFrameDecided = %t", xFrame, allRootsInXFrameDecided))

		if allRootsInXFrameDecided {
			ds.lastDecidedFrame = xFrame
			logger.Info(fmt.Sprintf("DecideClotho: Cập nhật lastDecidedFrame thành %d.", ds.lastDecidedFrame))
		} else {
			logger.Info(fmt.Sprintf("DecideClotho: xFrame %d chưa được quyết định hoàn toàn. Dừng DecideClotho cho các frame sau. lastDecidedFrame hiện tại: %d.", xFrame, ds.lastDecidedFrame))
			break // Thoát khỏi vòng lặp duyệt các xFrame
		}
	} // Kết thúc vòng lặp qua xFrame
	logger.Info(fmt.Sprintf("DecideClotho: Kết thúc. lastDecidedFrame cuối cùng: %d", ds.lastDecidedFrame))
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
		log.Printf("DagStore.GetEventsByCreatorSinceIndex: Invalid creatorPubKeyHex %s: %v", creatorPubKeyHex, err)
		return result
	}

	// Tối ưu hóa: Duyệt từ latestEvent của creator ngược về trước nếu cần
	// Hoặc cần một cấu trúc dữ liệu phụ trợ: map[creatorPubKeyHex][index]*Event
	// Hiện tại, duyệt tất cả event:
	var tempEvents []*Event
	for _, event := range ds.events {
		// So sánh byte slices cho creator
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

	// Sắp xếp kết quả theo index
	sort.Slice(tempEvents, func(i, j int) bool {
		return tempEvents[i].EventData.Index < tempEvents[j].EventData.Index
	})
	return tempEvents
}
