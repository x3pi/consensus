package dag

import (
	"encoding/hex"
	"errors"
	"fmt"

	// "log" // Bỏ comment nếu bạn cần log để debug
	"sync"
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
		return fmt.Errorf("event with ID %s already exists", eventID.String())
	}

	// Lấy creator public key dạng hex string làm key cho map latestEvents và stake
	creatorKey := hex.EncodeToString(event.EventData.Creator)

	// 2. Kiểm tra tính hợp lệ cơ bản của SelfParent
	currentLatestEventID, creatorHasPrevious := ds.latestEvents[creatorKey]
	if creatorHasPrevious {
		if event.EventData.SelfParent.IsZero() {
			return fmt.Errorf("event %s by %s has zero self parent but creator has previous events (latest: %s)", eventID.String()[:6], creatorKey, currentLatestEventID.String()[:6])
		}
		if event.EventData.SelfParent != currentLatestEventID {
			return fmt.Errorf("invalid self parent for event %s by %s: expected %s, got %s", eventID.String()[:6], creatorKey, currentLatestEventID.String()[:6], event.EventData.SelfParent.String()[:6])
		}
	} else { // Event đầu tiên của creator
		if !event.EventData.SelfParent.IsZero() {
			return fmt.Errorf("first event %s of creator %s must have zero self parent, got %s", eventID.String()[:6], creatorKey, event.EventData.SelfParent.String()[:6])
		}
		// Kiểm tra xem creator có stake không (quan trọng nếu là validator)
		if _, exists := ds.stake[creatorKey]; !exists {
			// Tùy thuộc vào logic: có thể là lỗi, cảnh báo, hoặc chấp nhận nếu là non-validator.
			// log.Printf("Warning: Creator %s (%s) adding first event but has no registered stake.", creatorKey, eventID.String()[:6])
		}
	}

	// 3. Kiểm tra xem OtherParents có tồn tại trong store không
	// Lưu ý: Trong một hệ thống phân tán, parent có thể chưa được đồng bộ.
	// Cần có cơ chế xử lý "orphan events" (event mồ côi) nếu parent chưa có.
	for _, otherParentID := range event.EventData.OtherParents {
		if otherParentID.IsZero() { // OtherParents có thể là zero (ví dụ, event đầu tiên trong mạng)
			continue
		}
		if _, exists := ds.events[otherParentID]; !exists {
			return fmt.Errorf("other parent with ID %s for event %s does not exist in DagStore", otherParentID.String(), eventID.String())
		}
	}

	// 4. Lưu event vào store
	ds.events[eventID] = event

	// 5. Cập nhật latest event cho creator này
	ds.latestEvents[creatorKey] = eventID

	// 6. Nếu event là Root, thêm vào danh sách rootsByFrame
	if event.EventData.IsRoot {
		ds.rootsByFrame[event.EventData.Frame] = append(ds.rootsByFrame[event.EventData.Frame], eventID)
		// Có thể sắp xếp roots ở đây nếu cần tính nhất quán cao hơn khi duyệt
	}
	// log.Printf("Successfully added event: %s (Creator: %s, Index: %d, Frame: %d, IsRoot: %t)",
	//	eventID.String()[:6], creatorKey, event.EventData.Index, event.EventData.Frame, event.EventData.IsRoot)
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
// creatorPubKeyHex là chuỗi hex public key của creator.
func (ds *DagStore) GetLatestEventIDByCreatorPubKeyHex(creatorPubKeyHex string) (EventID, bool) {
	ds.mu.RLock() // Khóa đọc
	defer ds.mu.RUnlock()
	eventID, exists := ds.latestEvents[creatorPubKeyHex]
	return eventID, exists
}

// GetRoots trả về slice chứa EventID của tất cả các root trong một frame cụ thể.
// Trả về bản sao để tránh sửa đổi dữ liệu nội bộ.
func (ds *DagStore) GetRoots(frame uint64) []EventID {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	roots, exists := ds.rootsByFrame[frame]
	if !exists || len(roots) == 0 {
		return []EventID{} // Trả về slice rỗng nếu frame không tồn tại hoặc không có root
	}
	// Tạo bản sao để tránh sửa đổi map bên trong từ bên ngoài
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
func (ds *DagStore) getStake(creatorPubKeyHex string) uint64 {
	ds.mu.RLock() // Khóa đọc
	defer ds.mu.RUnlock()
	stake, exists := ds.stake[creatorPubKeyHex]
	if !exists {
		return 0 // Validator không có stake hoặc không tồn tại
	}
	return stake
}

// isAncestor kiểm tra xem event ancestorID có phải là tổ tiên của event descendantID không.
// Sử dụng BFS để duyệt ngược từ descendantID.
func (ds *DagStore) isAncestor(ancestorID, descendantID EventID) bool {
	ds.mu.RLock() // Khóa đọc khi truy cập ds.events
	defer ds.mu.RUnlock()

	if ancestorID == descendantID { // Một event là tổ tiên của chính nó
		return true
	}
	if ancestorID.IsZero() { // Zero ID không thể là tổ tiên (trừ trường hợp đặc biệt)
		return false
	}
	if descendantID.IsZero() { // Nếu descendant là zero, không thể có tổ tiên khác zero
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
			// log.Printf("Warning: Event %s not found in store during isAncestor check for descendant %s, ancestor %s",
			//	currentID.String()[:6], descendantID.String()[:6], ancestorID.String()[:6])
			continue // Event không tồn tại trong store
		}

		// Lấy tất cả parents (self-parent và other-parents)
		var parents []EventID
		if !currentEvent.EventData.SelfParent.IsZero() {
			parents = append(parents, currentEvent.EventData.SelfParent)
		}
		if len(currentEvent.EventData.OtherParents) > 0 {
			parents = append(parents, currentEvent.EventData.OtherParents...)
		}

		for _, parentID := range parents {
			if parentID.IsZero() { // Bỏ qua zero parent
				continue
			}
			if parentID == ancestorID {
				return true // Tìm thấy tổ tiên
			}
			if !visited[parentID] {
				visited[parentID] = true
				queue = append(queue, parentID)
			}
		}
	}
	return false // Không tìm thấy tổ tiên
}

// forklessCause là hàm kiểm tra xem event x có "forkless cause" event y không.
// Đây là hàm giả định, chỉ kiểm tra tổ tiên đơn giản.
// Logic forklessCause thực tế phức tạp hơn nhiều, cần phát hiện và xử lý fork.
func (ds *DagStore) forklessCause(x, y *Event) bool {
	if x == nil || y == nil {
		return false
	}
	return ds.isAncestor(x.GetEventId(), y.GetEventId())
}

// DecideClotho thực hiện thuật toán lựa chọn Clotho.
// Nó duyệt qua các Root và tính toán phiếu bầu để quyết định trạng thái Clotho.
func (ds *DagStore) DecideClotho() {
	ds.mu.Lock() // Khóa ghi cho toàn bộ quá trình quyết định
	defer ds.mu.Unlock()

	startFrame := ds.lastDecidedFrame + 1
	var maxFrame uint64 = 0 // Khởi tạo maxFrame
	// Tìm frame cao nhất hiện có các roots để biết giới hạn duyệt
	for frame := range ds.rootsByFrame {
		if frame > maxFrame {
			maxFrame = frame
		}
	}

	if startFrame > maxFrame {
		// log.Println("No new frames to process for Clotho selection.")
		return
	}
	// log.Printf("Starting Clotho Selection from Frame %d up to Frame %d", startFrame, maxFrame)

	// Duyệt qua các Root (x) từ frame sau frame cuối cùng đã quyết định
	for xFrame := startFrame; xFrame <= maxFrame; xFrame++ {
		xRootsIDsInCurrentXFrame := ds.rootsByFrame[xFrame] // Lấy danh sách ID gốc
		if len(xRootsIDsInCurrentXFrame) == 0 {
			continue // Không có roots trong frame này
		}

		// Lấy các event object cho các root x chưa được quyết định trong frame này
		xEventsToProcess := make([]*Event, 0, len(xRootsIDsInCurrentXFrame))
		for _, xID := range xRootsIDsInCurrentXFrame {
			xEvent, exists := ds.events[xID]
			// Chỉ xem xét các root thực sự (IsRoot=true) và chưa được quyết định (ClothoStatus=UNDECIDED)
			if exists && xEvent.EventData.IsRoot && xEvent.ClothoStatus == ClothoUndecided {
				xEventsToProcess = append(xEventsToProcess, xEvent)
			}
		}

		if len(xEventsToProcess) == 0 { // Không có root nào cần xử lý trong frame này
			// Kiểm tra xem có nên cập nhật lastDecidedFrame không nếu tất cả root đã decided trước đó
			// (logic này nằm ở cuối vòng lặp xFrame)
			// log.Printf("No undecided roots to process in frame %d.", xFrame)
			// continue // Chuyển sang frame tiếp theo (nhưng logic cập nhật lastDecidedFrame ở dưới sẽ xử lý)
		}

	next_x_event_processing_loop: // Label để nhảy sang root x tiếp theo trong cùng xFrame
		for _, xEvent := range xEventsToProcess {
			xID := xEvent.GetEventId()

			// Vòng lặp qua các frame y (y.frame > x.frame) để các root y bỏ phiếu cho x
			for yFrame := xFrame + 1; yFrame <= maxFrame; yFrame++ {
				yRootsIDsInCurrentYFrame := ds.rootsByFrame[yFrame]
				if len(yRootsIDsInCurrentYFrame) == 0 {
					continue // Không có root nào trong frame y này
				}

				// Duyệt qua từng root y trong frame yFrame
				for _, yID := range yRootsIDsInCurrentYFrame {
					yEvent, yExists := ds.events[yID]
					if !yExists || !yEvent.EventData.IsRoot {
						continue // Bỏ qua nếu y không phải root hoặc không tồn tại
					}

					round := yEvent.EventData.Frame - xEvent.EventData.Frame // = yFrame - xFrame

					if round == 1 { // Round 1: y bỏ phiếu trực tiếp cho x
						vote := ds.forklessCause(xEvent, yEvent)
						yEvent.SetVote(xID, vote) // y bỏ phiếu cho x
						// log.Printf("  R1: Root %s (F%d) votes %t for Root %s (F%d)", yEvent.GetEventId().String()[:6], yFrame, vote, xID.String()[:6], xFrame)
					} else if round >= 2 { // Round >= 2: y tổng hợp phiếu bầu từ frame y-1
						prevVotersFrame := yFrame - 1
						prevVotersIDs := ds.GetRoots(prevVotersFrame) // Lấy roots ở frame y-1

						yesVotesStake := uint64(0)
						noVotesStake := uint64(0)

						for _, prevRootID := range prevVotersIDs {
							prevRootEvent, prevRootExists := ds.events[prevRootID]
							if !prevRootExists || !prevRootEvent.EventData.IsRoot {
								continue // Bỏ qua nếu prevRoot không hợp lệ
							}

							// Chỉ xem xét prevRoot nào forklessCause y
							if ds.forklessCause(prevRootEvent, yEvent) {
								prevVote, voteExists := prevRootEvent.GetVote(xID)
								if voteExists { // Chỉ tính nếu prevRoot đã bỏ phiếu cho x
									prevVoterStake := ds.getStake(hex.EncodeToString(prevRootEvent.EventData.Creator))
									if prevVote { // prevVote là true (YES)
										yesVotesStake += prevVoterStake
									} else { // prevVote là false (NO)
										noVotesStake += prevVoterStake
									}
								}
							}
						}

						// y bỏ phiếu giống đa số (theo stake) từ các prevRoot đã xem xét
						yFinalVote := (yesVotesStake >= noVotesStake)
						yEvent.SetVote(xID, yFinalVote) // y bỏ phiếu cho x
						// log.Printf("  R%d: Root %s (F%d) votes %t for %s (F%d) (YesS: %d, NoS: %d)",
						//	round, yEvent.GetEventId().String()[:6], yFrame, yFinalVote, xID.String()[:6], xFrame, yesVotesStake, noVotesStake)

						// Kiểm tra điều kiện quyết định Clotho cho x dựa trên phiếu bầu TỪ CÁC PREVROOTS (yesVotesStake, noVotesStake)
						if yesVotesStake >= QUORUM {
							xEvent.SetCandidate(true)
							xEvent.SetClothoStatus(ClothoIsClotho)
							// log.Printf("DECIDED: Root %s (F%d) is CLOTHO. Decided by votes aggregated by %s (F%d). YesStake: %d >= Quorum: %d",
							//	xID.String()[:6], xFrame, yEvent.GetEventId().String()[:6], yFrame, yesVotesStake, QUORUM)
							continue next_x_event_processing_loop // Đã quyết định cho xEvent này, chuyển sang xEvent tiếp theo
						}
						if noVotesStake >= QUORUM {
							xEvent.SetCandidate(false)
							xEvent.SetClothoStatus(ClothoIsNotClotho)
							// log.Printf("DECIDED: Root %s (F%d) is NOT-CLOTHO. Decided by votes aggregated by %s (F%d). NoStake: %d >= Quorum: %d",
							//	xID.String()[:6], xFrame, yEvent.GetEventId().String()[:6], yFrame, noVotesStake, QUORUM)
							continue next_x_event_processing_loop // Đã quyết định cho xEvent này, chuyển sang xEvent tiếp theo
						}
					}
				} // Kết thúc vòng lặp qua các root y trong yFrame
			} // Kết thúc vòng lặp qua các yFrame cho một xEvent

			// Nếu sau khi duyệt hết các yFrame mà xEvent vẫn chưa được quyết định:
			// if xEvent.ClothoStatus == ClothoUndecided {
			// log.Printf("Root %s (F%d) remains UNDECIDED after checking all y-frames up to %d", xID.String()[:6], xFrame, maxFrame)
			// }
		} // Kết thúc vòng lặp qua các xEvent trong xEventsToProcess (next_x_event_processing_loop)

		// Sau khi xử lý tất cả các root x ứng cử viên trong xFrame,
		// kiểm tra xem toàn bộ xFrame đã được quyết định chưa để cập nhật lastDecidedFrame.
		allRootsInXFrameDecided := true
		if len(xRootsIDsInCurrentXFrame) > 0 { // Chỉ kiểm tra nếu frame này có root
			for _, rootID := range xRootsIDsInCurrentXFrame {
				rootEvent, exists := ds.events[rootID]
				// Một root được coi là "không quyết định" nếu nó không tồn tại, không phải root, hoặc trạng thái là UNDECIDED
				if !exists || !rootEvent.EventData.IsRoot || rootEvent.ClothoStatus == ClothoUndecided {
					allRootsInXFrameDecided = false
					break
				}
			}
		} else { // Frame rỗng, không có root để quyết định
			allRootsInXFrameDecided = false // Coi như frame rỗng chưa "hoàn thành" để cập nhật lastDecidedFrame
			// Hoặc có thể đặt là true nếu bạn muốn lastDecidedFrame tiến qua frame rỗng.
			// Để an toàn, đặt false để tránh lastDecidedFrame nhảy cóc.
		}

		if allRootsInXFrameDecided {
			ds.lastDecidedFrame = xFrame
			// log.Printf("Updated lastDecidedFrame to %d as all roots in Frame %d are decided.", ds.lastDecidedFrame, xFrame)
		} else if len(xRootsIDsInCurrentXFrame) > 0 { // Có root trong frame nhưng chưa quyết định hết
			// log.Printf("Frame %d not fully decided yet. lastDecidedFrame remains %d.", xFrame, ds.lastDecidedFrame)
			// Nếu một frame chưa được quyết định hoàn toàn, dừng quá trình Clotho cho các frame sau đó
			// vì quyết định của frame sau phụ thuộc vào frame trước.
			// log.Println("Stopping Clotho selection for subsequent frames as current frame is not fully decided.")
			break // Thoát khỏi vòng lặp duyệt các xFrame
		}
		// Nếu frame rỗng (len(xRootsIDsInCurrentXFrame) == 0), vòng lặp xFrame sẽ tiếp tục.
	} // Kết thúc vòng lặp qua các xFrame
	// log.Println("Clotho Selection process finished.")
}

// GetDecidedRoots trả về một map các EventID tới *Event cho các root đã có quyết định Clotho.
func (ds *DagStore) GetDecidedRoots() map[EventID]*Event {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	decided := make(map[EventID]*Event)
	// Duyệt qua tất cả các event, kiểm tra nếu là root và đã quyết định
	for id, event := range ds.events {
		if event.EventData.IsRoot && event.ClothoStatus != ClothoUndecided {
			decided[id] = event // Sử dụng EventID làm key
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
		return ClothoUndecided, false // Không tồn tại hoặc không phải là root
	}
	return event.ClothoStatus, true
}

// *** CÁC PHƯƠNG THỨC MỚI ĐỂ LẤY HEIGHT VÀ IN-DEGREE ***

// GetHeightForNode trả về chỉ số (index) của event mới nhất được tạo bởi node có NodeID đã cho.
// nodeCreatorPubKeyHex là chuỗi hex public key của creator.
// Trả về height và một boolean cho biết node có tồn tại và có event hay không.
func (ds *DagStore) GetHeightForNode(nodeCreatorPubKeyHex string) (uint64, bool) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	latestEventID, exists := ds.latestEvents[nodeCreatorPubKeyHex]
	if !exists {
		return 0, false // Node chưa tạo event nào hoặc không tồn tại
	}

	event, eventExists := ds.events[latestEventID]
	if !eventExists {
		// Điều này không nên xảy ra nếu latestEvents được quản lý đúng cách
		// log.Printf("Error: Latest event %s for creator %s not found in events map.", latestEventID.String(), nodeCreatorPubKeyHex)
		return 0, false
	}
	return event.EventData.Index, true
}

// GetInDegreeForNode trả về "in-degree" cho event mới nhất của node có NodeID đã cho.
// In-degree được định nghĩa là số lượng creator *khác nhau* của các OtherParents
// trong event mới nhất (top event block) của node đó.
// nodeCreatorPubKeyHex là chuỗi hex public key của creator.
// Trả về in-degree và một boolean cho biết node có tồn tại và có event hay không.
func (ds *DagStore) GetInDegreeForNode(nodeCreatorPubKeyHex string) (uint64, bool) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	latestEventID, exists := ds.latestEvents[nodeCreatorPubKeyHex]
	if !exists {
		return 0, false // Node chưa tạo event nào hoặc không tồn tại
	}

	event, eventExists := ds.events[latestEventID]
	if !eventExists {
		// log.Printf("Error: Latest event %s for creator %s not found in events map for in-degree.", latestEventID.String(), nodeCreatorPubKeyHex)
		return 0, false
	}

	if len(event.EventData.OtherParents) == 0 {
		return 0, true // Không có other parents, in-degree là 0
	}

	// Sử dụng map để đếm số lượng creator khác nhau của other parents
	distinctCreators := make(map[string]struct{}) // Key là chuỗi hex public key của creator của parent

	for _, parentEventID := range event.EventData.OtherParents {
		if parentEventID.IsZero() {
			continue // Bỏ qua zero parent ID nếu có
		}
		parentEvent, parentExists := ds.events[parentEventID]
		if !parentExists {
			// Quan trọng: Xử lý trường hợp parent event không (chưa) có trong store.
			// log.Printf("Warning: Parent event %s not found in DagStore for in-degree calculation of node %s's event %s", parentEventID.String(), nodeCreatorPubKeyHex, latestEventID.String())
			continue // Bỏ qua parent này nếu không tìm thấy
		}
		// Lấy creator của parent event và chuyển sang chuỗi hex
		parentCreatorHex := hex.EncodeToString(parentEvent.EventData.Creator)
		distinctCreators[parentCreatorHex] = struct{}{}
	}
	return uint64(len(distinctCreators)), true
}
