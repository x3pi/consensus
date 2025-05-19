// package dag

package dag

import (
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
)

// QUORUM là ngưỡng bỏ phiếu cần thiết để quyết định trạng thái Clotho.
// Trong thực tế, QUORUM = 2/3 tổng Stake + 1.
// Ở đây dùng giá trị cố định cho ví dụ.
const QUORUM uint64 = 67 // Ví dụ: giả sử tổng stake là 100, quorum là 67

// DagStore là cấu trúc quản lý các Event Block trong DAG và logic Clotho Selection.
// Đây là implementation đơn giản trong bộ nhớ.
type DagStore struct {
	events           map[EventID]*Event   // Lưu trữ tất cả các event theo EventID (hash)
	latestEvents     map[string]EventID   // Lưu trữ EventID mới nhất cho mỗi creator (public key hex string -> EventID)
	rootsByFrame     map[uint64][]EventID // Lưu trữ EventID của các Root, nhóm theo Frame
	lastDecidedFrame uint64               // Frame cuối cùng mà Clotho đã được quyết định
	mu               sync.RWMutex         // Mutex để đồng bộ hóa truy cập
	stake            map[string]uint64    // Giả định map stake của mỗi validator (public key hex string -> stake)
}

// NewDagStore tạo một instance mới của DagStore.
func NewDagStore(initialStake map[string]uint64) *DagStore {
	// Chuyển key từ []byte sang string hex cho map stake
	stakeMap := make(map[string]uint64)
	for pubKeyBytes, s := range initialStake {
		stakeMap[hex.EncodeToString([]byte(pubKeyBytes))] = s
	}

	return &DagStore{
		events:           make(map[EventID]*Event),
		latestEvents:     make(map[string]EventID),
		rootsByFrame:     make(map[uint64][]EventID),
		lastDecidedFrame: 0, // Bắt đầu từ frame 0 hoặc 1 tùy quy ước
		mu:               sync.RWMutex{},
		stake:            stakeMap, // Lưu trữ stake
	}
}

// AddEvent thêm một event mới vào DagStore.
// Hàm này thực hiện các kiểm tra cơ bản, cập nhật latest event và thêm vào rootsByFrame nếu là root.
func (ds *DagStore) AddEvent(event *Event) error {
	if event == nil {
		return errors.New("cannot add nil event")
	}

	// Tính toán EventID (hash) nếu chưa có trong cache
	eventID := event.GetEventId()
	if eventID.IsZero() {
		// Điều này chỉ xảy ra nếu PrepareForHashing hoặc Borsh Serialize thất bại trong Event.Hash()
		computedID, err := event.Hash()
		if err != nil {
			return fmt.Errorf("failed to calculate event hash: %w", err)
		}
		eventID = computedID
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
			return errors.New("event has zero self parent but creator has previous events")
		}
		if event.EventData.SelfParent != currentLatestEventID {
			return fmt.Errorf("invalid self parent: expected %s, got %s", currentLatestEventID.String(), event.EventData.SelfParent.String())
		}
	} else {
		if !event.EventData.SelfParent.IsZero() {
			// Event đầu tiên của creator phải có SelfParent là Zero, trừ khi là event khởi tạo đặc biệt
			// Tùy thuộc vào quy ước của frame 0 hoặc các event khởi tạo
			// Ở đây giả định event đầu tiên luôn có SelfParent zero
			return errors.New("first event of creator must have zero self parent")
		}
		// Kiểm tra xem creator có stake không
		if _, exists := ds.stake[creatorKey]; !exists {
			return fmt.Errorf("creator %s has no registered stake", creatorKey)
		}
	}

	// 3. Kiểm tra xem OtherParents có tồn tại trong store không
	// LƯU Ý: Đây chỉ là kiểm tra sự tồn tại ID. Logic thực tế cần kiểm tra tính hợp lệ của parent.
	for _, otherParentID := range event.EventData.OtherParents {
		if otherParentID.IsZero() {
			continue // Bỏ qua zero ID
		}
		if _, exists := ds.events[otherParentID]; !exists {
			return fmt.Errorf("other parent with ID %s does not exist", otherParentID.String())
		}
	}

	// 4. Lưu event vào store
	ds.events[eventID] = event

	// 5. Cập nhật latest event cho creator này
	ds.latestEvents[creatorKey] = eventID

	// 6. Nếu event là Root, thêm vào danh sách rootsByFrame
	if event.EventData.IsRoot {
		ds.rootsByFrame[event.EventData.Frame] = append(ds.rootsByFrame[event.EventData.Frame], eventID)
		// Sắp xếp roots trong cùng frame để đảm bảo tính xác định (tùy chọn, có thể sắp xếp theo hash)
		// sort.SliceStable(ds.rootsByFrame[event.EventData.Frame], func(i, j int) bool {
		// 	return bytes.Compare(ds.rootsByFrame[event.EventData.Frame][i].Bytes(), ds.rootsByFrame[event.EventData.Frame][j].Bytes()) < 0
		// })
	}

	fmt.Printf("Successfully added event: %s (Creator: %s, Frame: %d, IsRoot: %t)\n", eventID.String(), creatorKey, event.EventData.Frame, event.EventData.IsRoot)

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

// GetLatestEventID trả về EventID mới nhất được tạo bởi một creator cụ thể.
func (ds *DagStore) GetLatestEventID(creatorID []byte) (EventID, bool) {
	ds.mu.RLock() // Khóa đọc
	defer ds.mu.RUnlock()

	creatorKey := hex.EncodeToString(creatorID)
	eventID, exists := ds.latestEvents[creatorKey]
	return eventID, exists
}

// GetRoots trả về slice chứa EventID của tất cả các root trong một frame cụ thể.
// Trả về bản sao để tránh sửa đổi dữ liệu nội bộ.
func (ds *DagStore) GetRoots(frame uint64) []EventID {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	roots, exists := ds.rootsByFrame[frame]
	if !exists {
		return []EventID{} // Trả về slice rỗng nếu frame không tồn tại
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

// getStake lấy stake của một validator dựa trên public key byte.
// Đây là hàm giả định, cần thay thế bằng logic quản lý stake thực tế.
func (ds *DagStore) getStake(creatorID []byte) uint64 {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	creatorKey := hex.EncodeToString(creatorID)
	stake, exists := ds.stake[creatorKey]
	if !exists {
		return 0 // Validator không có stake hoặc không tồn tại
	}
	return stake
}

// isAncestor kiểm tra xem event ancestorID có phải là tổ tiên của event descendantID không.
// Đây là hàm đơn giản hóa, không kiểm tra "forkless cause".
// Việc kiểm tra "forkless cause" phức tạp hơn, cần xem xét các fork.
func (ds *DagStore) isAncestor(ancestorID, descendantID EventID) bool {
	// Sử dụng BFS hoặc DFS để duyệt ngược từ descendantID
	// Để đơn giản, đây là BFS
	ds.mu.RLock() // Khóa đọc khi truy cập events map
	defer ds.mu.RUnlock()

	if ancestorID == descendantID {
		return true
	}
	if ancestorID.IsZero() {
		return false // Zero ID không thể là tổ tiên (trừ trường hợp đặc biệt)
	}

	queue := []EventID{descendantID}
	visited := make(map[EventID]bool)
	visited[descendantID] = true

	for len(queue) > 0 {
		currentID := queue[0]
		queue = queue[1:]

		currentEvent, exists := ds.events[currentID]
		if !exists {
			continue // Event không tồn tại trong store
		}

		// Lấy tất cả parents (self-parent và other-parents)
		parents := []EventID{}
		if !currentEvent.EventData.SelfParent.IsZero() {
			parents = append(parents, currentEvent.EventData.SelfParent)
		}
		parents = append(parents, currentEvent.EventData.OtherParents...)

		for _, parentID := range parents {
			if parentID.IsZero() {
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
// Đây là hàm giả định, thay thế logic phức tạp của Lachesis.
// Trong thực tế, cần kiểm tra đường đi từ x đến y và đảm bảo không có fork liên quan.
func (ds *DagStore) forklessCause(x, y *Event) bool {
	if x == nil || y == nil {
		return false
	}
	// Kiểm tra xem x có phải là tổ tiên của y không
	// LƯU Ý: Đây chỉ là kiểm tra tổ tiên đơn giản.
	// Logic forklessCause thực tế phức tạp hơn nhiều, cần phát hiện và xử lý fork.
	return ds.isAncestor(x.GetEventId(), y.GetEventId())
}

// DecideClotho thực hiện thuật toán lựa chọn Clotho.
// Nó duyệt qua các Root và tính toán phiếu bầu để quyết định trạng thái Clotho.
// Thuật toán dựa trên pseudo code Algorithm 1.
func (ds *DagStore) DecideClotho() {
	ds.mu.Lock() // Khóa ghi cho toàn bộ quá trình quyết định
	defer ds.mu.Unlock()

	// Bắt đầu từ frame sau frame cuối cùng đã quyết định
	startFrame := ds.lastDecidedFrame + 1

	// Tìm frame cao nhất hiện có các roots để biết giới hạn duyệt
	var maxFrame uint64
	for frame := range ds.rootsByFrame {
		if frame > maxFrame {
			maxFrame = frame
		}
	}

	fmt.Printf("Starting Clotho Selection from Frame %d up to Frame %d\n", startFrame, maxFrame)

	// Duyệt qua các Root (x) từ frame sau frame cuối cùng đã quyết định
	// Vòng lặp ngoài: for x range roots: lastDecidedFrame + 1 to up
	for xFrame := startFrame; xFrame <= maxFrame; xFrame++ {
		xRootsIDs := ds.rootsByFrame[xFrame]
		if len(xRootsIDs) == 0 {
			continue // Không có roots trong frame này
		}

		// Lấy các event cho các root x trong frame này
		xEvents := make([]*Event, 0, len(xRootsIDs))
		for _, xID := range xRootsIDs {
			xEvent, exists := ds.events[xID]
			if exists && xEvent.EventData.IsRoot && xEvent.ClothoStatus == ClothoUndecided {
				xEvents = append(xEvents, xEvent)
			}
		}

		// Duyệt qua từng root x chưa được quyết định trong frame xFrame
		for _, xEvent := range xEvents {
			xID := xEvent.GetEventId()
			// Sử dụng goto để thoát khỏi vòng lặp duyệt y khi root x được quyết định
			// Label cho goto
		next_y_for_x:

			// Vòng lặp trong: for y range roots: x.frame + 1 to up
			// Duyệt qua các Root (y) ở các frame sau frame của x
			for yFrame := xFrame + 1; yFrame <= maxFrame; yFrame++ {
				yRootsIDs := ds.rootsByFrame[yFrame]
				if len(yRootsIDs) == 0 {
					continue // Không có roots trong frame này
				}

				// Lấy các event cho các root y trong frame này
				yEvents := make([]*Event, 0, len(yRootsIDs))
				for _, yID := range yRootsIDs {
					yEvent, exists := ds.events[yID]
					// Chỉ xem xét các root y tồn tại và chưa được quyết định (trạng thái của y không ảnh hưởng đến việc y bỏ phiếu)
					if exists && yEvent.EventData.IsRoot {
						yEvents = append(yEvents, yEvent)
					}
				}

				for _, yEvent := range yEvents {
					yID := yEvent.GetEventId()

					// y là root bỏ phiếu, x là root được bỏ phiếu
					round := yEvent.EventData.Frame - xEvent.EventData.Frame

					// Thuật toán gốc chỉ định bỏ phiếu ở round 1 và sau đó tổng hợp ở round >= 2
					// Chúng ta sẽ theo logic đó.

					if round == 1 {
						// Round 1: y bỏ phiếu trực tiếp cho x
						// y.vote[x] ← forklessCause(x, y)
						// Lưu ý: Sử dụng isAncestor thay cho forklessCause phức tạp
						vote := ds.forklessCause(xEvent, yEvent) // Sử dụng hàm giả định
						yEvent.SetVote(xID, vote)
						// fmt.Printf("  Root %s (Frame %d) votes %t for Root %s (Frame %d) in Round 1\n", yID.String(), yFrame, vote, xID.String(), xFrame)

					} else if round >= 2 {
						// Round >= 2: y tổng hợp phiếu bầu từ các root ở frame y.frame - 1
						// prevVoters ← getRoots(y.frame-1)
						prevVotersFrame := yFrame - 1
						prevVotersIDs := ds.GetRoots(prevVotersFrame) // Lấy roots ở frame trước

						// Theo định nghĩa root, prevVoters chứa ít nhất QUORUM events.
						// Tuy nhiên, chỉ những prevRoot nào forklessCause y mới được xem xét.

						yesVotesStake := uint64(0)
						noVotesStake := uint64(0)

						// Duyệt qua các root ở frame trước (prevRoot)
						// for prevRoot range prevRoots do
						for _, prevRootID := range prevVotersIDs {
							prevRootEvent, exists := ds.events[prevRootID]
							if !exists {
								continue // Bỏ qua nếu prevRoot không tồn tại
							}

							// if forklessCause(prevRoot, y) == TRUE then
							// Chỉ xem xét prevRoot nào forklessCause y
							if ds.forklessCause(prevRootEvent, yEvent) { // Sử dụng hàm giả định
								// count number of positive and negative votes for x from roots which forkless cause y
								// if prevRoot.vote[x] == TRUE then
								// Kiểm tra phiếu bầu của prevRoot cho x
								prevVote, voteExists := prevRootEvent.GetVote(xID)

								if voteExists {
									// Lấy stake của creator của prevRoot
									prevVoterStake := ds.getStake(prevRootEvent.EventData.Creator)

									if prevVote == true {
										yesVotesStake += prevVoterStake
									} else {
										noVotesStake += prevVoterStake
									}
								}
								// Nếu voteExists là false, prevRoot chưa bỏ phiếu cho x ở round trước (round 1), bỏ qua
							}
						}

						// y votes likes a majority of roots from previous frame which forkless cause y (weighted by stake)
						// y.vote[x] ← (yesVotes - noVotes) >= 0
						// Nếu tổng stake YES >= tổng stake NO, y bỏ phiếu YES cho x
						yVote := (yesVotesStake >= noVotesStake)
						yEvent.SetVote(xID, yVote)
						// fmt.Printf("  Root %s (Frame %d) aggregates votes for Root %s (Frame %d) in Round %d. Result: %t (YES stake: %d, NO stake: %d)\n",
						// 	yID.String(), yFrame, xID.String(), xFrame, round, yVote, yesVotesStake, noVotesStake)

						// --- Kiểm tra điều kiện quyết định Clotho ---
						// Quyết định cho root x được thực hiện dựa trên phiếu bầu TỪ CÁC ROOT Ở FRAME TRƯỚC MÀ FORKLESS CAUSE Y.
						// Pseudo code kiểm tra quyết định ngay sau khi y bỏ phiếu.
						// Điều kiện quyết định: yesVotes (từ prevRoots forklessCause y) >= QUORUM HOẶC noVotes (từ prevRoots forklessCause y) >= QUORUM.

						// if yesVotes >= QUORUM then
						// Nếu tổng stake YES từ các root ở frame prevVotersFrame mà forklessCause y đạt QUORUM
						if yesVotesStake >= QUORUM {
							// x.candidate ← TRUE // decided as a candidate for Atropos
							xEvent.SetCandidate(true)
							xEvent.SetClothoStatus(ClothoIsClotho)
							fmt.Printf("Root %s (Frame %d) decided as IS-CLOTHO (Candidate for Atropos) by Root %s (Frame %d) in Round %d (YES stake: %d >= QUORUM: %d)\n",
								xID.String(), xFrame, yID.String(), yFrame, round, yesVotesStake, QUORUM)
							// break // Thoát vòng lặp duyệt y cho root x này
							goto next_y_for_x // Sử dụng goto để thoát khỏi vòng lặp duyệt y
						}

						// if noVotes >= QUORUM then
						// Nếu tổng stake NO từ các root ở frame prevVotersFrame mà forklessCause y đạt QUORUM
						if noVotesStake >= QUORUM {
							// x.candidate ← FALSE // decided as a non-candidate for Atropos
							xEvent.SetCandidate(false)
							xEvent.SetClothoStatus(ClothoIsNotClotho)
							fmt.Printf("Root %s (Frame %d) decided as IS-NOT-CLOTHO by Root %s (Frame %d) in Round %d (NO stake: %d >= QUORUM: %d)\n",
								xID.String(), xFrame, yID.String(), yFrame, round, noVotesStake, QUORUM)
							// break // Thoát vòng lặp duyệt y cho root x này
							goto next_y_for_x // Sử dụng goto để thoát khỏi vòng lặp duyệt y
						}
					}
				} // End for yEvent
			} // End for yFrame

			// Nếu root x chưa được quyết định sau khi duyệt hết các root y ở các frame sau, nó vẫn ở trạng thái Undecided.
			if xEvent.ClothoStatus == ClothoUndecided {
				fmt.Printf("Root %s (Frame %d) remains UNDECIDED after checking up to Frame %d\n", xID.String(), xFrame, maxFrame)
			}

		} // End for xEvent (looping through xEvents in xFrame)

		// Logic để cập nhật lastDecidedFrame ở đây.
		// Điều kiện thực tế để cập nhật lastDecidedFrame là khi MỘT FRAME ĐƯỢC FINALIZED,
		// không chỉ một root được quyết định.
		// Một frame được finalized khi tất cả các root trong frame đó đã được quyết định trạng thái Clotho.
		allRootsInFrameDecided := true
		currentFrameRoots := ds.GetRoots(xFrame) // Lấy lại danh sách roots (bản sao)
		if len(currentFrameRoots) > 0 {
			for _, rootID := range currentFrameRoots {
				rootEvent, exists := ds.events[rootID] // Lấy event gốc (không phải bản sao)
				if !exists || rootEvent.ClothoStatus == ClothoUndecided {
					allRootsInFrameDecided = false
					break
				}
			}
		} else {
			// Nếu không có root nào trong frame, coi như đã quyết định (không có gì để quyết định)
			// Tùy thuộc vào quy ước, frame rỗng có thể không cần xử lý.
			// Giả định frame có roots mới cần quyết định.
			allRootsInFrameDecided = false // Nếu không có roots, không thể quyết định frame này
		}

		if allRootsInFrameDecided {
			ds.lastDecidedFrame = xFrame
			fmt.Printf("Updated lastDecidedFrame to %d\n", ds.lastDecidedFrame)
		}

	} // End for xFrame

	fmt.Println("Clotho Selection process finished.")
}

// --- Hàm Helper để lấy Root đã quyết định (cho mục đích minh họa) ---
// Trong thực tế, bạn có thể cần một map riêng cho các root đã quyết định.
func (ds *DagStore) GetDecidedRoots() map[EventID]*Event {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	decided := make(map[EventID]*Event)
	for _, event := range ds.events {
		if event.EventData.IsRoot && event.ClothoStatus != ClothoUndecided {
			decided[event.GetEventId()] = event
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
		return ClothoUndecided, false // Không tồn tại hoặc không phải root
	}
	return event.ClothoStatus, true
}

// --- Ví dụ cách sử dụng (có thể đặt trong main.go hoặc file test) ---
/*
import (
	"crypto/ecdsa"
	"fmt"
	"time"
	"github.com/ethereum/go-ethereum/crypto"
	"encoding/hex"
	// Import package dag của bạn
	// Cần import package dag của bạn ở đây
)

// QUORUM cần được định nghĩa lại hoặc import nếu main.go ở package khác
const QUORUM uint64 = 67 // Ví dụ: giả sử tổng stake là 100, quorum là 67

func main() {
	// Khởi tạo stake giả cho vài validator
	// Stake là map từ public key hex string sang stake amount
	initialStake := map[string]uint64{
		hex.EncodeToString(crypto.CompressPubkey(&crypto.CreateFakeECCKey().PublicKey)): 20,
		hex.EncodeToString(crypto.CompressPubkey(&crypto.CreateFakeECCKey().PublicKey)): 30,
		hex.EncodeToString(crypto.CompressPubkey(&crypto.CreateFakeECCKey().PublicKey)): 25,
		hex.EncodeToString(crypto.CompressPubkey(&crypto.CreateFakeECCKey().PublicKey)): 35, // Tổng stake = 110 > QUORUM 67
	}

	store := NewDagStore(initialStake)

	// Tạo private/public keys cho các creator
	privKeys := make(map[string]*ecdsa.PrivateKey)
	creatorIDs := make(map[string][]byte)
	i := 0
	for hexKey := range initialStake {
		// Tạo key giả cho ví dụ
		priv, _ := crypto.GenerateKey() // Hoặc dùng crypto.CreateFakeECCKey()
		pub := crypto.CompressPubkey(&priv.PublicKey)
		privKeys[hex.EncodeToString(pub)] = priv
		creatorIDs[fmt.Sprintf("Creator%d", i)] = pub
		i++
	}

	// --- Mô phỏng tạo các Event và Root ---
	// Đây là mô phỏng đơn giản, việc xác định Frame và IsRoot thực tế phức tạp hơn.
	// Chúng ta tạo các root sao cho có thể mô phỏng việc bỏ phiếu.

	// Frame 1 Roots
	root1_c0_data := EventData{Transactions: [][]byte{[]byte("tx_c0_f1")}, SelfParent: EventID{}, OtherParents: []EventID{}, Creator: creatorIDs["Creator0"], Index: 0, Timestamp: time.Now().UnixNano(), Frame: 1, IsRoot: true}
	root1_c0 := NewEvent(root1_c0_data, nil) // Cần sign trong thực tế
	store.AddEvent(root1_c0)

	root1_c1_data := EventData{Transactions: [][]byte{[]byte("tx_c1_f1")}, SelfParent: EventID{}, OtherParents: []EventID{}, Creator: creatorIDs["Creator1"], Index: 0, Timestamp: time.Now().UnixNano(), Frame: 1, IsRoot: true}
	root1_c1 := NewEvent(root1_c1_data, nil)
	store.AddEvent(root1_c1)

	root1_c2_data := EventData{Transactions: [][]byte{[]byte("tx_c2_f1")}, SelfParent: EventID{}, OtherParents: []EventID{}, Creator: creatorIDs["Creator2"], Index: 0, Timestamp: time.Now().UnixNano(), Frame: 1, IsRoot: true}
	root1_c2 := NewEvent(root1_c2_data, nil)
	store.AddEvent(root1_c2)

	root1_c3_data := EventData{Transactions: [][]byte{[]byte("tx_c3_f1")}, SelfParent: EventID{}, OtherParents: []EventID{}, Creator: creatorIDs["Creator3"], Index: 0, Timestamp: time.Now().UnixNano(), Frame: 1, IsRoot: true}
	root1_c3 := NewEvent(root1_c3_data, nil)
	store.AddEvent(root1_c3)


	// Frame 2 Roots (tham chiếu Frame 1 Roots)
	// Các root F2 sẽ bỏ phiếu (Round 1) cho các root F1
	root2_c0_data := EventData{Transactions: [][]byte{[]byte("tx_c0_f2")}, SelfParent: root1_c0.GetEventId(), OtherParents: []EventID{root1_c1.GetEventId(), root1_c2.GetEventId()}, Creator: creatorIDs["Creator0"], Index: 1, Timestamp: time.Now().UnixNano(), Frame: 2, IsRoot: true}
	root2_c0 := NewEvent(root2_c0_data, nil)
	store.AddEvent(root2_c0)

	root2_c1_data := EventData{Transactions: [][]byte{[]byte("tx_c1_f2")}, SelfParent: root1_c1.GetEventId(), OtherParents: []EventID{root1_c0.GetEventId(), root1_c3.GetEventId()}, Creator: creatorIDs["Creator1"], Index: 1, Timestamp: time.Now().UnixNano(), Frame: 2, IsRoot: true}
	root2_c1 := NewEvent(root2_c1_data, nil)
	store.AddEvent(root2_c1)

	root2_c2_data := EventData{Transactions: [][]byte{[]byte("tx_c2_f2")}, SelfParent: root1_c2.GetEventId(), OtherParents: []EventID{root1_c0.GetEventId(), root1_c3.GetEventId()}, Creator: creatorIDs["Creator2"], Index: 1, Timestamp: time.Now().UnixNano(), Frame: 2, IsRoot: true}
	root2_c2 := NewEvent(root2_c2_data, nil)
	store.AddEvent(root2_c2)

	root2_c3_data := EventData{Transactions: [][]byte{[]byte("tx_c3_f2")}, SelfParent: root1_c3.GetEventId(), OtherParents: []EventID{root1_c0.GetEventId(), root1_c1.GetEventId()}, Creator: creatorIDs["Creator3"], Index: 1, Timestamp: time.Now().UnixNano(), Frame: 2, IsRoot: true}
	root2_c3 := NewEvent(root2_c3_data, nil)
	store.AddEvent(root2_c3)


	// Frame 3 Roots (tham chiếu Frame 2 Roots)
	// Các root F3 sẽ tổng hợp phiếu bầu (Round 2) cho các root F1 từ các root F2 mà forklessCause chúng
	root3_c0_data := EventData{Transactions: [][]byte{[]byte("tx_c0_f3")}, SelfParent: root2_c0.GetEventId(), OtherParents: []EventID{root2_c1.GetEventId(), root2_c2.GetEventId()}, Creator: creatorIDs["Creator0"], Index: 2, Timestamp: time.Now().UnixNano(), Frame: 3, IsRoot: true}
	root3_c0 := NewEvent(root3_c0_data, nil)
	store.AddEvent(root3_c0)

	root3_c1_data := EventData{Transactions: [][]byte{[]byte("tx_c1_f3")}, SelfParent: root2_c1.GetEventId(), OtherParents: []EventID{root2_c0.GetEventId(), root2_c3.GetEventId()}, Creator: creatorIDs["Creator1"], Index: 2, Timestamp: time.Now().UnixNano(), Frame: 3, IsRoot: true}
	root3_c1 := NewEvent(root3_c1_data, nil)
	store.AddEvent(root3_c1)

	root3_c2_data := EventData{Transactions: [][]byte{[]byte("tx_c2_f3")}, SelfParent: root2_c2.GetEventId(), OtherParents: []EventID{root2_c0.GetEventId(), root2_c3.GetEventId()}, Creator: creatorIDs["Creator2"], Index: 2, Timestamp: time.Now().UnixNano(), Frame: 3, IsRoot: true}
	root3_c2 := NewEvent(root3_c2_data, nil)
	store.AddEvent(root3_c2)

	root3_c3_data := EventData{Transactions: [][]byte{[]byte("tx_c3_f3")}, SelfParent: root2_c3.GetEventId(), OtherParents: []EventID{root2_c0.GetEventId(), root2_c1.GetEventId()}, Creator: creatorIDs["Creator3"], Index: 2, Timestamp: time.Now().UnixNano(), Frame: 3, IsRoot: true}
	root3_c3 := NewEvent(root3_c3_data, nil)
	store.AddEvent(root3_c3)


	// Frame 4 Roots (tham chiếu Frame 3 Roots)
	// Các root F4 sẽ tổng hợp phiếu bầu (Round 3) cho các root F1 từ các root F3 mà forklessCause chúng
	// Quyết định Clotho cho root F1 có thể xảy ra ở Round 3.
	root4_c0_data := EventData{Transactions: [][]byte{[]byte("tx_c0_f4")}, SelfParent: root3_c0.GetEventId(), OtherParents: []EventID{root3_c1.GetEventId(), root3_c2.GetEventId()}, Creator: creatorIDs["Creator0"], Index: 3, Timestamp: time.Now().UnixNano(), Frame: 4, IsRoot: true}
	root4_c0 := NewEvent(root4_c0_data, nil)
	store.AddEvent(root4_c0)

	root4_c1_data := EventData{Transactions: [][]byte{[]byte("tx_c1_f4")}, SelfParent: root3_c1.GetEventId(), OtherParents: []EventID{root3_c0.GetEventId(), root3_c3.GetEventId()}, Creator: creatorIDs["Creator1"], Index: 3, Timestamp: time.Now().UnixNano(), Frame: 4, IsRoot: true}
	root4_c1 := NewEvent(root4_c1_data, nil)
	store.AddEvent(root4_c1)

	root4_c2_data := EventData{Transactions: [][]byte{[]byte("tx_c2_f4")}, SelfParent: root3_c2.GetEventId(), OtherParents: []EventID{root3_c0.GetEventId(), root3_c3.GetEventId()}, Creator: creatorIDs["Creator2"], Index: 3, Timestamp: time.Now().UnixNano(), Frame: 4, IsRoot: true}
	root4_c2 := NewEvent(root4_c2_data, nil)
	store.AddEvent(root4_c2)

	root4_c3_data := EventData{Transactions: [][]byte{[]byte("tx_c3_f4")}, SelfParent: root3_c3.GetEventId(), OtherParents: []EventID{root3_c0.GetEventId(), root3_c1.GetEventId()}, Creator: creatorIDs["Creator3"], Index: 3, Timestamp: time.Now().UnixNano(), Frame: 4, IsRoot: true}
	root4_c3 := NewEvent(root4_c3_data, nil)
	store.AddEvent(root4_c3)


	fmt.Println("\n--- Starting Clotho Selection ---")
	store.DecideClotho()
	fmt.Println("--- Clotho Selection Finished ---")

	fmt.Println("\nDecided Roots:")
	decidedRoots := store.GetDecidedRoots()
	for id, event := range decidedRoots {
		fmt.Printf("Root ID: %s, Frame: %d, Creator: %s, Status: %s, Candidate: %t\n",
			id.String(), event.EventData.Frame, hex.EncodeToString(event.EventData.Creator), event.ClothoStatus, event.Candidate)
	}

	// Kiểm tra trạng thái của một root cụ thể sau quyết định
	root1c0_after, exists := store.GetEvent(root1_c0.GetEventId())
	if exists {
		fmt.Printf("\nStatus of Root %s (Frame %d): %s, Candidate: %t\n",
			root1c0_after.GetEventId().String(), root1c0_after.EventData.Frame, root1c0_after.ClothoStatus, root1c0_after.Candidate)
	}
}
*/
