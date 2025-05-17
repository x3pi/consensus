// package dag

package dag

import (
	"bytes" // Cần cho ví dụ sử dụng key
	"fmt"
	"sort"
	"sync"
	"sync/atomic" // Import package atomic

	// Cần cho ví dụ sử dụng timestamp
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	borsh "github.com/near/borsh-go"
)

//-----------------------------------------------------------------------------
// EventID Definition
//-----------------------------------------------------------------------------

// EventID là kiểu dữ liệu cho hash của Event. EventID chính là hash của EventData.
type EventID common.Hash

// Bytes trả về biểu diễn byte của EventID
func (id EventID) Bytes() []byte {
	return common.Hash(id).Bytes()
}

// String trả về biểu diễn hex của EventID
func (id EventID) String() string {
	return common.Hash(id).Hex()
}

// IsZero kiểm tra xem EventID có phải là zero hash không
func (id EventID) IsZero() bool {
	return common.Hash(id) == common.Hash{}
}

//-----------------------------------------------------------------------------
// Clotho Status Definition
//-----------------------------------------------------------------------------

// ClothoStatus định nghĩa trạng thái lựa chọn Clotho của một Root.
type ClothoStatus string

const (
	ClothoUndecided   ClothoStatus = "UNDECIDED"     // Chưa được quyết định
	ClothoIsClotho    ClothoStatus = "IS-CLOTHO"     // Đã quyết định là Clotho (ứng viên Atropos)
	ClothoIsNotClotho ClothoStatus = "IS-NOT-CLOTHO" // Đã quyết định không phải là Clotho
)

//-----------------------------------------------------------------------------
// EventData Definition
//-----------------------------------------------------------------------------

// EventData là cấu trúc chứa dữ liệu cốt lõi của Event được dùng để tính toán hash (EventID).
// Các trường trong cấu trúc này sẽ được tuần tự hóa bằng Borsh để hashing.
// Cấu trúc này thể hiện Event có một self-parent và nhiều other-parents.
type EventData struct {
	Transactions [][]byte  `borsh:"slice"`     // Danh sách các giao dịch hoặc dữ liệu khác (byte slice)
	SelfParent   EventID   `borsh:"[32]uint8"` // Hash của parent cùng validator (size 32 bytes) - Tham chiếu bản thân
	OtherParents []EventID `borsh:"slice"`     // Hash của các parent khác validator (slice các size 32 bytes) - k-1 Tham chiếu nút ngang hàng
	Creator      []byte    `borsh:"slice"`     // Public key của người tạo event (byte slice)
	Index        uint64    `borsh:"uint64"`    // Sequence number của event do validator này tạo (tăng dần)
	Timestamp    int64     `borsh:"int64"`     // Unix timestamp (thời gian tạo event)
	Frame        uint64    `borsh:"uint64"`    // Frame number của event này. Được tính toán trong quá trình xác định frame.
	IsRoot       bool      `borsh:"bool"`      // Cờ đánh dấu event này có phải là Root của frame không. Được xác định trong quá trình xác định frame.
	// LamportTimestamp uint64    `borsh:"uint64"` // Lamport Timestamp có thể được thêm vào đây và tính toán trước khi hashing
	// Bạn có thể thêm các trường dữ liệu khác cần thiết cho Event tại đây
}

//-----------------------------------------------------------------------------
// Event Definition
//-----------------------------------------------------------------------------

// Event là cấu trúc hoàn chỉnh của một Event trong DAG.
// Nó bao gồm dữ liệu EventData, chữ ký và cache hash.
// Các trường liên quan đến Clotho Selection được thêm vào đây.
type Event struct {
	EventData // Nhúng EventData - dữ liệu dùng để hash

	Signature []byte `borsh:"slice"` // Chữ ký của người tạo trên hash của EventData

	// Cache cho hash of EventData (EventID) sử dụng atomic.Pointer để an toàn cho đồng thời.
	// Trường này không được tuần tự hóa bằng Borsh do tag `borsh:"skip"`.
	cachedHash atomic.Pointer[common.Hash] `borsh:"skip"`

	// --- Trường liên quan đến Clotho Selection ---
	// Vote lưu trữ kết quả bỏ phiếu của event này cho các Root khác.
	// Key: EventID của Root được bỏ phiếu (voting subject root)
	// Value: Kết quả bỏ phiếu (true = YES, false = NO)
	// Chỉ có ý nghĩa nếu event này là một Root.
	Vote map[EventID]bool `borsh:"skip"` // Không tuần tự hóa Vote map trực tiếp, cần lưu trữ/tải riêng nếu cần persistence

	// Candidate đánh dấu Root này có phải là ứng viên Atropos không (sau khi Clotho Selection)
	Candidate bool `borsh:"skip"` // Không tuần tự hóa, trạng thái này được suy ra/lưu trữ riêng

	// ClothoStatus lưu trữ trạng thái quyết định Clotho của Root này.
	ClothoStatus ClothoStatus `borsh:"skip"` // Không tuần tự hóa, trạng thái này được suy ra/lưu trữ riêng

	// Mutex cho các trường không tuần tự hóa (Vote, Candidate, ClothoStatus)
	// Đảm bảo an toàn khi cập nhật các trạng thái này
	mu sync.Mutex `borsh:"skip"`
}

// NewEvent tạo một Event mới với các trường được khởi tạo.
func NewEvent(data EventData, signature []byte) *Event {
	event := &Event{
		EventData:    data,
		Signature:    signature,
		Vote:         make(map[EventID]bool), // Khởi tạo map Vote
		ClothoStatus: ClothoUndecided,        // Khởi tạo trạng thái là Undecided
	}
	// Tính toán và cache hash ngay khi tạo
	_, _ = event.Hash() // Bỏ qua lỗi, nên xử lý thực tế
	return event
}

// PrepareForHashing sắp xếp các giao dịch và các OtherParents để đảm bảo tính xác định
// trước khi tính hash của EventData. Việc sắp xếp này là quan trọng để cùng một tập dữ liệu
// luôn tạo ra cùng một hash, bất kể thứ tự ban đầu của các phần tử trong slice.
func (e *Event) PrepareForHashing() error {
	// 1. Sắp xếp các giao dịch
	if len(e.EventData.Transactions) > 0 {
		// Sắp xếp các giao dịch theo thứ tự byte của chúng (hoặc theo hash của giao dịch nếu có)
		// Đây là ví dụ đơn giản sắp xếp theo byte của slice
		sort.SliceStable(e.EventData.Transactions, func(i, j int) bool {
			return bytes.Compare(e.EventData.Transactions[i], e.EventData.Transactions[j]) < 0
		})
	}

	// 2. Sắp xếp các OtherParents
	if len(e.EventData.OtherParents) > 0 {
		// Sắp xếp các EventID của parent khác theo thứ tự byte của hash
		sort.SliceStable(e.EventData.OtherParents, func(i, j int) bool {
			// EventID là common.Hash, có thể so sánh trực tiếp byte của chúng
			return bytes.Compare(e.EventData.OtherParents[i].Bytes(), e.EventData.OtherParents[j].Bytes()) < 0
		})
	}

	return nil
}

// Hash tính toán hash (EventID) của EventData.
// Hash được tính toán trên dữ liệu đã được tuần tự hóa bằng Borsh sau khi PrepareForHashing.
// Kết quả được cache bằng atomic.Pointer để tránh tính toán lại nhiều lần.
func (e *Event) Hash() (EventID, error) {
	// Tải giá trị cache hiện tại một cách nguyên tử (thread-safe)
	cached := e.cachedHash.Load()
	if cached != nil {
		return EventID(*cached), nil // Trả về giá trị từ cache nếu đã có
	}

	// Chuẩn bị dữ liệu trước khi hashing (sắp xếp các slice)
	if err := e.PrepareForHashing(); err != nil {
		return EventID{}, fmt.Errorf("failed to prepare event for hashing: %w", err)
	}

	// Tuần tự hóa EventData bằng Borsh để có dữ liệu byte dùng cho hashing
	dataBytes, err := borsh.Serialize(e.EventData)
	if err != nil {
		return EventID{}, fmt.Errorf("failed to serialize EventData for hashing: %w", err)
	}

	// Tính toán Keccak256 hash của dữ liệu đã tuần tự hóa
	hash := crypto.Keccak256Hash(dataBytes)

	// Lưu giá trị hash vừa tính vào cache một cách nguyên tử (thread-safe)
	// Tạo một con trỏ tới common.Hash và lưu địa chỉ của con trỏ đó
	e.cachedHash.Store(&hash)

	// Trả về hash dưới dạng EventID
	return EventID(hash), nil
}

// GetEventId trả về EventID (hash) của Event.
// Nếu hash chưa được tính toán và cache, nó sẽ gọi Hash() để tính toán và lưu vào cache.
func (e *Event) GetEventId() EventID {
	// Tải giá trị từ cache một cách nguyên tử
	cached := e.cachedHash.Load()
	if cached != nil {
		// Dereference con trỏ để lấy giá trị common.Hash, rồi ép kiểu sang EventID
		return EventID(*cached)
	}

	// Nếu chưa có trong cache, tính toán và lưu vào cache.
	// Lưu ý: Ở đây bỏ qua lỗi trả về từ e.Hash() cho đơn giản, trong ứng dụng thực tế
	// bạn nên xử lý lỗi này một cách thích hợp (ví dụ: log lỗi, panic, hoặc trả về zero EventID).
	hash, _ := e.Hash()
	return hash
}

// Marshal tuần tự hóa toàn bộ Event (bao gồm cả Signature) thành dạng byte sử dụng Borsh.
// Lưu ý: Các trường không có tag borsh hoặc có tag `borsh:"skip"` sẽ không được tuần tự hóa.
func (e *Event) Marshal() ([]byte, error) {
	// Chỉ tuần tự hóa EventData và Signature. Các trường Clotho-specific không được tuần tự hóa.
	// Nếu cần persistence cho trạng thái Clotho, cần lưu trữ riêng.
	return borsh.Serialize(e)
}

// Unmarshal giải tuần tự hóa bytes thành cấu trúc Event sử dụng Borsh.
// Sau khi giải tuần tự hóa, nó sẽ tính toán và cache hash của EventData.
// Các trường Clotho-specific (Vote, Candidate, ClothoStatus) sẽ có giá trị mặc định (nil map, false, Undecided).
func Unmarshal(data []byte) (*Event, error) {
	var event Event
	// Giải tuần tự hóa dữ liệu vào cấu trúc Event
	err := borsh.Deserialize(&event, data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize event: %w", err)
	}

	// Khởi tạo các trường không tuần tự hóa
	event.Vote = make(map[EventID]bool)
	event.ClothoStatus = ClothoUndecided
	event.Candidate = false // Mặc định không phải ứng viên

	// Sau khi giải tuần tự hóa, tính toán và cache hash của EventData
	// để nó sẵn sàng cho các lần gọi GetEventId/Hash tiếp theo mà không cần tính lại.
	// Lưu ý: Bỏ qua lỗi ở đây, trong ứng dụng thực tế nên xử lý lỗi trả về từ event.Hash().
	_, _ = event.Hash() // This will calculate and store the hash in cachedHash

	return &event, nil
}

//-----------------------------------------------------------------------------
// Helper Functions for Event
//-----------------------------------------------------------------------------

// ToEventID là hàm helper để chuyển đổi common.Hash thành EventID một cách tường minh.
func ToEventID(hash common.Hash) EventID {
	return EventID(hash)
}

// HexToEventID là hàm helper để chuyển đổi chuỗi hex thành EventID.
// Thường dùng để khởi tạo EventID từ dạng chuỗi.
func HexToEventID(hexStr string) EventID {
	return EventID(common.HexToHash(hexStr))
}

// SetVote thiết lập phiếu bầu của event này cho một voting subject root.
func (e *Event) SetVote(subjectRootID EventID, vote bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Vote[subjectRootID] = vote
}

// GetVote lấy phiếu bầu của event này cho một voting subject root.
// Trả về false và false nếu chưa có phiếu bầu.
func (e *Event) GetVote(subjectRootID EventID) (vote bool, exists bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	vote, exists = e.Vote[subjectRootID]
	return
}

// SetClothoStatus thiết lập trạng thái Clotho của event này.
func (e *Event) SetClothoStatus(status ClothoStatus) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ClothoStatus = status
}

// SetCandidate thiết lập trạng thái ứng viên Atropos của event này.
func (e *Event) SetCandidate(candidate bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Candidate = candidate
}
