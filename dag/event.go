package dag

import (
	"bytes"
	"fmt"
	"log" // Thêm import log để ghi lại lỗi tiềm ẩn từ event.Hash()
	"sort"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	borsh "github.com/near/borsh-go"
)

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

// ClothoStatus định nghĩa trạng thái lựa chọn Clotho của một Root.
type ClothoStatus string

const (
	ClothoUndecided   ClothoStatus = "UNDECIDED"     // Chưa được quyết định
	ClothoIsClotho    ClothoStatus = "IS-CLOTHO"     // Đã quyết định là Clotho (ứng viên Atropos)
	ClothoIsNotClotho ClothoStatus = "IS-NOT-CLOTHO" // Đã quyết định không phải là Clotho
)

// EventData là cấu trúc chứa dữ liệu cốt lõi của Event được dùng để tính toán hash (EventID).
// Các trường trong cấu trúc này sẽ được tuần tự hóa bằng Borsh để hashing.
type EventData struct {
	Transactions []byte    `borsh:"slice"`     // Danh sách các giao dịch hoặc dữ liệu khác (byte slice)
	SelfParent   EventID   `borsh:"[32]uint8"` // Hash của parent cùng validator (size 32 bytes)
	OtherParents []EventID `borsh:"slice"`     // Hash của các parent khác validator (slice các size 32 bytes)
	Creator      []byte    `borsh:"slice"`     // Public key của người tạo event (byte slice)
	Index        uint64    `borsh:"uint64"`    // Sequence number của event do validator này tạo
	Timestamp    int64     `borsh:"int64"`     // Unix timestamp (thời gian tạo event)
	Frame        uint64    `borsh:"uint64"`    // Frame number của event này
	IsRoot       bool      `borsh:"bool"`      // Cờ đánh dấu event này có phải là Root của frame không
}

// Event là cấu trúc hoàn chỉnh của một Event trong DAG.
// Nó bao gồm dữ liệu EventData, chữ ký và cache hash.
type Event struct {
	EventData // Nhúng EventData - dữ liệu dùng để hash

	Signature []byte `borsh:"slice"` // Chữ ký của người tạo trên hash của EventData

	// Cache cho hash of EventData (EventID) sử dụng atomic.Pointer để an toàn cho đồng thời.
	cachedHash atomic.Pointer[common.Hash] `borsh:"skip"`

	// --- Trường liên quan đến Clotho Selection ---
	Vote map[EventID]bool `borsh:"skip"` // Key: EventID của Root được bỏ phiếu, Value: Kết quả (true = YES)
	// Chỉ có ý nghĩa nếu event này là một Root.

	Candidate bool `borsh:"skip"` // Đánh dấu Root này có phải là ứng viên Atropos không

	ClothoStatus ClothoStatus `borsh:"skip"` // Lưu trữ trạng thái quyết định Clotho của Root này.

	mu sync.Mutex `borsh:"skip"` // Mutex cho các trường không tuần tự hóa (Vote, Candidate, ClothoStatus)
}

// NewEvent tạo một Event mới với các trường được khởi tạo.
func NewEvent(data EventData, signature []byte) *Event {
	event := &Event{
		EventData:    data,
		Signature:    signature,
		Vote:         make(map[EventID]bool), // Khởi tạo map Vote
		ClothoStatus: ClothoUndecided,        // Khởi tạo trạng thái là Undecided
		Candidate:    false,                  // Mặc định không phải ứng viên
		// mu và cachedHash sẽ có giá trị zero mặc định của chúng
	}
	// Tính toán và cache hash ngay khi tạo
	_, err := event.Hash()
	if err != nil {
		// Trong thực tế, bạn có thể muốn xử lý lỗi này nghiêm túc hơn,
		// ví dụ: trả về lỗi từ NewEvent hoặc panic nếu hash là thiết yếu ngay lúc tạo.
		log.Printf("Warning: Lỗi khi tính hash trong NewEvent: %v", err)
	}
	return event
}

// PrepareForHashing sắp xếp các OtherParents để đảm bảo tính xác định
// trước khi tính hash của EventData.
func (e *Event) PrepareForHashing() error {
	if len(e.EventData.OtherParents) > 0 {
		sort.SliceStable(e.EventData.OtherParents, func(i, j int) bool {
			return bytes.Compare(e.EventData.OtherParents[i].Bytes(), e.EventData.OtherParents[j].Bytes()) < 0
		})
	}
	return nil
}

// Hash tính toán hash (EventID) của EventData.
// Kết quả được cache để tránh tính toán lại nhiều lần.
func (e *Event) Hash() (EventID, error) {
	cached := e.cachedHash.Load()
	if cached != nil {
		return EventID(*cached), nil
	}

	if err := e.PrepareForHashing(); err != nil {
		return EventID{}, fmt.Errorf("failed to prepare event for hashing: %w", err)
	}

	dataBytes, err := borsh.Serialize(e.EventData)
	if err != nil {
		return EventID{}, fmt.Errorf("failed to serialize EventData for hashing: %w", err)
	}

	hash := crypto.Keccak256Hash(dataBytes)
	e.cachedHash.Store(&hash)
	return EventID(hash), nil
}

// GetEventId trả về EventID (hash) của Event.
func (e *Event) GetEventId() EventID {
	cached := e.cachedHash.Load()
	if cached != nil {
		return EventID(*cached)
	}
	// Nếu hash chưa được cache, tính toán nó.
	// Trong NewEvent và Unmarshal, Hash() được gọi, nên trường hợp này ít khi xảy ra trừ khi có lỗi trước đó.
	hash, err := e.Hash()
	if err != nil {
		log.Printf("Warning: Lỗi khi gọi Hash() bên trong GetEventId(): %v. Trả về EventID rỗng.", err)
		return EventID{} // Trả về EventID rỗng nếu có lỗi
	}
	return hash
}

// Marshal tuần tự hóa toàn bộ Event (EventData và Signature) thành dạng byte sử dụng Borsh.
func (e *Event) Marshal() ([]byte, error) {
	// Định nghĩa một struct tạm thời chỉ chứa các trường cần serialize
	// để đảm bảo các trường `borsh:"skip"` không ảnh hưởng.
	type serializableEvent struct {
		EventData        // Nhúng EventData
		Signature []byte `borsh:"slice"`
	}
	temp := serializableEvent{
		EventData: e.EventData,
		Signature: e.Signature,
	}
	return borsh.Serialize(temp)
}

// serializableEventForUnmarshal là một struct tạm thời chỉ chứa các trường
// mà chúng ta muốn deserialize từ stream dữ liệu.
// Điều này giúp tránh các vấn đề reflection với các trường không được export
// hoặc các trường phức tạp được đánh dấu `borsh:"skip"` trong struct Event chính.
type serializableEventForUnmarshal struct {
	EventData        // Nhúng EventData
	Signature []byte `borsh:"slice"`
}

// Unmarshal giải tuần tự hóa bytes thành cấu trúc Event sử dụng Borsh.
// Nó sử dụng một struct tạm thời để deserialize các trường cốt lõi,
// sau đó khởi tạo các trường còn lại của Event.
func Unmarshal(data []byte) (*Event, error) {
	var temp serializableEventForUnmarshal
	err := borsh.Deserialize(&temp, data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize event core data: %w", err)
	}

	// Tạo struct Event hoàn chỉnh và điền dữ liệu từ struct tạm thời
	event := &Event{
		EventData:    temp.EventData,
		Signature:    temp.Signature,
		Vote:         make(map[EventID]bool), // Khởi tạo map Vote
		ClothoStatus: ClothoUndecided,        // Khởi tạo trạng thái là Undecided
		Candidate:    false,                  // Mặc định không phải ứng viên
		// mu (sync.Mutex) sẽ được khởi tạo với giá trị zero của nó.
		// cachedHash (atomic.Pointer) cũng sẽ được khởi tạo với giá trị zero (nil).
	}

	// Tính toán và cache hash sau khi deserialize và khởi tạo Event
	// Quan trọng: Hash() cần được gọi trên event đã được cấu trúc hoàn chỉnh (nếu logic hash phụ thuộc vào nó)
	// nhưng ở đây Hash() chỉ dựa trên EventData, nên thứ tự này là ổn.
	_, err = event.Hash()
	if err != nil {
		// Ghi log lỗi này vì nó có thể quan trọng cho việc debug sau này
		log.Printf("Warning: Lỗi khi tính toán hash sau khi Unmarshal event: %v", err)
		// Tùy thuộc vào yêu cầu, bạn có thể muốn trả về lỗi ở đây nếu hash là bắt buộc
		// return nil, fmt.Errorf("failed to calculate hash after unmarshalling event: %w", err)
	}

	return event, nil
}

// ToEventID là hàm helper để chuyển đổi common.Hash thành EventID.
func ToEventID(hash common.Hash) EventID {
	return EventID(hash)
}

// HexToEventID là hàm helper để chuyển đổi chuỗi hex thành EventID.
func HexToEventID(hexStr string) EventID {
	return EventID(common.HexToHash(hexStr))
}

// SetVote thiết lập phiếu bầu của event này cho một voting subject root.
func (e *Event) SetVote(subjectRootID EventID, vote bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.Vote == nil { // Đảm bảo map được khởi tạo
		e.Vote = make(map[EventID]bool)
	}
	e.Vote[subjectRootID] = vote
}

// GetVote lấy phiếu bầu của event này cho một voting subject root.
func (e *Event) GetVote(subjectRootID EventID) (vote bool, exists bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.Vote == nil {
		return false, false
	}
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
