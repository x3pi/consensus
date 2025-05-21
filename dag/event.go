package dag

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"log" // Used for logging potential errors during event hashing.
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	borsh "github.com/near/borsh-go"

	"github.com/blockchain/consensus/logger" // Custom logger.
)

// EventID represents the hash of an Event. It is specifically the hash of EventData.
type EventID common.Hash

// Bytes returns the byte representation of the EventID.
func (id EventID) Bytes() []byte {
	return common.Hash(id).Bytes()
}

// String returns the hexadecimal representation of the EventID.
func (id EventID) String() string {
	return common.Hash(id).Hex()
}

// IsZero checks if the EventID is a zero hash.
func (id EventID) IsZero() bool {
	return common.Hash(id) == common.Hash{}
}

// ClothoStatus defines the Clotho selection status of a Root event.
type ClothoStatus string

const (
	// ClothoUndecided indicates that the Clotho status has not yet been determined.
	ClothoUndecided ClothoStatus = "UNDECIDED"
	// ClothoIsClotho indicates that the event has been decided as a Clotho (Atropos candidate).
	ClothoIsClotho ClothoStatus = "IS-CLOTHO"
	// ClothoIsNotClotho indicates that the event has been decided not to be a Clotho.
	ClothoIsNotClotho ClothoStatus = "IS-NOT-CLOTHO"
)

// EventData contains the core data of an Event, used for hash computation (EventID).
// Fields in this struct are serialized using Borsh for hashing.
type EventData struct {
	Transactions []byte    `borsh:"slice"`     // List of transactions or other data (byte slice).
	SelfParent   EventID   `borsh:"[32]uint8"` // Hash of the self-parent event (from the same validator, 32 bytes).
	OtherParents []EventID `borsh:"slice"`     // Hashes of other parent events (from different validators, slice of 32-byte hashes).
	Creator      []byte    `borsh:"slice"`     // Public key of the event creator (byte slice).
	Index        uint64    `borsh:"uint64"`    // Sequence number of the event created by this validator.
	Timestamp    int64     `borsh:"int64"`     // Unix timestamp (event creation time).
	Frame        uint64    `borsh:"uint64"`    // Frame number of this event.
	IsRoot       bool      `borsh:"bool"`      // Flag indicating if this event is a Root of its frame.
}

// Event is the complete structure of an Event in the DAG.
// It includes EventData, signature, and a cached hash.
type Event struct {
	EventData // Embedded EventData - data used for hashing.

	Signature []byte `borsh:"slice"` // Signature of the EventData hash by the creator.

	// cachedHash caches the EventData hash (EventID) using atomic.Pointer for concurrent access safety.
	cachedHash atomic.Pointer[common.Hash] `borsh:"skip"`

	// Fields related to Clotho Selection.
	// Vote stores votes cast by this event (if it's a Root) for other Roots.
	// Key: EventID of the Root being voted on. Value: Vote (true = YES).
	Vote map[EventID]bool `borsh:"skip"`

	// Candidate indicates if this Root event is an Atropos candidate.
	Candidate bool `borsh:"skip"`

	// ClothoStatus stores the decided Clotho status of this Root event.
	ClothoStatus ClothoStatus `borsh:"skip"`

	// mu protects access to non-serialized fields (Vote, Candidate, ClothoStatus).
	mu sync.Mutex `borsh:"skip"`
}

// NewEvent creates a new Event with initialized fields.
func NewEvent(data EventData, signature []byte) *Event {
	event := &Event{
		EventData:    data,
		Signature:    signature,
		Vote:         make(map[EventID]bool),
		ClothoStatus: ClothoUndecided,
		Candidate:    false,
		// mu and cachedHash will have their default zero values.
	}
	// Calculate and cache the hash upon creation.
	_, err := event.Hash()
	if err != nil {
		// In a production environment, this error might need more robust handling,
		// e.g., returning an error from NewEvent or panicking if the hash is critical at creation.
		log.Printf("Warning: Error calculating hash in NewEvent: %v", err)
	}
	return event
}

// PrepareForHashing sorts OtherParents to ensure deterministic hashing of EventData.
func (e *Event) PrepareForHashing() error {
	if len(e.EventData.OtherParents) > 0 {
		sort.SliceStable(e.EventData.OtherParents, func(i, j int) bool {
			return bytes.Compare(e.EventData.OtherParents[i].Bytes(), e.EventData.OtherParents[j].Bytes()) < 0
		})
	}
	return nil
}

// Hash computes the hash (EventID) of the EventData.
// The result is cached to avoid recomputation.
func (e *Event) Hash() (EventID, error) {
	if cached := e.cachedHash.Load(); cached != nil {
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

// GetEventId returns the EventID (hash) of the Event.
func (e *Event) GetEventId() EventID {
	if cached := e.cachedHash.Load(); cached != nil {
		return EventID(*cached)
	}
	// If the hash is not cached, compute it.
	// Hash() is called in NewEvent and Unmarshal, so this case is unlikely unless there was a prior error.
	hash, err := e.Hash()
	if err != nil {
		log.Printf("Warning: Error calling Hash() inside GetEventId(): %v. Returning zero EventID.", err)
		return EventID{} // Return a zero EventID on error.
	}
	return hash
}

// Marshal serializes the entire Event (EventData and Signature) into a byte slice using Borsh.
func (e *Event) Marshal() ([]byte, error) {
	// Define a temporary struct containing only fields to be serialized
	// to ensure `borsh:"skip"` fields are not affected.
	type serializableEvent struct {
		EventData
		Signature []byte `borsh:"slice"`
	}
	temp := serializableEvent{
		EventData: e.EventData,
		Signature: e.Signature,
	}
	return borsh.Serialize(temp)
}

// serializableEventForUnmarshal is a temporary struct containing only the fields
// intended for deserialization from a data stream.
// This helps avoid reflection issues with unexported fields
// or complex fields marked `borsh:"skip"` in the main Event struct.
type serializableEventForUnmarshal struct {
	EventData
	Signature []byte `borsh:"slice"`
}

// Unmarshal deserializes a byte slice into an Event struct using Borsh.
// It uses a temporary struct to deserialize core fields,
// then initializes the remaining fields of the Event.
func Unmarshal(data []byte) (*Event, error) {
	var temp serializableEventForUnmarshal
	if err := borsh.Deserialize(&temp, data); err != nil {
		return nil, fmt.Errorf("failed to deserialize event core data: %w", err)
	}

	// Create the complete Event struct and populate it from the temporary struct.
	event := &Event{
		EventData:    temp.EventData,
		Signature:    temp.Signature,
		Vote:         make(map[EventID]bool),
		ClothoStatus: ClothoUndecided,
		Candidate:    false,
		// mu (sync.Mutex) will be initialized to its zero value.
		// cachedHash (atomic.Pointer) will also be initialized to its zero value (nil).
	}

	// Calculate and cache the hash after deserializing and initializing the Event.
	// Hash() relies only on EventData, so this order is safe.
	if _, err := event.Hash(); err != nil {
		// Log this error as it might be important for later debugging.
		log.Printf("Warning: Error calculating hash after unmarshalling event: %v", err)
		// Depending on requirements, you might want to return an error here if the hash is mandatory.
		// return nil, fmt.Errorf("failed to calculate hash after unmarshalling event: %w", err)
	}

	return event, nil
}

// ToEventID is a helper function to convert a common.Hash to an EventID.
func ToEventID(hash common.Hash) EventID {
	return EventID(hash)
}

// HexToEventID is a helper function to convert a hex string to an EventID.
func HexToEventID(hexStr string) EventID {
	return EventID(common.HexToHash(hexStr))
}

// SetVote sets this event's vote for a subject root event.
func (e *Event) SetVote(subjectRootID EventID, vote bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.Vote == nil { // Ensure the map is initialized.
		e.Vote = make(map[EventID]bool)
	}
	e.Vote[subjectRootID] = vote
}

// GetVote retrieves this event's vote for a subject root event.
// It returns the vote and a boolean indicating if the vote exists.
func (e *Event) GetVote(subjectRootID EventID) (vote bool, exists bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.Vote == nil {
		return false, false
	}
	vote, exists = e.Vote[subjectRootID]
	return
}

// SetClothoStatus sets the Clotho status of this event.
func (e *Event) SetClothoStatus(status ClothoStatus) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ClothoStatus = status
}

// SetCandidate sets the Atropos candidate status of this event.
func (e *Event) SetCandidate(candidate bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Candidate = candidate
}

// Short returns a short representation of the EventID (e.g., "0x" + first 6 hex characters).
// This method is verified to be present and correct as per requirements.
func (id EventID) Short() string {
	s := id.String() // Uses the existing String() method of EventID.
	// A full EventID is "0x" + 64 hex characters. We want "0x" + 6 characters (8 total).
	if len(s) > 8 {
		return s[:8] // Returns "0x" and the first 6 hex characters.
	}
	return s // If shorter, return the whole string.
}

// String returns a string representation of the Event, useful for logging and debugging.
// This method is verified to be present and correct as per requirements.
func (e *Event) String() string {
	if e == nil {
		return "<nil Event>"
	}

	var sb strings.Builder
	eventID := e.GetEventId()

	sb.WriteString(fmt.Sprintf("Event[%s]:\n", eventID.Short()))
	sb.WriteString(fmt.Sprintf("  Creator: %s\n", hex.EncodeToString(e.EventData.Creator)))
	sb.WriteString(fmt.Sprintf("  Index: %d\n", e.EventData.Index))
	sb.WriteString(fmt.Sprintf("  Frame: %d\n", e.EventData.Frame))
	sb.WriteString(fmt.Sprintf("  IsRoot: %t\n", e.EventData.IsRoot))
	sb.WriteString(fmt.Sprintf("  Timestamp: %s (%d)\n", time.Unix(e.EventData.Timestamp, 0).Format(time.RFC3339), e.EventData.Timestamp))

	if e.EventData.SelfParent.IsZero() {
		sb.WriteString("  SelfParent: None\n")
	} else {
		sb.WriteString(fmt.Sprintf("  SelfParent: %s\n", e.EventData.SelfParent.Short()))
	}

	if len(e.EventData.OtherParents) == 0 {
		sb.WriteString("  OtherParents: None\n")
	} else {
		var opShorts []string
		for _, op := range e.EventData.OtherParents {
			opShorts = append(opShorts, op.Short())
		}
		sb.WriteString(fmt.Sprintf("  OtherParents: [%s]\n", strings.Join(opShorts, ", ")))
	}

	sb.WriteString(fmt.Sprintf("  Transactions: %d bytes\n", len(e.EventData.Transactions)))
	sb.WriteString(fmt.Sprintf("  Signature: %s...\n", hex.EncodeToString(e.Signature[:min(8, len(e.Signature))]))) // Print part of the signature.

	// Clotho information (if applicable).
	if e.EventData.IsRoot {
		e.mu.Lock() // Lock needed when accessing Clotho fields.
		sb.WriteString(fmt.Sprintf("  ClothoStatus: %s\n", e.ClothoStatus))
		sb.WriteString(fmt.Sprintf("  Candidate: %t\n", e.Candidate))
		if len(e.Vote) > 0 {
			var voteStrings []string
			for votedID, voteVal := range e.Vote {
				voteStrings = append(voteStrings, fmt.Sprintf("%s->%t", votedID.Short(), voteVal))
			}
			sb.WriteString(fmt.Sprintf("  Votes: {%s}\n", strings.Join(voteStrings, ", ")))
		} else {
			sb.WriteString("  Votes: None\n")
		}
		e.mu.Unlock()
	}
	// The following line uses the custom logger as per the original file's logic.
	// Note: Logging within a String() method can have side effects and may log large amounts of data.
	logger.Info(e.EventData.Transactions) // - Kept as per original logic.
	return sb.String()
}

// min returns the smaller of a and b.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
