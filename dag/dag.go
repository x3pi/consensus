package dag

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/blockchain/consensus/logger" // Custom logger for the consensus package.
)

// QUORUM defines the voting threshold (by stake) required to decide Clotho status.
// For example, this could be 2/3 of the total network stake + 1.
// This value should be set based on the network's total stake distribution.
// The current value of 2 is likely a placeholder for a small test network.
const QUORUM uint64 = 2

// DagStore manages Event blocks within the DAG and implements Clotho Selection logic.
type DagStore struct {
	// events stores all events, keyed by their EventID (hash).
	events map[EventID]*Event
	// latestEvents maps the hex public key of a creator to their latest EventID.
	latestEvents map[string]EventID
	// rootsByFrame maps a frame number to a slice of EventIDs of Root events in that frame.
	rootsByFrame map[uint64][]EventID
	// lastDecidedFrame is the last frame for which Clotho has been decided for all its roots.
	lastDecidedFrame uint64
	// mu is a RWMutex to synchronize access to DagStore's maps and fields.
	mu sync.RWMutex
	// stake maps the hex public key of a creator (validator) to their stake amount.
	stake map[string]uint64
}

// NewDagStore creates a new instance of DagStore.
// initialStake is a map from a validator's hex public key to their stake amount.
func NewDagStore(initialStake map[string]uint64) *DagStore {
	// Copy initialStake to prevent external modification.
	stakeMap := make(map[string]uint64, len(initialStake))
	for pubKeyHex, s := range initialStake {
		stakeMap[pubKeyHex] = s
	}

	return &DagStore{
		events:           make(map[EventID]*Event),
		latestEvents:     make(map[string]EventID),
		rootsByFrame:     make(map[uint64][]EventID),
		lastDecidedFrame: 0, // Or 1, depending on the starting frame convention.
		mu:               sync.RWMutex{},
		stake:            stakeMap,
	}
}

// AddEvent adds a new event to the DagStore.
// It performs basic validity checks such as parent existence, index, and uniqueness.
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
		return nil // Event already exists, no need to add again.
	}

	creatorKey := hex.EncodeToString(event.EventData.Creator)

	currentLatestEventID, creatorHasPrevious := ds.latestEvents[creatorKey]
	if creatorHasPrevious {
		// Check self-parent for non-first events of a creator.
		if event.EventData.Index > 1 && event.EventData.SelfParent.IsZero() {
			return fmt.Errorf("event %s by %s (Index %d) has zero self parent but creator has previous events",
				eventID.String()[:6], creatorKey[:6], event.EventData.Index)
		}
		if event.EventData.Index > 1 && event.EventData.SelfParent != currentLatestEventID {
			var expectedPrevIndex uint64
			if selfParentEvent, ok := ds.events[currentLatestEventID]; ok {
				expectedPrevIndex = selfParentEvent.EventData.Index
			}
			return fmt.Errorf("invalid self parent for event %s by %s (Index %d): expected %s (Index %d), got %s (Index %d expected based on current)",
				eventID.String()[:6], creatorKey[:6], event.EventData.Index,
				currentLatestEventID.String()[:6], expectedPrevIndex,
				event.EventData.SelfParent.String()[:6], event.EventData.Index-1)
		}
		if prevEvent, ok := ds.events[currentLatestEventID]; ok {
			if event.EventData.Index != prevEvent.EventData.Index+1 {
				return fmt.Errorf("invalid index for event %s by %s: expected %d, got %d",
					eventID.String()[:6], creatorKey[:6], prevEvent.EventData.Index+1, event.EventData.Index)
			}
		}

	} else { // This is the creator's first event.
		if !event.EventData.SelfParent.IsZero() {
			return fmt.Errorf("first event %s of creator %s must have zero self parent, got %s", eventID.String()[:6], creatorKey[:6], event.EventData.SelfParent.String()[:6])
		}
		if event.EventData.Index != 1 {
			return fmt.Errorf("first event %s of creator %s must have index 1, got %d", eventID.String()[:6], creatorKey[:6], event.EventData.Index)
		}
		if _, exists := ds.stake[creatorKey]; !exists {
			logger.Warn(fmt.Sprintf("DagStore.AddEvent: Creator %s (%s) is adding their first event but has no registered stake.", creatorKey[:6], eventID.String()[:6]))
		}
	}

	for _, otherParentID := range event.EventData.OtherParents {
		if otherParentID.IsZero() {
			continue
		}
		if _, exists := ds.events[otherParentID]; !exists {
			logger.Warn(fmt.Sprintf("DagStore.AddEvent: Other parent %s for event %s does not exist in DagStore. Event might be an orphan temporarily.", otherParentID.String()[:6], eventID.String()[:6]))
		}
	}

	ds.events[eventID] = event

	// Update latestEvents only if the new event has a higher index.
	if !creatorHasPrevious || (ds.events[currentLatestEventID] != nil && event.EventData.Index > ds.events[currentLatestEventID].EventData.Index) {
		ds.latestEvents[creatorKey] = eventID
	}

	if event.EventData.IsRoot {
		// Check and add to rootsByFrame if not already present.
		isAlreadyRoot := false
		for _, existingRootID := range ds.rootsByFrame[event.EventData.Frame] {
			if existingRootID == eventID {
				isAlreadyRoot = true
				break
			}
		}
		if !isAlreadyRoot {
			ds.rootsByFrame[event.EventData.Frame] = append(ds.rootsByFrame[event.EventData.Frame], eventID)
			// Keep the list of roots consistently sorted.
			sort.Slice(ds.rootsByFrame[event.EventData.Frame], func(i, j int) bool {
				return ds.rootsByFrame[event.EventData.Frame][i].String() < ds.rootsByFrame[event.EventData.Frame][j].String()
			})
		}
	}
	logger.Info(fmt.Sprintf("DagStore.AddEvent: Successfully added event: %s (Creator: %s, Index: %d, Frame: %d, IsRoot: %t)",
		eventID.String()[:6], creatorKey[:6], event.EventData.Index, event.EventData.Frame, event.EventData.IsRoot))
	return nil
}

// GetEvent retrieves an event from the store by its EventID.
func (ds *DagStore) GetEvent(id EventID) (*Event, bool) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	event, exists := ds.events[id]
	return event, exists
}

// EventExists checks if an event exists in the store by its EventID.
func (ds *DagStore) EventExists(id EventID) bool {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	_, exists := ds.events[id]
	return exists
}

// GetLatestEventIDByCreatorPubKeyHex returns the latest EventID created by a specific creator.
func (ds *DagStore) GetLatestEventIDByCreatorPubKeyHex(creatorPubKeyHex string) (EventID, bool) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	eventID, exists := ds.latestEvents[creatorPubKeyHex]
	return eventID, exists
}

// GetRoots returns a slice containing EventIDs of all root events in a specific frame.
// Returns a copy to prevent external modification.
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

// GetLastDecidedFrame returns the last frame for which Clotho has been decided.
func (ds *DagStore) GetLastDecidedFrame() uint64 {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.lastDecidedFrame
}

// getStakeLocked retrieves the stake of a validator based on their hex public key.
// This method requires ds.mu to be held (read or write lock).
func (ds *DagStore) getStakeLocked(creatorPubKeyHex string) uint64 {
	stake, exists := ds.stake[creatorPubKeyHex]
	if !exists {
		return 0
	}
	return stake
}

// isAncestorLocked checks if event ancestorID is an ancestor of event descendantID.
// This method requires ds.mu to be held (read or write lock).
func (ds *DagStore) isAncestorLocked(ancestorID, descendantID EventID) bool {
	if ancestorID == descendantID {
		return true
	}
	if ancestorID.IsZero() || descendantID.IsZero() {
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
			logger.Warn(fmt.Sprintf("DagStore.isAncestorLocked: Event %s not found in store during ancestry check for descendant %s from ancestor %s",
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

// IsAncestor is the public version of isAncestorLocked, managing the lock.
func (ds *DagStore) IsAncestor(ancestorID, descendantID EventID) bool {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.isAncestorLocked(ancestorID, descendantID)
}

// forklessCause checks if event x "forkless causes" event y.
// This means x is an ancestor of y, and y does not see any forks related to x.
// This method requires ds.mu to be held for isAncestorLocked.
// Note: The current implementation is a simplified alias for isAncestorLocked.
// A full forklessCause implementation might be more complex.
func (ds *DagStore) forklessCause(x, y *Event) bool {
	if x == nil || y == nil {
		return false
	}
	return ds.isAncestorLocked(x.GetEventId(), y.GetEventId())
}

// DecideClotho implements the Clotho selection algorithm.
// This algorithm determines the Clotho status (IS-CLOTHO / IS-NOT-CLOTHO) for Root events.
func (ds *DagStore) DecideClotho() {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	logger.Info("DecideClotho: Starting Clotho decision process.")

	startFrame := ds.lastDecidedFrame + 1
	var maxFrameConsidered uint64 = 0
	for frame := range ds.rootsByFrame {
		if frame > maxFrameConsidered {
			maxFrameConsidered = frame
		}
	}
	logger.Info(fmt.Sprintf("DecideClotho: StartFrame=%d, MaxFrameConsidered=%d, CurrentLastDecidedFrame=%d", startFrame, maxFrameConsidered, ds.lastDecidedFrame))

	if startFrame > maxFrameConsidered {
		logger.Info("DecideClotho: No new frames to process.")
		return
	}

	// Map để theo dõi xem một xEvent đã được quyết định trong lượt chạy DecideClotho này chưa.
	// Điều này giúp continue nextXEventProcessingLoop hoạt động chính xác hơn
	// và tránh xử lý lại xEvent đã được quyết định ngay trong cùng một cuộc gọi DecideClotho.
	decidedInThisRun := make(map[EventID]bool)

	for xFrame := startFrame; xFrame <= maxFrameConsidered; xFrame++ {
		logger.Info(fmt.Sprintf("DecideClotho: Processing xFrame = %d", xFrame))
		xRootsIDsInCurrentXFrame := ds.rootsByFrame[xFrame]
		if len(xRootsIDsInCurrentXFrame) == 0 {
			logger.Info(fmt.Sprintf("DecideClotho: No roots found in xFrame %d.", xFrame))
			// Không có root trong frame này, nhưng có thể frame này vẫn cần được coi là "đã xử lý"
			// nếu không có frame nào trước đó bị kẹt. Tuy nhiên, logic allRootsInXFrameDecided
			// ở cuối sẽ xử lý việc cập nhật lastDecidedFrame một cách chính xác.
		}

		var currentXEventsToProcess []*Event
		for _, xID := range xRootsIDsInCurrentXFrame {
			xEvent, exists := ds.events[xID]
			// Chỉ xử lý nếu event tồn tại, là Root, và chưa được quyết định (ClothoUndecided)
			// VÀ chưa được quyết định trong chính lượt chạy DecideClotho này.
			if exists && xEvent.EventData.IsRoot && xEvent.ClothoStatus == ClothoUndecided && !decidedInThisRun[xID] {
				currentXEventsToProcess = append(currentXEventsToProcess, xEvent)
			}
		}

		if len(currentXEventsToProcess) == 0 {
			if len(xRootsIDsInCurrentXFrame) > 0 { // Có root nhưng tất cả đã được quyết định trước đó hoặc trong lượt này.
				logger.Info(fmt.Sprintf("DecideClotho: All relevant roots in xFrame %d were already decided (or decided in this run).", xFrame))
			}
		} else {
			logger.Info(fmt.Sprintf("DecideClotho: Found %d undecided roots in xFrame %d to process in this run.", len(currentXEventsToProcess), xFrame))
		}

	nextXEventProcessingLoop:
		for _, xEvent := range currentXEventsToProcess {
			logger.Info(xEvent.Transactions)
			// Nếu xEvent đã được quyết định trong các vòng lặp yFrame trước đó của cùng lượt DecideClotho này, bỏ qua.
			// Điều này quan trọng nếu một xEvent được quyết định bởi một yEvent ở yFrame sớm,
			// chúng ta không muốn xử lý lại nó với các yEvent ở yFrame muộn hơn trong cùng một cuộc gọi DecideClotho.
			if xEvent.ClothoStatus != ClothoUndecided || decidedInThisRun[xEvent.GetEventId()] { // Kiểm tra lại trạng thái thực tế và cờ decidedInThisRun
				continue
			}

			// logger.Info("Processing xEvent for Clotho: ", xEvent.String()) // Log chi tiết nếu cần
			xID := xEvent.GetEventId()
			logger.Info(fmt.Sprintf("DecideClotho: Evaluating xEvent %s (Frame %d)", xID.Short(), xFrame))

			// Tối ưu hóa quan trọng: Chỉ thực hiện bỏ phiếu nếu có yFrame mới hoặc yEvent mới kể từ lần cuối xEvent này được xem xét.
			// Tuy nhiên, với cách tiếp cận "không trạng thái" giữa các lần gọi DecideClotho,
			// việc này khó thực hiện mà không lưu thêm trạng thái.
			// Hiện tại, nó sẽ tính toán lại phiếu bầu từ các yEvent.

			for yFrame := xFrame + 1; yFrame <= maxFrameConsidered; yFrame++ {
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

					// Tối ưu hóa: Nếu yEvent đã bỏ phiếu cho xID này trong một lần xử lý trước đó
					// (trong cùng một cuộc gọi DecideClotho hoặc từ cuộc gọi trước đó mà trạng thái DAG không đổi),
					// và các điều kiện cho phiếu bầu đó không thay đổi, có thể xem xét bỏ qua.
					// Tuy nhiên, "điều kiện không đổi" rất khó xác định một cách đơn giản.

					if round == 1 {
						// Phiếu bầu trực tiếp không thay đổi nếu xEvent và yEvent không đổi.
						// Nhưng chúng ta vẫn cần gọi SetVote để yEvent có thông tin này cho các vòng sau.
						vote := ds.forklessCause(xEvent, yEvent)
						yEvent.SetVote(xID, vote)
						// logger.Debug(fmt.Sprintf("DecideClotho: yEvent %s (F%d) votes %t for xEvent %s (F%d) in round 1", yID.Short(), yFrame, vote, xID.Short(), xFrame))
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
								if voteExists { // Chỉ tính nếu prevRootEvent đã có phiếu cho xID
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
						yEvent.SetVote(xID, yFinalVote) // yEvent ghi nhận phiếu bầu tổng hợp của nó cho xID
						// logger.Debug(fmt.Sprintf("DecideClotho: yEvent %s (F%d) votes %t for xEvent %s (F%d) in round %d (YesStake: %d, NoStake: %d)", yID.Short(), yFrame, yFinalVote, xID.Short(), xFrame, round, yesVotesStake, noVotesStake))

						// Kiểm tra xem xEvent đã đạt QUORUM chưa
						if yesVotesStake >= QUORUM {
							xEvent.SetCandidate(true)
							xEvent.SetClothoStatus(ClothoIsClotho)
							decidedInThisRun[xID] = true // Đánh dấu đã quyết định trong lượt này
							logger.Info(fmt.Sprintf("DecideClotho: DECIDED - Root %s (F%d) IS CLOTHO. Determined by aggregated votes at yEvent %s (F%d). YesStake: %d >= Quorum: %d",
								xID.Short(), xFrame, yID.Short(), yFrame, yesVotesStake, QUORUM))
							// logger.Info("Transactions for decided CLOTHO event: ", xEvent.EventData.Transactions) // Log giao dịch nếu cần
							continue nextXEventProcessingLoop
						}
						if noVotesStake >= QUORUM {
							xEvent.SetCandidate(false)
							xEvent.SetClothoStatus(ClothoIsNotClotho)
							decidedInThisRun[xID] = true // Đánh dấu đã quyết định trong lượt này
							logger.Info(fmt.Sprintf("DecideClotho: DECIDED - Root %s (F%d) IS NOT CLOTHO. Determined by aggregated votes at yEvent %s (F%d). NoStake: %d >= Quorum: %d",
								xID.Short(), xFrame, yID.Short(), yFrame, noVotesStake, QUORUM))
							continue nextXEventProcessingLoop
						}
					}
				} // Kết thúc vòng lặp yID (các root trong yFrame)
			} // Kết thúc vòng lặp yFrame

			// Sau khi lặp qua tất cả các yFrame có thể
			if xEvent.ClothoStatus == ClothoUndecided { // Nếu vẫn chưa quyết định
				logger.Info(fmt.Sprintf("DecideClotho: Root %s (F%d) remains UNDECIDED after checking all y-frames up to %d in this run.", xID.Short(), xFrame, maxFrameConsidered))
			}
		} // Kết thúc vòng lặp xEvent (currentXEventsToProcess)
		logger.Info(fmt.Sprintf("DecideClotho: Finished evaluating undecided roots for xFrame %d", xFrame))

		// Kiểm tra xem tất cả các root trong xFrame hiện tại đã được quyết định chưa
		// (bao gồm cả những root đã được quyết định từ các lượt DecideClotho trước đó)
		allOriginalRootsInXFrameDecided := true
		if len(xRootsIDsInCurrentXFrame) > 0 { // Chỉ kiểm tra nếu frame này có root ban đầu
			for _, rootID := range xRootsIDsInCurrentXFrame {
				rootEvent, exists := ds.events[rootID]
				// Một root được coi là "chưa xử lý xong" nếu nó tồn tại, là root, nhưng vẫn Undecided
				if !exists || !rootEvent.EventData.IsRoot || rootEvent.ClothoStatus == ClothoUndecided {
					allOriginalRootsInXFrameDecided = false
					logger.Info(fmt.Sprintf("DecideClotho: xFrame %d not fully decided. Root %s still UNDECIDED (exists: %t, isRoot: %t, status: %s).",
						xFrame, rootID.Short(), exists, (exists && rootEvent.EventData.IsRoot), rootEvent.ClothoStatus))
					break
				}
			}
		} else { // Nếu không có root nào trong xFrame, coi như frame đó đã "xong" cho việc tiến lastDecidedFrame
			allOriginalRootsInXFrameDecided = true
			logger.Info(fmt.Sprintf("DecideClotho: xFrame %d is empty, considered processed for advancing lastDecidedFrame.", xFrame))
		}

		logger.Info(fmt.Sprintf("DecideClotho: End of xFrame %d processing. All original roots decided status: %t", xFrame, allOriginalRootsInXFrameDecided))

		if allOriginalRootsInXFrameDecided {
			ds.lastDecidedFrame = xFrame
			logger.Info(fmt.Sprintf("DecideClotho: Updated lastDecidedFrame to %d because all roots in this frame are now decided.", ds.lastDecidedFrame))
		} else {
			logger.Info(fmt.Sprintf("DecideClotho: xFrame %d not fully decided. Stopping advancement of lastDecidedFrame. Current lastDecidedFrame: %d.", xFrame, ds.lastDecidedFrame))
			break // Dừng vì không thể tiến xa hơn nếu có frame chưa quyết định
		}
	}
	logger.Info(fmt.Sprintf("DecideClotho: Process finished. Final lastDecidedFrame: %d", ds.lastDecidedFrame))
}

// PruneOldEvents removes events from frames older than oldestFrameToKeep.
// oldestFrameToKeep is the OLDEST frame number that should be RETAINED.
// Events in frames < oldestFrameToKeep will be deleted.
func (ds *DagStore) PruneOldEvents(oldestFrameToKeep uint64) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	logger.Info(fmt.Sprintf("PruneOldEvents: Starting cleanup of events in frames older than %d. Current lastDecidedFrame: %d", oldestFrameToKeep, ds.lastDecidedFrame))

	if oldestFrameToKeep == 0 {
		logger.Warn("PruneOldEvents: oldestFrameToKeep is 0. Skipping pruning to avoid deleting crucial data.")
		return
	}

	// Safety condition: Only allow pruning if oldestFrameToKeep is less than or equal to ds.lastDecidedFrame.
	// This ensures that the last fully decided frame is not accidentally pruned.
	if oldestFrameToKeep > ds.lastDecidedFrame {
		logger.Info(fmt.Sprintf("PruneOldEvents: oldestFrameToKeep (%d) is greater than the last decided frame (%d). Skipping pruning for safety.", oldestFrameToKeep, ds.lastDecidedFrame))
		return
	}

	eventsToDelete := make(map[EventID]struct{}) // Use a map for efficient deletion tracking.

	// 1. Identify events to delete from ds.events.
	for id, event := range ds.events {
		if event.EventData.Frame < oldestFrameToKeep {
			eventsToDelete[id] = struct{}{}
		}
	}

	if len(eventsToDelete) == 0 {
		logger.Info(fmt.Sprintf("PruneOldEvents: No events older than frame %d found to delete.", oldestFrameToKeep))
		return
	}

	logger.Info(fmt.Sprintf("PruneOldEvents: Identified %d events for deletion.", len(eventsToDelete)))

	// 2. Delete identified events from ds.events.
	for id := range eventsToDelete {
		delete(ds.events, id)
	}

	// 3. Clean up ds.rootsByFrame.
	framesProcessedForRoots := make(map[uint64]bool) // To avoid repetitive logging for the same frame.
	for frame := range ds.rootsByFrame {
		if frame < oldestFrameToKeep {
			if !framesProcessedForRoots[frame] {
				logger.Debug(fmt.Sprintf("PruneOldEvents: Removing entry for frame %d from rootsByFrame.", frame))
				framesProcessedForRoots[frame] = true
			}
			delete(ds.rootsByFrame, frame)
		} else {
			// For remaining frames, filter out rootIDs that were deleted.
			var validRoots []EventID
			initialRootCountInFrame := len(ds.rootsByFrame[frame])
			for _, rootID := range ds.rootsByFrame[frame] {
				if _, isDeleted := eventsToDelete[rootID]; !isDeleted {
					validRoots = append(validRoots, rootID)
				}
			}

			if len(validRoots) < initialRootCountInFrame && initialRootCountInFrame > 0 {
				logger.Debug(fmt.Sprintf("PruneOldEvents: Removed %d root(s) from frame %d in rootsByFrame. %d root(s) remaining.", initialRootCountInFrame-len(validRoots), frame, len(validRoots)))
			}

			if len(validRoots) == 0 && initialRootCountInFrame > 0 {
				// If the frame had roots but now all are invalid, delete the frame entry.
				delete(ds.rootsByFrame, frame)
			} else if len(validRoots) > 0 || (len(validRoots) == 0 && initialRootCountInFrame == 0) {
				// Update only if there are valid roots left, or if the frame was already empty.
				ds.rootsByFrame[frame] = validRoots
			}
		}
	}

	// Helper function for safe string slicing (for logging).
	minDisplayHexLen := func(s string, l int) int {
		if len(s) < l {
			return len(s)
		}
		return l
	}

	// 4. Clean up ds.latestEvents.
	// If a creator's latestEvent was deleted, find and update their new latest event.
	creatorsToRecheck := make([]string, 0, len(ds.latestEvents))
	for creatorKey := range ds.latestEvents {
		creatorsToRecheck = append(creatorsToRecheck, creatorKey)
	}

	for _, creatorKey := range creatorsToRecheck {
		currentLatestID, existsInMap := ds.latestEvents[creatorKey]
		if !existsInMap { // Should not happen if iterating keys from the map itself.
			continue
		}

		if _, wasDeleted := eventsToDelete[currentLatestID]; wasDeleted {
			logger.Info(fmt.Sprintf("PruneOldEvents: Latest event %s for creator %s was deleted. Searching for new latest event...",
				currentLatestID.String()[:6], creatorKey[:minDisplayHexLen(creatorKey, 6)]))

			delete(ds.latestEvents, creatorKey) // Remove the outdated entry.

			var newLatestEventForCreator *Event = nil
			// Iterate over the pruned ds.events map to find the new latest.
			for _, event := range ds.events {
				if hex.EncodeToString(event.EventData.Creator) == creatorKey {
					if newLatestEventForCreator == nil || event.EventData.Index > newLatestEventForCreator.EventData.Index {
						newLatestEventForCreator = event
					}
				}
			}

			if newLatestEventForCreator != nil {
				ds.latestEvents[creatorKey] = newLatestEventForCreator.GetEventId()
				logger.Info(fmt.Sprintf("PruneOldEvents: Updated latest event for creator %s to %s (Index: %d, Frame: %d)",
					creatorKey[:minDisplayHexLen(creatorKey, 6)],
					newLatestEventForCreator.GetEventId().String()[:6],
					newLatestEventForCreator.EventData.Index,
					newLatestEventForCreator.EventData.Frame))
			} else {
				logger.Info(fmt.Sprintf("PruneOldEvents: No remaining events found for creator %s after pruning. Their entry in latestEvents is removed.",
					creatorKey[:minDisplayHexLen(creatorKey, 6)]))
			}
		}
	}

	logger.Info(fmt.Sprintf("PruneOldEvents: Finished. Deleted %d events. Remaining events: %d. Frames in rootsByFrame: %d. Entries in latestEvents: %d",
		len(eventsToDelete), len(ds.events), len(ds.rootsByFrame), len(ds.latestEvents)))
}

// GetDecidedRoots returns a map of EventIDs to *Event for roots that have a decided Clotho status.
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

// GetRootStatus returns the Clotho status of a Root event.
func (ds *DagStore) GetRootStatus(rootID EventID) (ClothoStatus, bool) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	event, exists := ds.events[rootID]
	if !exists || !event.EventData.IsRoot {
		return ClothoUndecided, false
	}
	return event.ClothoStatus, true
}

// GetHeightForNode returns the index of the latest event created by the node with the given creator public key hex.
func (ds *DagStore) GetHeightForNode(nodeCreatorPubKeyHex string) (uint64, bool) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	latestEventID, exists := ds.latestEvents[nodeCreatorPubKeyHex]
	if !exists {
		return 0, false
	}

	event, eventExists := ds.events[latestEventID]
	if !eventExists {
		logger.Error(fmt.Sprintf("DagStore.GetHeightForNode: Latest event %s for creator %s not found in events map.", latestEventID.String(), nodeCreatorPubKeyHex))
		return 0, false
	}
	return event.EventData.Index, true
}

// GetInDegreeForNode returns the "in-degree" for the latest event of the node with the given creator public key hex.
// In-degree is defined as the number of distinct creators (excluding the event's own creator)
// whose events are OtherParents of the event being considered.
func (ds *DagStore) GetInDegreeForNode(nodeCreatorPubKeyHex string) (uint64, bool) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	latestEventID, exists := ds.latestEvents[nodeCreatorPubKeyHex]
	if !exists {
		return 0, false
	}

	event, eventExists := ds.events[latestEventID]
	if !eventExists {
		logger.Error(fmt.Sprintf("DagStore.GetInDegreeForNode: Latest event %s for creator %s not found in DagStore for in-degree calculation.", latestEventID.String(), nodeCreatorPubKeyHex))
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
			logger.Warn(fmt.Sprintf("DagStore.GetInDegreeForNode: Parent event %s not found when calculating in-degree for %s's event %s",
				parentEventID.String()[:6], nodeCreatorPubKeyHex[:6], latestEventID.String()[:6]))
			continue
		}
		parentCreatorHex := hex.EncodeToString(parentEvent.EventData.Creator)
		// Only count other parents from distinct creators.
		// The definition of in-degree usually refers to distinct *other* creators.
		if parentCreatorHex != nodeCreatorPubKeyHex {
			distinctCreators[parentCreatorHex] = struct{}{}
		}
	}
	return uint64(len(distinctCreators)), true
}

// GetAllEventsSnapshot returns a slice containing a snapshot of all events in the store.
// Events are sorted by Frame, then by Timestamp, then by EventID to ensure a deterministic order.
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

// GetLatestEventsMapSnapshot returns a copy of the latestEvents map.
func (ds *DagStore) GetLatestEventsMapSnapshot() map[string]EventID {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	latestCopy := make(map[string]EventID, len(ds.latestEvents))
	for k, v := range ds.latestEvents {
		latestCopy[k] = v
	}
	return latestCopy
}

// GetEventsByCreatorSinceIndex returns events from a specific creator
// starting from (but not including) a given startIndex.
// The returned events are sorted by their index.
func (ds *DagStore) GetEventsByCreatorSinceIndex(creatorPubKeyHex string, startIndex uint64) []*Event {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	var result []*Event
	creatorBytes, err := hex.DecodeString(creatorPubKeyHex)
	if err != nil {
		logger.Error(fmt.Sprintf("DagStore.GetEventsByCreatorSinceIndex: Invalid creator public key hex '%s': %v", creatorPubKeyHex, err))
		return result // Return empty slice on error.
	}

	var tempEvents []*Event
	for _, event := range ds.events {
		if bytes.Equal(event.EventData.Creator, creatorBytes) && event.EventData.Index > startIndex {
			tempEvents = append(tempEvents, event)
		}
	}

	sort.Slice(tempEvents, func(i, j int) bool {
		return tempEvents[i].EventData.Index < tempEvents[j].EventData.Index
	})
	return tempEvents
}
