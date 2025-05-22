package dag

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/blockchain/consensus/logger" // Custom logger for the consensus package.
)

// QUORUM defines the voting threshold (by stake) required to decide Clotho status.
// For example, this could be 2/3 of the total network stake + 1.
// This value should be set based on the network's total stake distribution.
// The current value of 2 is likely a placeholder for a small test network.
const QUORUM uint64 = 30 //

// Define the round at which a decision for xEvent is made based on yEvents.
// For example, if K_DECISION_ROUND = 2, yEvents in xFrame+2 will decide xEvent.
const K_DECISION_ROUND uint64 = 2

// DagStore manages Event blocks within the DAG and implements Clotho Selection logic.
type DagStore struct {
	// events stores all events, keyed by their EventID (hash).
	events map[EventID]*Event //
	// latestEvents maps the hex public key of a creator to their latest EventID.
	latestEvents map[string]EventID //
	// rootsByFrame maps a frame number to a slice of EventIDs of Root events in that frame.
	rootsByFrame map[uint64][]EventID //
	// lastDecidedFrame is the last frame for which Clotho has been decided for all its roots.
	lastDecidedFrame uint64 //
	// mu is a RWMutex to synchronize access to DagStore's maps and fields.
	mu sync.RWMutex //
	// stake maps the hex public key of a creator (validator) to their stake amount.
	stake map[string]uint64 //
}

// NewDagStore creates a new instance of DagStore.
// initialStake is a map from a validator's hex public key to their stake amount.
func NewDagStore(initialStake map[string]uint64) *DagStore { //
	// Copy initialStake to prevent external modification.
	stakeMap := make(map[string]uint64, len(initialStake)) //
	for pubKeyHex, s := range initialStake {               //
		stakeMap[pubKeyHex] = s //
	}

	return &DagStore{ //
		events:           make(map[EventID]*Event),   //
		latestEvents:     make(map[string]EventID),   //
		rootsByFrame:     make(map[uint64][]EventID), //
		lastDecidedFrame: 0,                          //
		mu:               sync.RWMutex{},             //
		stake:            stakeMap,                   //
	}
}

// AddEvent adds a new event to the DagStore.
// It performs basic validity checks such as parent existence, index, and uniqueness.
func (ds *DagStore) AddEvent(event *Event) error { //
	if event == nil { //
		return errors.New("cannot add nil event") //
	}

	eventID := event.GetEventId() //
	if eventID.IsZero() {         //
		return errors.New("event hash is zero, cannot add to DagStore") //
	}

	ds.mu.Lock()         //
	defer ds.mu.Unlock() //

	if _, exists := ds.events[eventID]; exists { //
		return nil // Event already exists, no need to add again. //
	}

	creatorKey := hex.EncodeToString(event.EventData.Creator) //

	currentLatestEventID, creatorHasPrevious := ds.latestEvents[creatorKey] //
	if creatorHasPrevious {                                                 //
		// Check self-parent for non-first events of a creator.
		if event.EventData.Index > 1 && event.EventData.SelfParent.IsZero() { //
			return fmt.Errorf("event %s by %s (Index %d) has zero self parent but creator has previous events", //
				eventID.String()[:6], creatorKey[:6], event.EventData.Index) //
		}
		if event.EventData.Index > 1 && event.EventData.SelfParent != currentLatestEventID { //
			var expectedPrevIndex uint64                                    //
			if selfParentEvent, ok := ds.events[currentLatestEventID]; ok { //
				expectedPrevIndex = selfParentEvent.EventData.Index //
			}
			return fmt.Errorf("invalid self parent for event %s by %s (Index %d): expected %s (Index %d), got %s (Index %d expected based on current)", //
				eventID.String()[:6], creatorKey[:6], event.EventData.Index, //
				currentLatestEventID.String()[:6], expectedPrevIndex, //
				event.EventData.SelfParent.String()[:6], event.EventData.Index-1) //
		}
		if prevEvent, ok := ds.events[currentLatestEventID]; ok { //
			if event.EventData.Index != prevEvent.EventData.Index+1 { //
				return fmt.Errorf("invalid index for event %s by %s: expected %d, got %d", //
					eventID.String()[:6], creatorKey[:6], prevEvent.EventData.Index+1, event.EventData.Index) //
			}
		}

	} else { // This is the creator's first event. //
		if !event.EventData.SelfParent.IsZero() { //
			return fmt.Errorf("first event %s of creator %s must have zero self parent, got %s", eventID.String()[:6], creatorKey[:6], event.EventData.SelfParent.String()[:6]) //
		}
		if event.EventData.Index != 1 { //
			return fmt.Errorf("first event %s of creator %s must have index 1, got %d", eventID.String()[:6], creatorKey[:6], event.EventData.Index) //
		}
		if _, exists := ds.stake[creatorKey]; !exists { //
			logger.Warn(fmt.Sprintf("DagStore.AddEvent: Creator %s (%s) is adding their first event but has no registered stake.", creatorKey[:6], eventID.String()[:6])) //
		}
	}

	for _, otherParentID := range event.EventData.OtherParents { //
		if otherParentID.IsZero() { //
			continue //
		}
		if _, exists := ds.events[otherParentID]; !exists { //
			logger.Warn(fmt.Sprintf("DagStore.AddEvent: Other parent %s for event %s does not exist in DagStore. Event might be an orphan temporarily.", otherParentID.String()[:6], eventID.String()[:6])) //
		}
	}

	ds.events[eventID] = event //

	// Update latestEvents only if the new event has a higher index.
	if !creatorHasPrevious || (ds.events[currentLatestEventID] != nil && event.EventData.Index > ds.events[currentLatestEventID].EventData.Index) { //
		ds.latestEvents[creatorKey] = eventID //
	}

	if event.EventData.IsRoot { //
		// Check and add to rootsByFrame if not already present.
		isAlreadyRoot := false                                                  //
		for _, existingRootID := range ds.rootsByFrame[event.EventData.Frame] { //
			if existingRootID == eventID { //
				isAlreadyRoot = true //
				break                //
			}
		}
		if !isAlreadyRoot { //
			ds.rootsByFrame[event.EventData.Frame] = append(ds.rootsByFrame[event.EventData.Frame], eventID) //
			// Keep the list of roots consistently sorted.
			sort.Slice(ds.rootsByFrame[event.EventData.Frame], func(i, j int) bool { //
				return ds.rootsByFrame[event.EventData.Frame][i].String() < ds.rootsByFrame[event.EventData.Frame][j].String() //
			})
		}
	}
	logger.Info(fmt.Sprintf("DagStore.AddEvent: Successfully added event: %s (Creator: %s, Index: %d, Frame: %d, IsRoot: %t)", //
		eventID.String()[:6], creatorKey[:6], event.EventData.Index, event.EventData.Frame, event.EventData.IsRoot)) //
	return nil //
}

// GetEvent retrieves an event from the store by its EventID.
func (ds *DagStore) GetEvent(id EventID) (*Event, bool) { //
	ds.mu.RLock()                  //
	defer ds.mu.RUnlock()          //
	event, exists := ds.events[id] //
	return event, exists           //
}

// EventExists checks if an event exists in the store by its EventID.
func (ds *DagStore) EventExists(id EventID) bool { //
	ds.mu.RLock()              //
	defer ds.mu.RUnlock()      //
	_, exists := ds.events[id] //
	return exists              //
}

// GetLatestEventIDByCreatorPubKeyHex returns the latest EventID created by a specific creator.
func (ds *DagStore) GetLatestEventIDByCreatorPubKeyHex(creatorPubKeyHex string) (EventID, bool) { //
	ds.mu.RLock()                                        //
	defer ds.mu.RUnlock()                                //
	eventID, exists := ds.latestEvents[creatorPubKeyHex] //
	return eventID, exists                               //
}

// GetRoots returns a slice containing EventIDs of all root events in a specific frame.
// Returns a copy to prevent external modification.
func (ds *DagStore) GetRoots(frame uint64) []EventID { //
	ds.mu.RLock()                           //
	defer ds.mu.RUnlock()                   //
	roots, exists := ds.rootsByFrame[frame] //
	if !exists || len(roots) == 0 {         //
		return []EventID{} //
	}
	rootsCopy := make([]EventID, len(roots)) //
	copy(rootsCopy, roots)                   //
	return rootsCopy                         //
}

// GetLastDecidedFrame returns the last frame for which Clotho has been decided.
func (ds *DagStore) GetLastDecidedFrame() uint64 { //
	ds.mu.RLock()              //
	defer ds.mu.RUnlock()      //
	return ds.lastDecidedFrame //
}

// getStakeLocked retrieves the stake of a validator based on their hex public key.
// This method requires ds.mu to be held (read or write lock).
func (ds *DagStore) getStakeLocked(creatorPubKeyHex string) uint64 { //
	stake, exists := ds.stake[creatorPubKeyHex] //
	if !exists {                                //
		return 0 //
	}
	return stake //
}

// isAncestorLocked checks if event ancestorID is an ancestor of event descendantID.
// This method requires ds.mu to be held (read or write lock).
func (ds *DagStore) isAncestorLocked(ancestorID, descendantID EventID) bool { //
	if ancestorID == descendantID { //
		return true //
	}
	if ancestorID.IsZero() || descendantID.IsZero() { //
		return false //
	}

	queue := []EventID{descendantID}  //
	visited := make(map[EventID]bool) //
	visited[descendantID] = true      //

	for len(queue) > 0 { //
		currentID := queue[0] //
		queue = queue[1:]     //

		currentEvent, exists := ds.events[currentID] //
		if !exists {                                 //
			logger.Warn(fmt.Sprintf("DagStore.isAncestorLocked: Event %s not found in store during ancestry check for descendant %s from ancestor %s", //
				currentID.String()[:6], descendantID.String()[:6], ancestorID.String()[:6])) //
			continue //
		}

		var parents []EventID                            //
		if !currentEvent.EventData.SelfParent.IsZero() { //
			parents = append(parents, currentEvent.EventData.SelfParent) //
		}
		if len(currentEvent.EventData.OtherParents) > 0 { //
			parents = append(parents, currentEvent.EventData.OtherParents...) //
		}

		for _, parentID := range parents { //
			if parentID.IsZero() { //
				continue //
			}
			if parentID == ancestorID { //
				return true //
			}
			if !visited[parentID] { //
				visited[parentID] = true        //
				queue = append(queue, parentID) //
			}
		}
	}
	return false //
}

// IsAncestor is the public version of isAncestorLocked, managing the lock.
func (ds *DagStore) IsAncestor(ancestorID, descendantID EventID) bool { //
	ds.mu.RLock()                                        //
	defer ds.mu.RUnlock()                                //
	return ds.isAncestorLocked(ancestorID, descendantID) //
}

// forklessCause checks if event x "forkless causes" event y.
// This means x is an ancestor of y, and y does not see any forks related to x.
// This method requires ds.mu to be held for isAncestorLocked.
// Note: The current implementation is a simplified alias for isAncestorLocked.
// A full forklessCause implementation might be more complex.
func (ds *DagStore) forklessCause(x, y *Event) bool { //
	if x == nil || y == nil { //
		return false //
	}
	return ds.isAncestorLocked(x.GetEventId(), y.GetEventId()) //
}

// DecideClotho implements the Clotho selection algorithm.
// This algorithm determines the Clotho status (IS-CLOTHO / IS-NOT-CLOTHO) for Root events.
func (ds *DagStore) DecideClotho() { //
	ds.mu.Lock()                                                   //
	defer ds.mu.Unlock()                                           //
	logger.Info("DecideClotho: Starting Clotho decision process.") //

	startFrame := ds.lastDecidedFrame + 1 //
	var maxFrameWithRoots uint64 = 0      //
	for frame := range ds.rootsByFrame {  //
		if frame > maxFrameWithRoots { //
			maxFrameWithRoots = frame //
		}
	}
	logger.Info(fmt.Sprintf("DecideClotho: StartFrame=%d, MaxFrameWithRoots=%d, CurrentLastDecidedFrame=%d", startFrame, maxFrameWithRoots, ds.lastDecidedFrame)) //

	if startFrame > maxFrameWithRoots { //
		logger.Info("DecideClotho: No new frames to process for xEvents.") //
		return                                                             //
	}

	for xFrame := startFrame; xFrame <= maxFrameWithRoots; xFrame++ { //
		logger.Info(fmt.Sprintf("DecideClotho: Processing xFrame = %d", xFrame)) //
		xRootsIDsInCurrentXFrame := ds.rootsByFrame[xFrame]                      //
		if len(xRootsIDsInCurrentXFrame) == 0 {                                  //
			logger.Info(fmt.Sprintf("DecideClotho: No roots found in xFrame %d.", xFrame)) //
		}

		allRootsInXFrameDecided := true // Assume true until proven otherwise //

		for _, xID := range xRootsIDsInCurrentXFrame { //
			xEvent, exists := ds.events[xID]                                                   //
			if !exists || !xEvent.EventData.IsRoot || xEvent.ClothoStatus != ClothoUndecided { //
				if exists && xEvent.EventData.IsRoot && xEvent.ClothoStatus != ClothoUndecided { //
					// Already decided, skip but don't break advancement of lastDecidedFrame
				} else {
					// If event doesn't exist, isn't a root, or status is problematic,
					// this frame might not be fully decidable yet.
					allRootsInXFrameDecided = false //
				}
				continue //
			}

			logger.Info(fmt.Sprintf("DecideClotho: Evaluating xEvent %s (Frame %d)", xID.Short(), xFrame)) //

			// Determine votes from yEvents in xFrame+1 for xEvent
			// This is for round 1 voting (yEvents in xFrame + 1 vote on xEvents in xFrame)
			yFrameForRound1Voting := xFrame + 1             //
			if yFrameForRound1Voting <= maxFrameWithRoots { //
				yRootsForRound1Voting := ds.rootsByFrame[yFrameForRound1Voting] //
				for _, yIDForRound1 := range yRootsForRound1Voting {            //
					yEventForRound1, yExists := ds.events[yIDForRound1] //
					if yExists && yEventForRound1.EventData.IsRoot {    //
						vote := ds.forklessCause(xEvent, yEventForRound1) //
						yEventForRound1.SetVote(xID, vote)                //
						// logger.Debug(fmt.Sprintf("DecideClotho: yEvent %s (F%d) votes %t for xEvent %s (F%d) in round 1 (yFrame calculation)", yIDForRound1.Short(), yFrameForRound1Voting, vote, xID.Short(), xFrame)) //
					}
				}
			}

			// Now, attempt to decide xEvent based on votes from yEvents in the decision frame
			decisionFrameForX := xFrame + K_DECISION_ROUND //
			if decisionFrameForX > maxFrameWithRoots {     //
				logger.Info(fmt.Sprintf("DecideClotho: Not enough subsequent frames to decide xEvent %s (F%d). Needs up to F%d, max available F%d.", xID.Short(), xFrame, decisionFrameForX, maxFrameWithRoots)) //
				allRootsInXFrameDecided = false                                                                                                                                                                  //
				continue                                                                                                                                                                                         // // Move to next xEvent
			}

			accumulatedYesStakeForX := uint64(0)                        //
			accumulatedNoStakeForX := uint64(0)                         //
			yRootsInDecisionFrame := ds.rootsByFrame[decisionFrameForX] //

			if len(yRootsInDecisionFrame) == 0 {
				logger.Info(fmt.Sprintf("DecideClotho: No roots in decision frame F%d to decide xEvent %s (F%d).", decisionFrameForX, xID.Short(), xFrame)) //
				allRootsInXFrameDecided = false                                                                                                             //
				continue                                                                                                                                    //
			}

			logger.Info(fmt.Sprintf("DecideClotho: xEvent %s (F%d) attempting decision with %d roots from F%d.", xID.Short(), xFrame, len(yRootsInDecisionFrame), decisionFrameForX)) //

			for _, yDecisionID := range yRootsInDecisionFrame { //
				yDecisionEvent, yDecisionExists := ds.events[yDecisionID] //
				if !yDecisionExists || !yDecisionEvent.EventData.IsRoot { //
					continue //
				}

				// yDecisionEvent (in frame xFrame+K_DECISION_ROUND) needs to determine its vote for xEvent.
				// Its vote is based on votes from prevVoters (in frame xFrame+K_DECISION_ROUND-1) for xEvent.
				prevVotersFrame := decisionFrameForX - 1          // This is yFrameForRound1Voting //
				prevVotersIDs := ds.rootsByFrame[prevVotersFrame] //
				yesStakeForYVote := uint64(0)                     //
				noStakeForYVote := uint64(0)                      //

				for _, prevRootID := range prevVotersIDs { //
					prevRootEvent, prevRootExists := ds.events[prevRootID]  //
					if !prevRootExists || !prevRootEvent.EventData.IsRoot { //
						continue //
					}

					if ds.forklessCause(prevRootEvent, yDecisionEvent) { // yDecisionEvent must see prevRootEvent //
						prevVoteForX, voteExists := prevRootEvent.GetVote(xID) // prevRootEvent's (round 1) vote for xID //
						if voteExists {                                        //
							prevVoterStake := ds.getStakeLocked(hex.EncodeToString(prevRootEvent.EventData.Creator)) //
							if prevVoteForX {                                                                        //
								yesStakeForYVote += prevVoterStake //
							} else {
								noStakeForYVote += prevVoterStake //
							}
						}
					}
				}
				// yDecisionEvent determines its final vote for xEvent
				yFinalVoteForX := (yesStakeForYVote >= noStakeForYVote) //
				yDecisionEvent.SetVote(xID, yFinalVoteForX)             // Store y's final vote on x for transparency/future rounds if K_DECISION_ROUND is large //
				// logger.Debug(fmt.Sprintf("DecideClotho: yDecisionEvent %s (F%d) votes %t for xEvent %s (F%d). Based on (yesStake: %d, noStake: %d from F%d voters)",yDecisionID.Short(), decisionFrameForX, yFinalVoteForX, xID.Short(), xFrame, yesStakeForYVote, noStakeForYVote, prevVotersFrame)) //

				// Now, use yDecisionEvent's vote (yFinalVoteForX) and its stake for xEvent's decision
				yDecisionEventStake := ds.getStakeLocked(hex.EncodeToString(yDecisionEvent.EventData.Creator)) //
				if yFinalVoteForX {                                                                            //
					accumulatedYesStakeForX += yDecisionEventStake //
				} else {
					accumulatedNoStakeForX += yDecisionEventStake //
				}
			} // End loop over yRootsInDecisionFrame

			// After checking all yEvents in the decision frame for xEvent
			logger.Info(fmt.Sprintf("DecideClotho: For xEvent %s (F%d), AccumulatedYesStake: %d, AccumulatedNoStake: %d, QUORUM: %d from F%d voters", xID.Short(), xFrame, accumulatedYesStakeForX, accumulatedNoStakeForX, QUORUM, decisionFrameForX)) //

			if accumulatedYesStakeForX >= QUORUM { //
				xEvent.SetCandidate(true)                                                                                              //
				xEvent.SetClothoStatus(ClothoIsClotho)                                                                                 //
				logger.Error(xEvent.Transactions)                                                                                      //
				logger.Info(fmt.Sprintf("DecideClotho: DECIDED - Root %s (F%d) IS CLOTHO. YesStake: %d >= Quorum: %d from F%d voters", //
					xID.Short(), xFrame, accumulatedYesStakeForX, QUORUM, decisionFrameForX)) //
			} else if accumulatedNoStakeForX >= QUORUM { //
				xEvent.SetCandidate(false)                                                                                                //
				xEvent.SetClothoStatus(ClothoIsNotClotho)                                                                                 //
				logger.Info(fmt.Sprintf("DecideClotho: DECIDED - Root %s (F%d) IS NOT CLOTHO. NoStake: %d >= Quorum: %d from F%d voters", //
					xID.Short(), xFrame, accumulatedNoStakeForX, QUORUM, decisionFrameForX)) //
			} else {
				logger.Info(fmt.Sprintf("DecideClotho: Root %s (F%d) remains UNDECIDED. Not enough stake for quorum from F%d voters. (Yes: %d, No: %d, Quorum: %d)", xID.Short(), xFrame, decisionFrameForX, accumulatedYesStakeForX, accumulatedNoStakeForX, QUORUM)) //
				allRootsInXFrameDecided = false                                                                                                                                                                                                                        //
			}
		} // End loop for xEvent in xRootsIDsInCurrentXFrame

		logger.Info(fmt.Sprintf("DecideClotho: Finished evaluating roots for xFrame %d. All decided in this frame: %t", xFrame, allRootsInXFrameDecided)) //

		if allRootsInXFrameDecided && len(xRootsIDsInCurrentXFrame) > 0 { // Ensure frame was not empty //
			ds.lastDecidedFrame = xFrame                                                                                                                   //
			logger.Info(fmt.Sprintf("DecideClotho: Updated lastDecidedFrame to %d because all roots in this frame are now decided.", ds.lastDecidedFrame)) //
		} else if len(xRootsIDsInCurrentXFrame) == 0 && xFrame <= maxFrameWithRoots { // Empty frame, can advance if not stuck //
			// Only advance lastDecidedFrame for an empty frame if it's not breaking a sequence of undecided non-empty frames.
			// This means if previous frames were decided, we can skip over empty ones.
			if ds.lastDecidedFrame == xFrame-1 { //
				ds.lastDecidedFrame = xFrame                                                                                  //
				logger.Info(fmt.Sprintf("DecideClotho: Advanced lastDecidedFrame to %d (empty frame).", ds.lastDecidedFrame)) //
			} else {
				logger.Info(fmt.Sprintf("DecideClotho: Empty xFrame %d. lastDecidedFrame %d. Not advancing.", xFrame, ds.lastDecidedFrame)) //
				break                                                                                                                       // // Stop if there's a gap caused by a previous non-empty undecided frame
			}
		} else {
			logger.Info(fmt.Sprintf("DecideClotho: xFrame %d not fully decided or empty. Stopping advancement of lastDecidedFrame. Current lastDecidedFrame: %d.", xFrame, ds.lastDecidedFrame)) //
			break                                                                                                                                                                                // Dừng vì không thể tiến xa hơn nếu có frame chưa quyết định //
		}
	} // End loop for xFrame
	logger.Info(fmt.Sprintf("DecideClotho: Process finished. Final lastDecidedFrame: %d", ds.lastDecidedFrame)) //
}

// PruneOldEvents removes events from frames older than oldestFrameToKeep.
// oldestFrameToKeep is the OLDEST frame number that should be RETAINED.
// Events in frames < oldestFrameToKeep will be deleted.
func (ds *DagStore) PruneOldEvents(oldestFrameToKeep uint64) { //
	ds.mu.Lock()         //
	defer ds.mu.Unlock() //

	logger.Info(fmt.Sprintf("PruneOldEvents: Starting cleanup of events in frames older than %d. Current lastDecidedFrame: %d", oldestFrameToKeep, ds.lastDecidedFrame)) //

	if oldestFrameToKeep == 0 { //
		logger.Warn("PruneOldEvents: oldestFrameToKeep is 0. Skipping pruning to avoid deleting crucial data.") //
		return                                                                                                  //
	}

	// Safety condition: Only allow pruning if oldestFrameToKeep is less than or equal to ds.lastDecidedFrame.
	// This ensures that the last fully decided frame is not accidentally pruned.
	if oldestFrameToKeep > ds.lastDecidedFrame { //
		logger.Info(fmt.Sprintf("PruneOldEvents: oldestFrameToKeep (%d) is greater than the last decided frame (%d). Skipping pruning for safety.", oldestFrameToKeep, ds.lastDecidedFrame)) //
		return                                                                                                                                                                               //
	}

	eventsToDelete := make(map[EventID]struct{}) // Use a map for efficient deletion tracking. //

	// 1. Identify events to delete from ds.events.
	for id, event := range ds.events { //
		if event.EventData.Frame < oldestFrameToKeep { //
			eventsToDelete[id] = struct{}{} //
		}
	}

	if len(eventsToDelete) == 0 { //
		logger.Info(fmt.Sprintf("PruneOldEvents: No events older than frame %d found to delete.", oldestFrameToKeep)) //
		return                                                                                                        //
	}

	logger.Info(fmt.Sprintf("PruneOldEvents: Identified %d events for deletion.", len(eventsToDelete))) //

	// 2. Delete identified events from ds.events.
	for id := range eventsToDelete { //
		delete(ds.events, id) //
	}

	// 3. Clean up ds.rootsByFrame.
	framesProcessedForRoots := make(map[uint64]bool) // To avoid repetitive logging for the same frame. //
	for frame := range ds.rootsByFrame {             //
		if frame < oldestFrameToKeep { //
			if !framesProcessedForRoots[frame] { //
				logger.Debug(fmt.Sprintf("PruneOldEvents: Removing entry for frame %d from rootsByFrame.", frame)) //
				framesProcessedForRoots[frame] = true                                                              //
			}
			delete(ds.rootsByFrame, frame) //
		} else {
			// For remaining frames, filter out rootIDs that were deleted.
			var validRoots []EventID                               //
			initialRootCountInFrame := len(ds.rootsByFrame[frame]) //
			for _, rootID := range ds.rootsByFrame[frame] {        //
				if _, isDeleted := eventsToDelete[rootID]; !isDeleted { //
					validRoots = append(validRoots, rootID) //
				}
			}

			if len(validRoots) < initialRootCountInFrame && initialRootCountInFrame > 0 { //
				logger.Debug(fmt.Sprintf("PruneOldEvents: Removed %d root(s) from frame %d in rootsByFrame. %d root(s) remaining.", initialRootCountInFrame-len(validRoots), frame, len(validRoots))) //
			}

			if len(validRoots) == 0 && initialRootCountInFrame > 0 { //
				// If the frame had roots but now all are invalid, delete the frame entry.
				delete(ds.rootsByFrame, frame) //
			} else if len(validRoots) > 0 || (len(validRoots) == 0 && initialRootCountInFrame == 0) { //
				// Update only if there are valid roots left, or if the frame was already empty.
				ds.rootsByFrame[frame] = validRoots //
			}
		}
	}

	// Helper function for safe string slicing (for logging).
	minDisplayHexLen := func(s string, l int) int { //
		if len(s) < l { //
			return len(s) //
		}
		return l //
	}

	// 4. Clean up ds.latestEvents.
	// If a creator's latestEvent was deleted, find and update their new latest event.
	creatorsToRecheck := make([]string, 0, len(ds.latestEvents)) //
	for creatorKey := range ds.latestEvents {                    //
		creatorsToRecheck = append(creatorsToRecheck, creatorKey) //
	}

	for _, creatorKey := range creatorsToRecheck { //
		currentLatestID, existsInMap := ds.latestEvents[creatorKey] //
		if !existsInMap {                                           // Should not happen if iterating keys from the map itself. //
			continue //
		}

		if _, wasDeleted := eventsToDelete[currentLatestID]; wasDeleted { //
			logger.Info(fmt.Sprintf("PruneOldEvents: Latest event %s for creator %s was deleted. Searching for new latest event...", //
				currentLatestID.String()[:6], creatorKey[:minDisplayHexLen(creatorKey, 6)])) //

			delete(ds.latestEvents, creatorKey) // Remove the outdated entry. //

			var newLatestEventForCreator *Event = nil //
			// Iterate over the pruned ds.events map to find the new latest.
			for _, event := range ds.events { //
				if hex.EncodeToString(event.EventData.Creator) == creatorKey { //
					if newLatestEventForCreator == nil || event.EventData.Index > newLatestEventForCreator.EventData.Index { //
						newLatestEventForCreator = event //
					}
				}
			}

			if newLatestEventForCreator != nil { //
				ds.latestEvents[creatorKey] = newLatestEventForCreator.GetEventId()                                                                                                                                                                                                                                //
				logger.Info(fmt.Sprintf("PruneOldEvents: Updated latest event for creator %s to %s (Index: %d, Frame: %d)", creatorKey[:minDisplayHexLen(creatorKey, 6)], newLatestEventForCreator.GetEventId().String()[:6], newLatestEventForCreator.EventData.Index, newLatestEventForCreator.EventData.Frame)) //
			} else {
				logger.Info(fmt.Sprintf("PruneOldEvents: No remaining events found for creator %s after pruning. Their entry in latestEvents is removed.", //
					creatorKey[:minDisplayHexLen(creatorKey, 6)])) //
			}
		}
	}

	logger.Info(fmt.Sprintf("PruneOldEvents: Finished. Deleted %d events. Remaining events: %d. Frames in rootsByFrame: %d. Entries in latestEvents: %d", //
		len(eventsToDelete), len(ds.events), len(ds.rootsByFrame), len(ds.latestEvents))) //
}

// GetDecidedRoots returns a map of EventIDs to *Event for roots that have a decided Clotho status.
func (ds *DagStore) GetDecidedRoots() map[EventID]*Event { //
	ds.mu.RLock()         //
	defer ds.mu.RUnlock() //

	decided := make(map[EventID]*Event) //
	for id, event := range ds.events {  //
		if event.EventData.IsRoot && event.ClothoStatus != ClothoUndecided { //
			decided[id] = event //
		}
	}
	return decided //
}

// GetRootStatus returns the Clotho status of a Root event.
func (ds *DagStore) GetRootStatus(rootID EventID) (ClothoStatus, bool) { //
	ds.mu.RLock()         //
	defer ds.mu.RUnlock() //

	event, exists := ds.events[rootID]      //
	if !exists || !event.EventData.IsRoot { //
		return ClothoUndecided, false //
	}
	return event.ClothoStatus, true //
}

// GetHeightForNode returns the index of the latest event created by the node with the given creator public key hex.
func (ds *DagStore) GetHeightForNode(nodeCreatorPubKeyHex string) (uint64, bool) { //
	ds.mu.RLock()         //
	defer ds.mu.RUnlock() //

	latestEventID, exists := ds.latestEvents[nodeCreatorPubKeyHex] //
	if !exists {                                                   //
		return 0, false //
	}

	event, eventExists := ds.events[latestEventID] //
	if !eventExists {                              //
		logger.Error(fmt.Sprintf("DagStore.GetHeightForNode: Latest event %s for creator %s not found in events map.", latestEventID.String(), nodeCreatorPubKeyHex)) //
		return 0, false                                                                                                                                               //
	}
	return event.EventData.Index, true //
}

// GetInDegreeForNode returns the "in-degree" for the latest event of the node with the given creator public key hex.
// In-degree is defined as the number of distinct creators (excluding the event's own creator)
// whose events are OtherParents of the event being considered.
func (ds *DagStore) GetInDegreeForNode(nodeCreatorPubKeyHex string) (uint64, bool) { //
	ds.mu.RLock()         //
	defer ds.mu.RUnlock() //

	latestEventID, exists := ds.latestEvents[nodeCreatorPubKeyHex] //
	if !exists {                                                   //
		return 0, false //
	}

	event, eventExists := ds.events[latestEventID] //
	if !eventExists {                              //
		logger.Error(fmt.Sprintf("DagStore.GetInDegreeForNode: Latest event %s for creator %s not found in DagStore for in-degree calculation.", latestEventID.String(), nodeCreatorPubKeyHex)) //
		return 0, false                                                                                                                                                                         //
	}

	if len(event.EventData.OtherParents) == 0 { //
		return 0, true //
	}

	distinctCreators := make(map[string]struct{})                //
	for _, parentEventID := range event.EventData.OtherParents { //
		if parentEventID.IsZero() { //
			continue //
		}
		parentEvent, parentExists := ds.events[parentEventID] //
		if !parentExists {                                    //
			logger.Warn(fmt.Sprintf("DagStore.GetInDegreeForNode: Parent event %s not found when calculating in-degree for %s's event %s", //
				parentEventID.String()[:6], nodeCreatorPubKeyHex[:6], latestEventID.String()[:6])) //
			continue //
		}
		parentCreatorHex := hex.EncodeToString(parentEvent.EventData.Creator) //
		// Only count other parents from distinct creators.
		// The definition of in-degree usually refers to distinct *other* creators.
		if parentCreatorHex != nodeCreatorPubKeyHex { //
			distinctCreators[parentCreatorHex] = struct{}{} //
		}
	}
	return uint64(len(distinctCreators)), true //
}

// GetAllEventsSnapshot returns a slice containing a snapshot of all events in the store.
// Events are sorted by Frame, then by Timestamp, then by EventID to ensure a deterministic order.
func (ds *DagStore) GetAllEventsSnapshot() []*Event { //
	ds.mu.RLock()                               //
	defer ds.mu.RUnlock()                       //
	events := make([]*Event, 0, len(ds.events)) //
	for _, event := range ds.events {           //
		events = append(events, event) //
	}
	sort.Slice(events, func(i, j int) bool { //
		if events[i].EventData.Frame != events[j].EventData.Frame { //
			return events[i].EventData.Frame < events[j].EventData.Frame //
		}
		if events[i].EventData.Timestamp != events[j].EventData.Timestamp { //
			return events[i].EventData.Timestamp < events[j].EventData.Timestamp //
		}
		return events[i].GetEventId().String() < events[j].GetEventId().String() //
	})
	return events //
}

// GetLatestEventsMapSnapshot returns a copy of the latestEvents map.
func (ds *DagStore) GetLatestEventsMapSnapshot() map[string]EventID { //
	ds.mu.RLock()                                                //
	defer ds.mu.RUnlock()                                        //
	latestCopy := make(map[string]EventID, len(ds.latestEvents)) //
	for k, v := range ds.latestEvents {                          //
		latestCopy[k] = v //
	}
	return latestCopy //
}

// GetEventsByCreatorSinceIndex returns events from a specific creator
// starting from (but not including) a given startIndex.
// The returned events are sorted by their index.
func (ds *DagStore) GetEventsByCreatorSinceIndex(creatorPubKeyHex string, startIndex uint64) []*Event { //
	ds.mu.RLock()         //
	defer ds.mu.RUnlock() //

	var result []*Event                                     //
	creatorBytes, err := hex.DecodeString(creatorPubKeyHex) //
	if err != nil {                                         //
		logger.Error(fmt.Sprintf("DagStore.GetEventsByCreatorSinceIndex: Invalid creator public key hex '%s': %v", creatorPubKeyHex, err)) //
		return result                                                                                                                      // Return empty slice on error. //
	}

	var tempEvents []*Event           //
	for _, event := range ds.events { //
		if bytes.Equal(event.EventData.Creator, creatorBytes) && event.EventData.Index > startIndex { //
			tempEvents = append(tempEvents, event) //
		}
	}

	sort.Slice(tempEvents, func(i, j int) bool { //
		return tempEvents[i].EventData.Index < tempEvents[j].EventData.Index //
	})
	return tempEvents //
}

// PrintDagStoreStatus logs a detailed status of the DagStore for debugging.
// WARNING: This can be very verbose if the DagStore contains many events.
func (ds *DagStore) PrintDagStoreStatus() { //
	ds.mu.RLock()         //
	defer ds.mu.RUnlock() //

	var status strings.Builder //
	// Define total internal width for content between the main borders
	// Increased width for more space
	const totalInternalWidth = 98                                                                                                  //
	const topBottomBorder = "╔══════════════════════════════════════════════════════════════════════════════════════════════════╗" //
	const midBorder = "╠══════════════════════════════════════════════════════════════════════════════════════════════════╣"       //
	const endBorder = "╚══════════════════════════════════════════════════════════════════════════════════════════════════╝"       //

	status.WriteString("\n" + topBottomBorder + "\n") //
	// Helper function to create a padded line
	line := func(content string) string { //
		// Trim potential leading/trailing newlines from content itself before calculating length
		trimmedContent := strings.TrimRight(content, "\n\r")    //
		paddingSize := totalInternalWidth - len(trimmedContent) //
		if paddingSize < 0 {                                    //
			paddingSize = 0 // Prevent negative padding //
		}
		return fmt.Sprintf("║%s%s║\n", trimmedContent, strings.Repeat(" ", paddingSize)) //
	}
	// Helper function for centered section titles
	centeredTitle := func(title string) string { //
		titleText := " " + title + " "          //
		totalTitleLen := len(titleText)         //
		if totalTitleLen > totalInternalWidth { // Title is too long, truncate //
			titleText = titleText[:totalInternalWidth-3] + "..." //
			totalTitleLen = totalInternalWidth                   //
		}
		leftPaddingCount := (totalInternalWidth - totalTitleLen) / 2               //
		rightPaddingCount := totalInternalWidth - totalTitleLen - leftPaddingCount //
		if leftPaddingCount < 0 {                                                  //
			leftPaddingCount = 0 //
		}
		if rightPaddingCount < 0 { //
			rightPaddingCount = 0 //
		}
		return fmt.Sprintf("╠%s%s%s╣\n", strings.Repeat("═", leftPaddingCount), titleText, strings.Repeat("═", rightPaddingCount)) //
	}

	status.WriteString(line(fmt.Sprintf(" %-*s", totalInternalWidth-1, "Detailed DagStore Status"))) // Left align title within the space //
	status.WriteString(midBorder + "\n")                                                             //

	status.WriteString(line(fmt.Sprintf(" Total Events Stored: %d", len(ds.events))))                           //
	status.WriteString(line(fmt.Sprintf(" Known Creators (Latest Events Map Size): %d", len(ds.latestEvents)))) //
	status.WriteString(line(fmt.Sprintf(" Last Decided Frame: %d", ds.lastDecidedFrame)))                       //
	status.WriteString(line(fmt.Sprintf(" Quorum for Clotho Decision: %d", QUORUM)))                            //

	status.WriteString(centeredTitle("Stake Distribution"))                        //
	status.WriteString(line(fmt.Sprintf(" Number of Stakers: %d", len(ds.stake)))) //
	if len(ds.stake) > 0 {                                                         //
		var stakerKeys []string     //
		for key := range ds.stake { //
			stakerKeys = append(stakerKeys, key) //
		}
		sort.Strings(stakerKeys)         // Sort for consistent output //
		for _, key := range stakerKeys { //
			status.WriteString(line(fmt.Sprintf("   - Creator %s...: Stake %d", key[:min(10, len(key))], ds.stake[key]))) //
		}
	} else {
		status.WriteString(line("   No stakers defined.")) //
	}

	status.WriteString(centeredTitle("Latest Events by Creator")) //
	if len(ds.latestEvents) > 0 {                                 //
		var creatorKeys []string           //
		for key := range ds.latestEvents { //
			creatorKeys = append(creatorKeys, key) //
		}
		sort.Strings(creatorKeys)                // Sort for consistent output //
		for _, creatorKey := range creatorKeys { //
			eventID := ds.latestEvents[creatorKey]                 //
			var eventIndex uint64                                  //
			var eventFrame uint64                                  //
			if latestEvent, exists := ds.events[eventID]; exists { //
				eventIndex = latestEvent.EventData.Index //
				eventFrame = latestEvent.EventData.Frame //
			}
			status.WriteString(line(fmt.Sprintf("   - Creator %s...: Latest Event %s (Idx: %d, Frame: %d)", //
				creatorKey[:min(10, len(creatorKey))], //
				eventID.Short(),                       //
				eventIndex,                            //
				eventFrame)))                          //
		}
	} else {
		status.WriteString(line("   No latest events tracked.")) //
	}

	status.WriteString(centeredTitle("Roots By Frame"))                                   //
	status.WriteString(line(fmt.Sprintf(" Frames with Roots: %d", len(ds.rootsByFrame)))) //
	if len(ds.rootsByFrame) > 0 {                                                         //
		var framesWithRoots []uint64         //
		for frame := range ds.rootsByFrame { //
			framesWithRoots = append(framesWithRoots, frame) //
		}
		sort.Slice(framesWithRoots, func(i, j int) bool { // Sort frames numerically //
			return framesWithRoots[i] < framesWithRoots[j] //
		})
		for _, frame := range framesWithRoots { //
			rootIDs := ds.rootsByFrame[frame]                                                       //
			status.WriteString(line(fmt.Sprintf("   Frame %-4d (%d roots):", frame, len(rootIDs)))) //

			for _, rootID := range rootIDs { // rootIDs are already sorted by string value during AddEvent //
				rootEvent, exists := ds.events[rootID] //
				if exists {                            //
					status.WriteString(line(fmt.Sprintf("     - %s - %s (Creator: %s..., Idx: %d, Status: %s, Candidate: %t)", //
						rootEvent.GetEventId().Short(), //
						rootID.Short(),                 // Using .Short() for better Event ID visibility //
						hex.EncodeToString(rootEvent.EventData.Creator)[:min(6, len(hex.EncodeToString(rootEvent.EventData.Creator)))], //
						rootEvent.EventData.Index, //
						rootEvent.ClothoStatus,    //
						rootEvent.Candidate)))     //
				} else {
					status.WriteString(line(fmt.Sprintf("     - %s (Details not found in events map)", rootID.Short()))) //
				}
				if len(rootEvent.EventData.OtherParents) > 0 { //
					status.WriteString(line("       - Other Parents:"))         //
					for _, parentID := range rootEvent.EventData.OtherParents { //
						status.WriteString(line("         - " + parentID.Short())) //
					}
				}
			}
		}
	} else {
		status.WriteString(line("   No frames with roots.")) //
	}

	status.WriteString(centeredTitle("All Events")) //
	if len(ds.events) > 0 {                         //
		allEvents := make([]*Event, 0, len(ds.events)) //
		for _, event := range ds.events {              //
			allEvents = append(allEvents, event) //
		}
		sort.Slice(allEvents, func(i, j int) bool { //
			if allEvents[i].EventData.Frame != allEvents[j].EventData.Frame { //
				return allEvents[i].EventData.Frame < allEvents[j].EventData.Frame //
			}
			if allEvents[i].EventData.Timestamp != allEvents[j].EventData.Timestamp { //
				return allEvents[i].EventData.Timestamp < allEvents[j].EventData.Timestamp //
			}
			return allEvents[i].GetEventId().String() < allEvents[j].GetEventId().String() //
		})

		status.WriteString(line(fmt.Sprintf(" Listing %d events (sorted by Frame, Timestamp, EventID):", len(allEvents)))) //

		for i, event := range allEvents { //
			status.WriteString(line(fmt.Sprintf(" --- Event %d/%d ---", i+1, len(allEvents)))) //

			eventStringLines := strings.Split(strings.TrimSpace(event.String()), "\n") //
			for _, eventLineStr := range eventStringLines {                            //
				trimmedLine := strings.TrimSpace(eventLineStr) //
				// Max length for the indented content part.
				// totalInternalWidth for the whole line content, -2 for "  " indent.
				const maxLineContentLength = totalInternalWidth - 2 //

				displayLine := "  " + trimmedLine            // Indent by 2 spaces //
				if len(displayLine) > maxLineContentLength { //
					// If the indented line is too long, truncate it.
					// We need to make sure the displayLine itself, after potential truncation,
					// fits within the "line" helper function's expectation correctly.
					// The "line" helper expects the content *without* the final padding.
					// So, displayLine here should be the string that will be passed to `line()`.
					// If `displayLine` is already `totalInternalWidth` long or more,
					// then the `line` helper's padding calculation might become 0 or negative.
					// It's better to truncate `displayLine` so it's less than `totalInternalWidth`.
					if len(displayLine) > totalInternalWidth-3 { // -3 for "..." //
						displayLine = displayLine[:totalInternalWidth-3] + "..." //
					}
				}
				status.WriteString(line(displayLine)) //
			}
		}
	} else {
		status.WriteString(line(" No events in DagStore.")) //
	}

	status.WriteString(endBorder + "\n") //

	logger.Info(status.String()) //
}
