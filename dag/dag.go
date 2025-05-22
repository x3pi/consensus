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

// QUORUM_PERCENTAGE defines the voting threshold (by stake) required to decide Clotho status.
// For example, 0.67 represents approximately 2/3 of the total network stake.
const QUORUM_PERCENTAGE float64 = 0.67 // Represents > 2/3

// K_DECISION_ROUND defines the round at which a decision for xEvent is made based on yEvents.
// For example, if K_DECISION_ROUND = 2, yEvents in xFrame+2 will decide xEvent.
const K_DECISION_ROUND uint64 = 2

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
	// totalStake stores the sum of all stakes in the network.
	totalStake uint64
}

// NewDagStore creates a new instance of DagStore.
// initialStake is a map from a validator's hex public key to their stake amount.
func NewDagStore(initialStake map[string]uint64) *DagStore {
	stakeMap := make(map[string]uint64, len(initialStake))
	var currentTotalStake uint64 = 0
	for pubKeyHex, s := range initialStake {
		stakeMap[pubKeyHex] = s
		currentTotalStake += s
	}
	if currentTotalStake == 0 && len(initialStake) > 0 {
		logger.Warn("NewDagStore: initialStake provided, but totalStake is 0. This might indicate all stakes are zero.")
	} else if len(initialStake) == 0 {
		logger.Warn("NewDagStore: initialStake is empty. totalStake will be 0. Consensus may not function correctly without stake information.")
	}

	return &DagStore{
		events:           make(map[EventID]*Event),
		latestEvents:     make(map[string]EventID),
		rootsByFrame:     make(map[uint64][]EventID),
		lastDecidedFrame: 0, // Lachesis frames are typically 1-indexed. 0 means no frames decided.
		mu:               sync.RWMutex{},
		stake:            stakeMap,
		totalStake:       currentTotalStake,
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
		// logger.Debug(fmt.Sprintf("DagStore.AddEvent: Event %s already exists. Skipping.", eventID.Short()))
		return nil // Event already exists, no need to add again.
	}

	creatorKey := hex.EncodeToString(event.EventData.Creator)

	// Validate SelfParent and Index
	currentLatestEventID, creatorHasPrevious := ds.latestEvents[creatorKey]
	if creatorHasPrevious {
		// This is not the first event from this creator
		if event.EventData.Index == 1 {
			return fmt.Errorf("event %s by %s is marked as Index 1, but creator already has previous events (latest: %s)",
				eventID.Short(), creatorKey[:6], currentLatestEventID.Short())
		}
		if event.EventData.SelfParent.IsZero() {
			return fmt.Errorf("non-first event %s by %s (Index %d) has zero self parent",
				eventID.Short(), creatorKey[:6], event.EventData.Index)
		}
		if event.EventData.SelfParent != currentLatestEventID {
			var expectedPrevIndex uint64
			if selfParentEvent, ok := ds.events[currentLatestEventID]; ok {
				expectedPrevIndex = selfParentEvent.EventData.Index
			}
			return fmt.Errorf("invalid self parent for event %s by %s (Index %d): expected %s (Index %d), got %s (Index %d expected based on current)",
				eventID.Short(), creatorKey[:6], event.EventData.Index,
				currentLatestEventID.Short(), expectedPrevIndex,
				event.EventData.SelfParent.Short(), event.EventData.Index-1)
		}
		if prevEvent, ok := ds.events[currentLatestEventID]; ok {
			if event.EventData.Index != prevEvent.EventData.Index+1 {
				return fmt.Errorf("invalid index for event %s by %s: expected %d (previous was %d), got %d",
					eventID.Short(), creatorKey[:6], prevEvent.EventData.Index+1, prevEvent.EventData.Index, event.EventData.Index)
			}
		} else {
			// This case (creatorHasPrevious is true but currentLatestEventID not in ds.events) should ideally not happen
			// if the DagStore is consistent.
			return fmt.Errorf("internal inconsistency: creator %s has latest event %s, but event not found in store",
				creatorKey[:6], currentLatestEventID.Short())
		}
	} else {
		// This is the creator's first event
		if !event.EventData.SelfParent.IsZero() {
			return fmt.Errorf("first event %s of creator %s must have zero self parent, got %s", eventID.Short(), creatorKey[:6], event.EventData.SelfParent.Short())
		}
		if event.EventData.Index != 1 {
			return fmt.Errorf("first event %s of creator %s must have index 1, got %d", eventID.Short(), creatorKey[:6], event.EventData.Index)
		}
		if _, exists := ds.stake[creatorKey]; !exists {
			logger.Warn(fmt.Sprintf("DagStore.AddEvent: Creator %s (%s) is adding their first event but has no registered stake. Event added but consensus might be affected.", creatorKey[:6], eventID.Short()))
		}
	}

	// Validate OtherParents (check if they exist in the store)
	// In a distributed system, it's possible to receive an event whose OtherParents are not yet known.
	// Such events are often called "orphans" and are typically stored temporarily.
	// For simplicity here, we'll log a warning if an OtherParent is missing.
	for i, otherParentID := range event.EventData.OtherParents {
		if otherParentID.IsZero() {
			logger.Warn(fmt.Sprintf("DagStore.AddEvent: Event %s has a zero OtherParent at index %d.", eventID.Short(), i))
			continue // Allow zero other parents if the protocol supports it.
		}
		if _, exists := ds.events[otherParentID]; !exists {
			logger.Info(fmt.Sprintf("DagStore.AddEvent: Other parent %s for event %s does not exist in DagStore. Event might be an orphan temporarily.", otherParentID.Short(), eventID.Short()))
			// Depending on the system's design, you might reject such events or queue them.
		}
	}

	ds.events[eventID] = event

	// Update latestEvents only if the new event has a higher index.
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
			// Keep the list of roots consistently sorted by EventID string.
			sort.Slice(ds.rootsByFrame[event.EventData.Frame], func(i, j int) bool {
				return ds.rootsByFrame[event.EventData.Frame][i].String() < ds.rootsByFrame[event.EventData.Frame][j].String()
			})
		}
	}
	logger.Info(fmt.Sprintf("DagStore.AddEvent: Added event: %s (Creator: %s, Idx: %d, F: %d, Root: %t, SelfP: %s, OtherPs: %d)",
		eventID.Short(), creatorKey[:6], event.EventData.Index, event.EventData.Frame, event.EventData.IsRoot, event.EventData.SelfParent.Short(), len(event.EventData.OtherParents)))
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
		// logger.Warn(fmt.Sprintf("getStakeLocked: Stake not found for creator %s", creatorPubKeyHex[:6]))
		return 0 // Return 0 if stake not found, or handle as an error.
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
			// logger.Warn(fmt.Sprintf("DagStore.isAncestorLocked: Event %s not found in store during ancestry check for descendant %s from ancestor %s",
			// 	currentID.Short(), descendantID.Short(), ancestorID.Short()))
			continue // Or handle as an error.
		}

		var parents []EventID
		if !currentEvent.EventData.SelfParent.IsZero() {
			parents = append(parents, currentEvent.EventData.SelfParent)
		}
		// Include OtherParents in the traversal
		for _, opID := range currentEvent.EventData.OtherParents {
			if !opID.IsZero() {
				parents = append(parents, opID)
			}
		}

		for _, parentID := range parents {
			if parentID.IsZero() { // Should not happen if OtherParents are validated
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

// getParentsEventsLocked returns the actual Event objects for all parents of a given event.
// Requires ds.mu to be held.
func (ds *DagStore) getParentsEventsLocked(event *Event) []*Event {
	if event == nil {
		return nil
	}
	parents := make([]*Event, 0)
	if !event.EventData.SelfParent.IsZero() {
		if p, ok := ds.events[event.EventData.SelfParent]; ok {
			parents = append(parents, p)
		}
	}
	for _, opID := range event.EventData.OtherParents {
		if !opID.IsZero() {
			if p, ok := ds.events[opID]; ok {
				parents = append(parents, p)
			}
		}
	}
	return parents
}

// stronglySeeLocked checks if event e1 strongly sees event e2.
// e1 strongly sees e2 if e2 is an ancestor of e1, and e1 is connected to e2
// by parent-child links through a "supermajority" of creators at each step.
// This is a complex part of Lachesis. The implementation here is a placeholder.
// Requires ds.mu to be held.
func (ds *DagStore) stronglySeeLocked(e1ID, e2ID EventID) bool {
	_, e1Exists := ds.events[e1ID]
	_, e2Exists := ds.events[e2ID]
	if !e1Exists || !e2Exists {
		// logger.Warn(fmt.Sprintf("stronglySeeLocked: e1 (%s exists: %t) or e2 (%s exists: %t) not found.", e1ID.Short(), e1Exists, e2ID.Short(), e2Exists))
		return false
	}

	if !ds.isAncestorLocked(e2ID, e1ID) {
		return false // Basic condition: e2 must be an ancestor of e1.
	}

	// --- Start of Placeholder for Full "Strongly See" Logic ---
	// A full "strongly see" implementation involves:
	// 1. Finding all events created by a supermajority of stake (e.g., > 2/3 total stake).
	//    Let this set of creators be S_supermajority.
	// 2. Checking if e1 can reach e2 through a path of events such that for each
	//    event `u` on the path (child) and its parent `v` on the path:
	//    `u` must have parents from S_supermajority (or all its parents if less than supermajority size)
	//    that also strongly see `v` (or are `v`).
	//
	// This is recursive and computationally intensive.
	// For now, we simplify: if e2 is an ancestor of e1, we consider it "strongly seen".
	// This simplification will likely lead to more events being considered "strongly seen"
	// than in a strict Lachesis implementation, which might affect consensus accuracy.
	//
	// logger.Debug(fmt.Sprintf("StronglySee (simplified): %s sees %s because it's an ancestor.", e1ID.Short(), e2ID.Short()))
	return true // Placeholder: True if ancestor.
	// --- End of Placeholder ---
}

// StronglySee is the public, lock-managed version of stronglySeeLocked.
func (ds *DagStore) StronglySee(e1ID, e2ID EventID) bool {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.stronglySeeLocked(e1ID, e2ID)
}

// forklessCause checks if event x "forkless causes" event y.
// In Lachesis, this typically means x is an ancestor of y, and y strongly sees x.
// The current `StronglySee` is simplified.
func (ds *DagStore) forklessCause(x, y *Event) bool {
	if x == nil || y == nil {
		return false
	}
	// ds.mu must be held by the caller (e.g. DecideClotho)
	return ds.stronglySeeLocked(y.GetEventId(), x.GetEventId()) // y strongly sees x
}

// CalculateQuorumThreshold computes the actual stake value needed for a quorum.
// It's based on QUORUM_PERCENTAGE of the total stake.
func (ds *DagStore) CalculateQuorumThreshold() uint64 {
	// ds.mu must be held by the caller if totalStake can change dynamically.
	// Here, totalStake is set at creation and assumed stable for this calculation.
	if ds.totalStake == 0 {
		logger.Warn("CalculateQuorumThreshold: totalStake is 0. Quorum will be 1. This is unusual.")
		return 1 // Avoid division by zero, or handle as an error.
	}
	// The +1 ensures it's strictly greater than (percentage * totalStake).
	// E.g., if totalStake=30, percentage=0.67 (2/3), threshold = 0.67*30 + 1 = 20.1 + 1 = 21.
	// This means > 2/3 of stake.
	return uint64(QUORUM_PERCENTAGE*float64(ds.totalStake)) + 1
}

// DecideClotho implements the Clotho selection algorithm.
// This algorithm determines the Clotho status (IS-CLOTHO / IS-NOT-CLOTHO) for Root events.
func (ds *DagStore) DecideClotho() {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	// logger.Info("DecideClotho: Starting Clotho decision process.")

	quorumThreshold := ds.CalculateQuorumThreshold()
	if ds.totalStake > 0 { // Only log if totalStake is meaningful
		logger.Info(fmt.Sprintf("DecideClotho: TotalStake=%d, QuorumPercentage=%.3f, Calculated QuorumThreshold=%d", ds.totalStake, QUORUM_PERCENTAGE, quorumThreshold))
	}

	startFrame := ds.lastDecidedFrame + 1
	var maxFrameWithRoots uint64 = 0
	for frame := range ds.rootsByFrame {
		if frame > maxFrameWithRoots {
			maxFrameWithRoots = frame
		}
	}
	// logger.Info(fmt.Sprintf("DecideClotho: StartFrame=%d, MaxFrameWithRoots=%d, CurrentLastDecidedFrame=%d", startFrame, maxFrameWithRoots, ds.lastDecidedFrame))

	if startFrame > maxFrameWithRoots {
		// logger.Info("DecideClotho: No new frames to process for xEvents.")
		return
	}

	for xFrame := startFrame; xFrame <= maxFrameWithRoots; xFrame++ {
		// logger.Info(fmt.Sprintf("DecideClotho: Processing xFrame = %d", xFrame))
		xRootsIDsInCurrentXFrame := ds.rootsByFrame[xFrame] // Already sorted by EventID string
		if len(xRootsIDsInCurrentXFrame) == 0 {
			// logger.Info(fmt.Sprintf("DecideClotho: No roots found in xFrame %d.", xFrame))
			// Continue to check if this empty frame can advance lastDecidedFrame later.
		}

		allRootsInXFrameDecided := true // Assume true until proven otherwise

		for _, xID := range xRootsIDsInCurrentXFrame {
			xEvent, exists := ds.events[xID]
			if !exists { // Should not happen if rootsByFrame is consistent with events
				logger.Error(fmt.Sprintf("DecideClotho: Root event %s from xFrame %d not found in events map. Skipping.", xID.Short(), xFrame))
				allRootsInXFrameDecided = false
				continue
			}
			if !xEvent.EventData.IsRoot { // Should not happen
				logger.Error(fmt.Sprintf("DecideClotho: Event %s from rootsByFrame (xFrame %d) is not marked as root. Skipping.", xID.Short(), xFrame))
				allRootsInXFrameDecided = false
				continue
			}

			if xEvent.ClothoStatus != ClothoUndecided {
				// logger.Debug(fmt.Sprintf("DecideClotho: xEvent %s (F%d) already decided as %s. Skipping.", xID.Short(), xFrame, xEvent.ClothoStatus))
				continue // Already decided, skip but don't break advancement of lastDecidedFrame
			}

			// logger.Info(fmt.Sprintf("DecideClotho: Evaluating xEvent %s (Frame %d)", xID.Short(), xFrame))

			// Determine votes from yEvents in xFrame+1 for xEvent (Round 1 voting)
			// These yEvents are roots in frame xFrame+1.
			yFrameForRound1Voting := xFrame + 1
			if yFrameForRound1Voting <= maxFrameWithRoots {
				yRootsForRound1Voting := ds.rootsByFrame[yFrameForRound1Voting]
				for _, yIDForRound1 := range yRootsForRound1Voting {
					yEventForRound1, yExists := ds.events[yIDForRound1]
					if yExists && yEventForRound1.EventData.IsRoot {
						// Vote is true if yEventForRound1 "forklessCauses" (strongly sees) xEvent.
						// With current simplified stronglySee, this means yEventForRound1 must see xEvent.
						vote := ds.forklessCause(xEvent, yEventForRound1) // xEvent is older, yEventForRound1 is newer
						yEventForRound1.SetVote(xID, vote)                // yEventForRound1 votes on xID
						// logger.Debug(fmt.Sprintf("DecideClotho: yEvent %s (F%d) votes %t for xEvent %s (F%d) in round 1 (yFrame calculation)", yIDForRound1.Short(), yFrameForRound1Voting, vote, xID.Short(), xFrame))
					}
				}
			}

			// Attempt to decide xEvent based on votes from yEvents in the decision frame (xFrame + K_DECISION_ROUND)
			decisionFrameForX := xFrame + K_DECISION_ROUND
			if decisionFrameForX > maxFrameWithRoots {
				// logger.Info(fmt.Sprintf("DecideClotho: Not enough subsequent frames to decide xEvent %s (F%d). Needs up to F%d, max available F%d.", xID.Short(), xFrame, decisionFrameForX, maxFrameWithRoots))
				allRootsInXFrameDecided = false
				continue // Move to next xEvent in the same xFrame
			}

			accumulatedYesStakeForX := uint64(0)
			accumulatedNoStakeForX := uint64(0)
			yRootsInDecisionFrame := ds.rootsByFrame[decisionFrameForX]

			if len(yRootsInDecisionFrame) == 0 {
				// logger.Info(fmt.Sprintf("DecideClotho: No roots in decision frame F%d to decide xEvent %s (F%d).", decisionFrameForX, xID.Short(), xFrame))
				allRootsInXFrameDecided = false
				continue
			}

			// logger.Info(fmt.Sprintf("DecideClotho: xEvent %s (F%d) attempting decision with %d roots from F%d.", xID.Short(), xFrame, len(yRootsInDecisionFrame), decisionFrameForX))

			for _, yDecisionID := range yRootsInDecisionFrame {
				yDecisionEvent, yDecisionExists := ds.events[yDecisionID]
				if !yDecisionExists || !yDecisionEvent.EventData.IsRoot {
					// logger.Warn(fmt.Sprintf("DecideClotho: yDecisionEvent %s (F%d) not found or not root. Skipping.", yDecisionID.Short(), decisionFrameForX))
					continue
				}

				// yDecisionEvent (in frame xFrame+K_DECISION_ROUND) determines its vote for xEvent.
				// Its vote is based on the votes (for xEvent) from prevVoters (roots in frame xFrame+K_DECISION_ROUND-1)
				// that yDecisionEvent forklessCauses (strongly sees).
				prevVotersFrame := decisionFrameForX - 1 // This is xFrame + 1
				prevVotersIDs := ds.rootsByFrame[prevVotersFrame]
				yesStakeForYVote := uint64(0) // Stake supporting YES for xEvent, from perspective of yDecisionEvent
				noStakeForYVote := uint64(0)  // Stake supporting NO for xEvent, from perspective of yDecisionEvent

				for _, prevRootID := range prevVotersIDs {
					prevRootEvent, prevRootExists := ds.events[prevRootID]
					if !prevRootExists || !prevRootEvent.EventData.IsRoot {
						// logger.Warn(fmt.Sprintf("DecideClotho: prevRootEvent %s (F%d) for yDecisionEvent %s not found or not root. Skipping.", prevRootID.Short(), prevVotersFrame, yDecisionID.Short()))
						continue
					}

					// yDecisionEvent must "forklessCause" (strongly see) prevRootEvent to consider its vote.
					if ds.forklessCause(yDecisionEvent, prevRootEvent) { // yDecisionEvent is newer, prevRootEvent is older
						prevVoteForX, voteExists := prevRootEvent.GetVote(xID) // prevRootEvent's (round 1) vote for xID
						if voteExists {
							prevVoterStake := ds.getStakeLocked(hex.EncodeToString(prevRootEvent.EventData.Creator))
							if prevVoterStake == 0 {
								// logger.Warn(fmt.Sprintf("DecideClotho: prevRootEvent %s (creator %s) has 0 stake. Vote not counted for yDecisionEvent %s's decision on xEvent %s.", prevRootID.Short(), hex.EncodeToString(prevRootEvent.EventData.Creator)[:6], yDecisionID.Short(), xID.Short()))
							}
							if prevVoteForX { // If prevRootEvent voted YES for xEvent
								yesStakeForYVote += prevVoterStake
							} else { // If prevRootEvent voted NO for xEvent
								noStakeForYVote += prevVoterStake
							}
						} else {
							// logger.Debug(fmt.Sprintf("DecideClotho: prevRootEvent %s (F%d) did not cast a vote for xEvent %s. Not counted for yDecisionEvent %s.", prevRootID.Short(), prevVotersFrame, xID.Short(), yDecisionID.Short()))
						}
					} else {
						// logger.Debug(fmt.Sprintf("DecideClotho: yDecisionEvent %s (F%d) does not forklessCause prevRootEvent %s (F%d). Vote not counted.",yDecisionID.Short(), decisionFrameForX, prevRootID.Short(), prevVotersFrame))
					}
				}
				// yDecisionEvent determines its final vote for xEvent based on aggregated stake.
				// Tie-breaking rule: default to NO if stake is equal (or YES, be consistent). Lachesis paper often defaults to YES (candidate=true).
				// Let's use YES if yesStake >= noStake.
				yFinalVoteForX := (yesStakeForYVote >= noStakeForYVote)
				yDecisionEvent.SetVote(xID, yFinalVoteForX) // Store y's final vote on x for transparency/future rounds.
				// logger.Debug(fmt.Sprintf("DecideClotho: yDecisionEvent %s (F%d) votes %t for xEvent %s (F%d). Based on (yesStake: %d, noStake: %d from F%d voters)",
				// yDecisionID.Short(), decisionFrameForX, yFinalVoteForX, xID.Short(), xFrame, yesStakeForYVote, noStakeForYVote, prevVotersFrame))

				// Now, use yDecisionEvent's vote (yFinalVoteForX) and its stake for xEvent's decision
				yDecisionEventStake := ds.getStakeLocked(hex.EncodeToString(yDecisionEvent.EventData.Creator))
				if yDecisionEventStake == 0 {
					// logger.Warn(fmt.Sprintf("DecideClotho: yDecisionEvent %s (creator %s) has 0 stake. Its vote for xEvent %s is not counted towards quorum.", yDecisionID.Short(), hex.EncodeToString(yDecisionEvent.EventData.Creator)[:6], xID.Short()))
				}

				if yFinalVoteForX {
					accumulatedYesStakeForX += yDecisionEventStake
				} else {
					accumulatedNoStakeForX += yDecisionEventStake
				}
			} // End loop over yRootsInDecisionFrame

			// After checking all yEvents in the decision frame for xEvent
			logger.Info(fmt.Sprintf("DecideClotho: For xEvent %s (F%d), AccumulatedYesStake: %d, AccumulatedNoStake: %d, QuorumThreshold: %d (from F%d voters)",
				xID.Short(), xFrame, accumulatedYesStakeForX, accumulatedNoStakeForX, quorumThreshold, decisionFrameForX))

			if accumulatedYesStakeForX >= quorumThreshold {
				xEvent.SetCandidate(true) // Atropos candidate
				xEvent.SetClothoStatus(ClothoIsClotho)
				// logger.Error(xEvent.Transactions) // Log transactions of decided event - might be too verbose
				logger.Info(fmt.Sprintf("DecideClotho: <<< DECIDED >>> Root %s (F%d) IS CLOTHO. YesStake: %d >= Quorum: %d (from F%d voters)",
					xID.Short(), xFrame, accumulatedYesStakeForX, quorumThreshold, decisionFrameForX))
			} else if accumulatedNoStakeForX >= quorumThreshold {
				xEvent.SetCandidate(false)
				xEvent.SetClothoStatus(ClothoIsNotClotho)
				logger.Info(fmt.Sprintf("DecideClotho: <<< DECIDED >>> Root %s (F%d) IS NOT CLOTHO. NoStake: %d >= Quorum: %d (from F%d voters)",
					xID.Short(), xFrame, accumulatedNoStakeForX, quorumThreshold, decisionFrameForX))
			} else {
				// logger.Info(fmt.Sprintf("DecideClotho: Root %s (F%d) remains UNDECIDED. Not enough stake for quorum from F%d voters. (Yes: %d, No: %d, Quorum: %d)", xID.Short(), xFrame, decisionFrameForX, accumulatedYesStakeForX, accumulatedNoStakeForX, quorumThreshold))
				allRootsInXFrameDecided = false
			}
		} // End loop for xEvent in xRootsIDsInCurrentXFrame

		// logger.Info(fmt.Sprintf("DecideClotho: Finished evaluating roots for xFrame %d. All decided in this frame: %t", xFrame, allRootsInXFrameDecided))

		if allRootsInXFrameDecided && len(xRootsIDsInCurrentXFrame) > 0 {
			ds.lastDecidedFrame = xFrame
			logger.Info(fmt.Sprintf("DecideClotho: Updated lastDecidedFrame to %d because all roots in this non-empty frame are now decided.", ds.lastDecidedFrame))
		} else if allRootsInXFrameDecided && len(xRootsIDsInCurrentXFrame) == 0 { // Empty frame, can advance
			// Only advance lastDecidedFrame for an empty frame if it's not breaking a sequence of undecided non-empty frames.
			if ds.lastDecidedFrame == xFrame-1 {
				ds.lastDecidedFrame = xFrame
				// logger.Info(fmt.Sprintf("DecideClotho: Advanced lastDecidedFrame to %d (empty frame, previous was decided).", ds.lastDecidedFrame))
			} else {
				// logger.Info(fmt.Sprintf("DecideClotho: Empty xFrame %d. lastDecidedFrame %d. Not advancing due to potential gap.", xFrame, ds.lastDecidedFrame))
				break // Stop if there's a gap caused by a previous non-empty undecided frame
			}
		} else { // Not all roots decided in this non-empty frame, or it's an empty frame after an undecided one.
			// logger.Info(fmt.Sprintf("DecideClotho: xFrame %d not fully decided or is an unbridgeable empty frame. Stopping advancement of lastDecidedFrame. Current lastDecidedFrame: %d.", xFrame, ds.lastDecidedFrame))
			break // Stop because we can't advance lastDecidedFrame further in this iteration.
		}
	} // End loop for xFrame
	// logger.Info(fmt.Sprintf("DecideClotho: Process finished. Final lastDecidedFrame: %d", ds.lastDecidedFrame))
}

// PruneOldEvents removes events from frames older than oldestFrameToKeep.
// oldestFrameToKeep is the OLDEST frame number that should be RETAINED.
// Events in frames < oldestFrameToKeep will be deleted.
func (ds *DagStore) PruneOldEvents(oldestFrameToKeep uint64) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	// logger.Info(fmt.Sprintf("PruneOldEvents: Starting cleanup of events in frames older than %d. Current lastDecidedFrame: %d", oldestFrameToKeep, ds.lastDecidedFrame))

	if oldestFrameToKeep == 0 {
		// logger.Warn("PruneOldEvents: oldestFrameToKeep is 0. Skipping pruning to avoid deleting crucial data (e.g. frame 0 if used).")
		return
	}

	// Safety condition: Only allow pruning if oldestFrameToKeep is less than or equal to ds.lastDecidedFrame.
	// This ensures that the last fully decided frame is not accidentally pruned before it's truly old.
	// However, for Lachesis, pruning is usually tied to frames well behind the last *finalized* (Atropos) frame.
	// Let's adjust this slightly: allow pruning up to a certain number of frames *before* the lastDecidedFrame.
	// For example, keep K_DECISION_ROUND + some buffer frames.
	// For simplicity, we'll stick to the original logic for now: prune if oldestFrameToKeep <= lastDecidedFrame.
	// A more robust pruning strategy would be based on the last *finalized* block/frame.
	if oldestFrameToKeep > ds.lastDecidedFrame && ds.lastDecidedFrame > 0 { // ds.lastDecidedFrame > 0 ensures we don't block pruning if no frames decided yet
		logger.Info(fmt.Sprintf("PruneOldEvents: oldestFrameToKeep (%d) is greater than the last decided frame (%d). Skipping pruning for safety.", oldestFrameToKeep, ds.lastDecidedFrame))
		return
	}

	eventsToDelete := make(map[EventID]struct{})

	// 1. Identify events to delete from ds.events.
	for id, event := range ds.events {
		if event.EventData.Frame < oldestFrameToKeep {
			eventsToDelete[id] = struct{}{}
		}
	}

	if len(eventsToDelete) == 0 {
		// logger.Info(fmt.Sprintf("PruneOldEvents: No events older than frame %d found to delete.", oldestFrameToKeep))
		return
	}

	// logger.Info(fmt.Sprintf("PruneOldEvents: Identified %d events for deletion.", len(eventsToDelete)))

	// 2. Delete identified events from ds.events.
	for id := range eventsToDelete {
		delete(ds.events, id)
	}

	// 3. Clean up ds.rootsByFrame.
	framesProcessedForRoots := make(map[uint64]bool)
	for frame := range ds.rootsByFrame {
		if frame < oldestFrameToKeep {
			if !framesProcessedForRoots[frame] {
				// logger.Debug(fmt.Sprintf("PruneOldEvents: Removing entry for frame %d from rootsByFrame.", frame))
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
				// logger.Debug(fmt.Sprintf("PruneOldEvents: Removed %d root(s) from frame %d in rootsByFrame. %d root(s) remaining.", initialRootCountInFrame-len(validRoots), frame, len(validRoots)))
			}

			if len(validRoots) == 0 && initialRootCountInFrame > 0 {
				delete(ds.rootsByFrame, frame)
			} else if len(validRoots) > 0 || (len(validRoots) == 0 && initialRootCountInFrame == 0) {
				ds.rootsByFrame[frame] = validRoots
			}
		}
	}

	// minDisplayHexLen := func(s string, l int) int {
	// 	if len(s) < l {
	// 		return len(s)
	// 	}
	// 	return l
	// }

	// 4. Clean up ds.latestEvents.
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
			// logger.Info(fmt.Sprintf("PruneOldEvents: Latest event %s for creator %s was deleted. Searching for new latest event...",
			// 	currentLatestID.Short(), creatorKey[:minDisplayHexLen(creatorKey, 6)]))

			delete(ds.latestEvents, creatorKey)

			var newLatestEventForCreator *Event = nil
			for _, event := range ds.events { // Iterate over the pruned ds.events map
				if hex.EncodeToString(event.EventData.Creator) == creatorKey {
					if newLatestEventForCreator == nil || event.EventData.Index > newLatestEventForCreator.EventData.Index {
						newLatestEventForCreator = event
					}
				}
			}

			if newLatestEventForCreator != nil {
				ds.latestEvents[creatorKey] = newLatestEventForCreator.GetEventId()
				// logger.Info(fmt.Sprintf("PruneOldEvents: Updated latest event for creator %s to %s (Index: %d, Frame: %d)", creatorKey[:minDisplayHexLen(creatorKey, 6)], newLatestEventForCreator.GetEventId().Short(), newLatestEventForCreator.EventData.Index, newLatestEventForCreator.EventData.Frame))
			} else {
				// logger.Info(fmt.Sprintf("PruneOldEvents: No remaining events found for creator %s after pruning. Their entry in latestEvents is removed.",
				// 	creatorKey[:minDisplayHexLen(creatorKey, 6)]))
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
		// logger.Error(fmt.Sprintf("DagStore.GetHeightForNode: Latest event %s for creator %s not found in events map.", latestEventID.String(), nodeCreatorPubKeyHex))
		return 0, false
	}
	return event.EventData.Index, true
}

// GetInDegreeForNode returns the "in-degree" for the latest event of the node with the given creator public key hex.
// In-degree is defined as the number of distinct creators of its OtherParents.
func (ds *DagStore) GetInDegreeForNode(nodeCreatorPubKeyHex string) (uint64, bool) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	latestEventID, exists := ds.latestEvents[nodeCreatorPubKeyHex]
	if !exists {
		return 0, false
	}

	event, eventExists := ds.events[latestEventID]
	if !eventExists {
		// logger.Error(fmt.Sprintf("DagStore.GetInDegreeForNode: Latest event %s for creator %s not found in DagStore for in-degree calculation.", latestEventID.String(), nodeCreatorPubKeyHex))
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
			// logger.Warn(fmt.Sprintf("DagStore.GetInDegreeForNode: Parent event %s not found when calculating in-degree for %s's event %s",
			// 	parentEventID.Short(), nodeCreatorPubKeyHex[:6], latestEventID.Short()))
			continue
		}
		parentCreatorHex := hex.EncodeToString(parentEvent.EventData.Creator)
		// The definition of in-degree usually refers to distinct *other* creators.
		// However, Lachesis's paper might just count distinct creators of OtherParents.
		// Let's count distinct creators of OtherParents, regardless of whether they are the event's own creator.
		distinctCreators[parentCreatorHex] = struct{}{}
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
		// logger.Error(fmt.Sprintf("DagStore.GetEventsByCreatorSinceIndex: Invalid creator public key hex '%s': %v", creatorPubKeyHex, err))
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

// PrintDagStoreStatus logs a detailed status of the DagStore for debugging.
func (ds *DagStore) PrintDagStoreStatus() {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	var status strings.Builder
	const totalInternalWidth = 160 // Increased width
	topBottomBorder := "╔" + strings.Repeat("═", totalInternalWidth) + "╗"
	midBorder := "╠" + strings.Repeat("═", totalInternalWidth) + "╣"
	endBorder := "╚" + strings.Repeat("═", totalInternalWidth) + "╝"

	status.WriteString("\n" + topBottomBorder + "\n")
	line := func(content string) string {
		trimmedContent := strings.TrimRight(content, "\n\r")
		paddingSize := totalInternalWidth - len(trimmedContent)
		if paddingSize < 0 {
			paddingSize = 0
		}
		return fmt.Sprintf("║%s%s║\n", trimmedContent, strings.Repeat(" ", paddingSize))
	}
	centeredTitle := func(title string) string {
		titleText := " " + title + " "
		totalTitleLen := len(titleText)
		if totalTitleLen > totalInternalWidth {
			titleText = titleText[:totalInternalWidth-3] + "..."
			totalTitleLen = totalInternalWidth
		}
		leftPaddingCount := (totalInternalWidth - totalTitleLen) / 2
		rightPaddingCount := totalInternalWidth - totalTitleLen - leftPaddingCount
		if leftPaddingCount < 0 {
			leftPaddingCount = 0
		}
		if rightPaddingCount < 0 {
			rightPaddingCount = 0
		}
		return fmt.Sprintf("╠%s%s%s╣\n", strings.Repeat("═", leftPaddingCount), titleText, strings.Repeat("═", rightPaddingCount))
	}

	status.WriteString(line(fmt.Sprintf(" %-*s", totalInternalWidth-1, "Detailed DagStore Status")))
	status.WriteString(midBorder + "\n")

	status.WriteString(line(fmt.Sprintf(" Total Events Stored: %d", len(ds.events))))
	status.WriteString(line(fmt.Sprintf(" Known Creators (Latest Events Map Size): %d", len(ds.latestEvents))))
	status.WriteString(line(fmt.Sprintf(" Last Decided Frame: %d", ds.lastDecidedFrame)))
	status.WriteString(line(fmt.Sprintf(" K_DECISION_ROUND: %d", K_DECISION_ROUND)))
	status.WriteString(line(fmt.Sprintf(" Total Network Stake: %d", ds.totalStake)))
	status.WriteString(line(fmt.Sprintf(" Quorum Percentage: %.3f", QUORUM_PERCENTAGE)))
	status.WriteString(line(fmt.Sprintf(" Calculated Quorum Threshold: %d", ds.CalculateQuorumThreshold())))

	status.WriteString(centeredTitle("Stake Distribution"))
	status.WriteString(line(fmt.Sprintf(" Number of Stakers: %d", len(ds.stake))))
	if len(ds.stake) > 0 {
		var stakerKeys []string
		for key := range ds.stake {
			stakerKeys = append(stakerKeys, key)
		}
		sort.Strings(stakerKeys)
		for _, key := range stakerKeys {
			status.WriteString(line(fmt.Sprintf("   - Creator %s...: Stake %d", key[:min(10, len(key))], ds.stake[key])))
		}
	} else {
		status.WriteString(line("   No stakers defined."))
	}

	status.WriteString(centeredTitle("Latest Events by Creator"))
	if len(ds.latestEvents) > 0 {
		var creatorKeys []string
		for key := range ds.latestEvents {
			creatorKeys = append(creatorKeys, key)
		}
		sort.Strings(creatorKeys)
		for _, creatorKey := range creatorKeys {
			eventID := ds.latestEvents[creatorKey]
			var eventIndex, eventFrame uint64
			var eventIsRoot bool
			if latestEvent, exists := ds.events[eventID]; exists {
				eventIndex = latestEvent.EventData.Index
				eventFrame = latestEvent.EventData.Frame
				eventIsRoot = latestEvent.EventData.IsRoot
			}
			status.WriteString(line(fmt.Sprintf("   - Creator %s...: Latest Event %s (Idx: %d, F: %d, Root: %t)",
				creatorKey[:min(10, len(creatorKey))], eventID.Short(), eventIndex, eventFrame, eventIsRoot)))
		}
	} else {
		status.WriteString(line("   No latest events tracked."))
	}

	status.WriteString(centeredTitle("Roots By Frame"))
	status.WriteString(line(fmt.Sprintf(" Frames with Roots: %d", len(ds.rootsByFrame))))
	if len(ds.rootsByFrame) > 0 {
		var framesWithRoots []uint64
		for frame := range ds.rootsByFrame {
			framesWithRoots = append(framesWithRoots, frame)
		}
		sort.Slice(framesWithRoots, func(i, j int) bool { return framesWithRoots[i] < framesWithRoots[j] })

		for _, frame := range framesWithRoots {
			rootIDs := ds.rootsByFrame[frame] // Already sorted by string value
			status.WriteString(line(fmt.Sprintf("   Frame %-4d (%d roots):", frame, len(rootIDs))))

			for _, rootID := range rootIDs {
				rootEvent, exists := ds.events[rootID]
				if exists {
					var opShorts []string
					for _, op := range rootEvent.EventData.OtherParents {
						opShorts = append(opShorts, op.Short())
					}
					opStr := "None"
					if len(opShorts) > 0 {
						opStr = strings.Join(opShorts, ",")
					}
					status.WriteString(line(fmt.Sprintf("     - %s (Creator: %s..., Idx: %d, Status: %s, Cand: %t, SelfP: %s, OtherP: [%s])",
						rootID.Short(),
						hex.EncodeToString(rootEvent.EventData.Creator)[:min(6, len(hex.EncodeToString(rootEvent.EventData.Creator)))],
						rootEvent.EventData.Index,
						rootEvent.ClothoStatus,
						rootEvent.Candidate,
						rootEvent.EventData.SelfParent.Short(),
						opStr)))

					// Log votes by this root event
					rootEvent.mu.Lock() // Assuming mu protects Vote map
					if len(rootEvent.Vote) > 0 {
						var voteDetails []string
						// Sort vote keys for deterministic output
						votedOnIDs := make([]EventID, 0, len(rootEvent.Vote))
						for vid := range rootEvent.Vote {
							votedOnIDs = append(votedOnIDs, vid)
						}
						sort.Slice(votedOnIDs, func(i, j int) bool {
							return votedOnIDs[i].String() < votedOnIDs[j].String()
						})

						for _, votedOnID := range votedOnIDs {
							voteVal := rootEvent.Vote[votedOnID]
							voteDetails = append(voteDetails, fmt.Sprintf("%s->%t", votedOnID.Short(), voteVal))
						}
						status.WriteString(line(fmt.Sprintf("       Votes Cast By This Root: {%s}", strings.Join(voteDetails, "; "))))
					}
					rootEvent.mu.Unlock()

				} else {
					status.WriteString(line(fmt.Sprintf("     - %s (Details not found in events map)", rootID.Short())))
				}
			}
		}
	} else {
		status.WriteString(line("   No frames with roots."))
	}

	// Optionally, list all events if the number is small or for deep debugging.
	// This can be very verbose.
	/*
		status.WriteString(centeredTitle("All Events (Brief)"))
		if len(ds.events) > 0 {
			allEvents := ds.GetAllEventsSnapshot() // Already sorted
			status.WriteString(line(fmt.Sprintf(" Listing %d events:", len(allEvents))))
			for i, event := range allEvents {
				if i >= 20 && len(allEvents) > 25 { // Limit output for very large dags
					status.WriteString(line(fmt.Sprintf("   ... and %d more events not shown ...", len(allEvents)-i)))
					break
				}
				var opShorts []string
				for _, op := range event.EventData.OtherParents {
					opShorts = append(opShorts, op.Short())
				}
				opStr := "None"
				if len(opShorts) > 0 {
					opStr = strings.Join(opShorts, ",")
				}
				status.WriteString(line(fmt.Sprintf("   - ID: %s, C: %s.., I: %d, F: %d, R: %t, SP: %s, OP: [%s], CS: %s",
					event.GetEventId().Short(),
					hex.EncodeToString(event.EventData.Creator)[:min(6,len(hex.EncodeToString(event.EventData.Creator)))],
					event.EventData.Index,
					event.EventData.Frame,
					event.EventData.IsRoot,
					event.EventData.SelfParent.Short(),
					opStr,
					event.ClothoStatus,
				)))
			}
		} else {
			status.WriteString(line(" No events in DagStore."))
		}
	*/

	status.WriteString(endBorder + "\n")
	logger.Info(status.String())
}

// // min helper
// func min(a, b int) int {
// 	if a < b {
// 		return a
// 	}
// 	return b
// }
