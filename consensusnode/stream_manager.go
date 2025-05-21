package consensusnode

import (
	"context"
	"encoding/hex" // For encoding event creator public keys in syncRequestHandler.
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/blockchain/consensus/dag" // Ensure this path is correct for your DAG package.
)

// --- Protocol IDs ---

// TransactionsRequestProtocol defines the protocol ID for requesting transactions.
const TransactionsRequestProtocol protocol.ID = "/meta-node/transactions-request/1.0.0"

// TransactionStreamProtocol could be used for streaming transactions (currently illustrative).
const TransactionStreamProtocol protocol.ID = "/meta-node/transaction-stream/1.0.0"

// SyncRequestProtocol defines the protocol ID for DAG synchronization requests.
const SyncRequestProtocol protocol.ID = "/meta-node/sync-request/1.0.0"

// transactionsRequestHandler handles incoming requests for the TransactionsRequestProtocol.
// This handler is typically run on a master node to provide transaction data.
func (mn *ManagedNode) transactionsRequestHandler(stream network.Stream) {
	remotePeerID := stream.Conn().RemotePeer()
	log.Printf("Received TransactionsRequestProtocol stream from peer: %s", remotePeerID)

	defer func() {
		if errClose := stream.Close(); errClose != nil {
			log.Printf("Error closing stream (transactionsRequestHandler) from %s: %v", remotePeerID, errClose)
		} else {
			log.Printf("Closed stream (transactionsRequestHandler) from %s", remotePeerID)
		}
	}()

	// Limit the amount of data read from the stream to prevent abuse.
	limitedReader := io.LimitReader(stream, int64(mn.config.MaxMessageSize))
	requestBytes, err := io.ReadAll(limitedReader)
	if err != nil {
		log.Printf("Error reading data from stream (transactionsRequestHandler) from peer %s: %v", remotePeerID, err)
		_ = stream.Reset() // Reset the stream on error.
		return
	}

	if len(requestBytes) == 0 {
		log.Printf("Empty request payload from peer %s (transactionsRequestHandler).", remotePeerID)
		// Respond with an error if the payload is empty.
		responsePayload := []byte(`{"error": "empty request payload"}`)
		_, writeErr := stream.Write(responsePayload)
		if writeErr != nil {
			log.Printf("Error writing error response (empty request) to stream for peer %s: %v", remotePeerID, writeErr)
		}
		_ = stream.Reset()
		return
	}

	log.Printf("Received request (%d bytes) from %s (TransactionsRequestProtocol): %s", len(requestBytes), remotePeerID, string(requestBytes))

	// --- ACTUAL REQUEST PROCESSING LOGIC GOES HERE ---
	// Based on `requestBytes`, implement the business logic.
	// For example, if requestBytes is `{"action": "get_pending_count"}`,
	// retrieve the count of pending transactions and return it.

	// For demonstration, the Master node sends back a confirmation and echoes part of the request.
	// Replace this with actual transaction data or relevant response.
	responsePayload := []byte(fmt.Sprintf("Master node received and processed your request for TransactionsRequestProtocol. Data received: %s", string(requestBytes)))
	log.Printf("Preparing to send response: %s", string(responsePayload))

	// --- SEND RESPONSE BACK ON THE STREAM ---
	if responsePayload != nil {
		bytesWritten, writeErr := stream.Write(responsePayload)
		if writeErr != nil {
			log.Printf("Error writing response to stream (transactionsRequestHandler) for peer %s: %v", remotePeerID, writeErr)
			_ = stream.Reset()
			return
		}
		log.Printf("Sent response (%d bytes) to peer %s (TransactionsRequestProtocol)", bytesWritten, remotePeerID)
	} else {
		log.Printf("No response prepared to send for peer %s (TransactionsRequestProtocol)", remotePeerID)
	}
}

// SyncRequestPayload defines the structure for an index-based synchronization request.
type SyncRequestPayload struct {
	Action          string            `json:"action"`            // Describes the sync action, e.g., "request_events_based_on_indices".
	RequesterNodeID string            `json:"requester_node_id"` // PubKeyHex of the node requesting synchronization.
	KnownMaxIndices map[string]uint64 `json:"known_max_indices"` // Key: Creator PubKeyHex, Value: Max event index known by the requester.
}

// SyncResponsePayload defines the structure for a synchronization response.
type SyncResponsePayload struct {
	Events               [][]byte          `json:"events"`                 // Slice of marshaled dag.Event objects.
	PartnerLatestIndices map[string]uint64 `json:"partner_latest_indices"` // Optional: Latest indices known by the responding partner.
}

// syncRequestHandler handles incoming DAG synchronization requests (index-based version).
// This handler runs on the node providing events (Node B).
func (mn *ManagedNode) syncRequestHandler(stream network.Stream) {
	remotePeerID := stream.Conn().RemotePeer()
	log.Printf("SYNC_HANDLER: Received SyncRequestProtocol stream from peer: %s", remotePeerID)

	defer func() {
		if errClose := stream.Close(); errClose != nil {
			log.Printf("SYNC_HANDLER: Error closing stream from %s: %v", remotePeerID, errClose)
		} else {
			log.Printf("SYNC_HANDLER: Closed stream from %s", remotePeerID)
		}
	}()

	limitedReader := io.LimitReader(stream, int64(mn.config.MaxMessageSize))
	requestBytes, err := io.ReadAll(limitedReader)
	if err != nil {
		log.Printf("SYNC_HANDLER: Error reading sync request data from peer %s: %v", remotePeerID, err)
		_ = stream.Reset()
		return
	}

	if len(requestBytes) == 0 {
		log.Printf("SYNC_HANDLER: Empty sync request payload from peer %s.", remotePeerID)
		_ = stream.Reset() // It's good practice to reset if the request is malformed or empty.
		return
	}

	log.Printf("SYNC_HANDLER: Received sync request (%d bytes) from %s: %s", len(requestBytes), remotePeerID, string(requestBytes))

	var reqPayload SyncRequestPayload
	if err := json.Unmarshal(requestBytes, &reqPayload); err != nil {
		log.Printf("SYNC_HANDLER: Error unmarshaling sync request from %s: %v. Payload: %s", remotePeerID, err, string(requestBytes))
		// Send an error response back to the client if needed.
		errorResponse := []byte(`{"error": "invalid sync request payload format"}`)
		_, _ = stream.Write(errorResponse) // Best effort to notify the sender.
		_ = stream.Reset()
		return
	}

	log.Printf("SYNC_HANDLER: Parsed sync request from Node ID %s. Action: %s. KnownMaxIndices count: %d",
		reqPayload.RequesterNodeID, reqPayload.Action, len(reqPayload.KnownMaxIndices))

	var eventsToSend []*dag.Event
	var marshaledEventsData [][]byte

	// Retrieve all events from the current node's (Node B - provider) DagStore.
	// Assumes mn.dagStore.GetAllEventsSnapshot() returns events, possibly sorted or in a specific order.
	allEventsInStore := mn.dagStore.GetAllEventsSnapshot() // This method needs to exist in DagStore.

	for _, event := range allEventsInStore {
		creatorHex := hex.EncodeToString(event.EventData.Creator)
		knownMaxIndex, requesterKnowsThisCreator := reqPayload.KnownMaxIndices[creatorHex]

		if !requesterKnowsThisCreator {
			// Requester (Node A) does not know about this creator.
			// Send all events from this creator, or events starting from index 1.
			// For simplicity here, sending the current event if Node A doesn't know the creator.
			// A more robust logic might send all events of this creator that Node B has.
			eventsToSend = append(eventsToSend, event)
		} else {
			// Requester (Node A) knows this creator. Send the event only if its index
			// is greater than the max index Node A already knows for this creator.
			if event.EventData.Index > knownMaxIndex {
				eventsToSend = append(eventsToSend, event)
			}
		}
	}

	log.Printf("SYNC_HANDLER: Preparing to send %d event(s) to %s.", len(eventsToSend), reqPayload.RequesterNodeID)

	for _, event := range eventsToSend {
		eventBytes, errMarshal := event.Marshal() // Use Marshal from dag/event.go
		if errMarshal != nil {
			log.Printf("SYNC_HANDLER: Error marshaling event %s for sync response: %v", event.GetEventId().String(), errMarshal)
			continue // Skip this event if it cannot be marshaled.
		}
		marshaledEventsData = append(marshaledEventsData, eventBytes)
	}

	// Optional: Prepare PartnerLatestIndices from this node's (Node B) DagStore.
	partnerIndices := make(map[string]uint64)
	// Example: Populate partnerIndices with the latest event indices from mn.dagStore.
	// latestEventsMap := mn.dagStore.GetLatestEventsMapSnapshot() // Needs GetLatestEventsMapSnapshot in DagStore.
	// for creatorPubKeyHex, latestEventID := range latestEventsMap {
	//     latestEvent, exists := mn.dagStore.GetEvent(latestEventID)
	//     if exists {
	//         partnerIndices[creatorPubKeyHex] = latestEvent.EventData.Index
	//     }
	// }

	respPayload := SyncResponsePayload{
		Events:               marshaledEventsData,
		PartnerLatestIndices: partnerIndices, // Will be empty if the above optional part is not implemented.
	}

	responsePayloadBytes, err := json.Marshal(respPayload)
	if err != nil {
		log.Printf("SYNC_HANDLER: Error marshaling sync response for %s: %v", remotePeerID, err)
		errorResponse := []byte(`{"error": "internal server error during sync response generation"}`)
		_, _ = stream.Write(errorResponse)
		_ = stream.Reset()
		return
	}

	// Send the response, even if it contains no new events (empty Events slice).
	// This confirms receipt and processing of the sync request.
	bytesWritten, writeErr := stream.Write(responsePayloadBytes)
	if writeErr != nil {
		log.Printf("SYNC_HANDLER: Error writing sync response to stream for peer %s: %v", remotePeerID, writeErr)
		_ = stream.Reset()
		return
	}
	if len(eventsToSend) > 0 {
		log.Printf("SYNC_HANDLER: Sent sync response (%d bytes, %d events) to peer %s", bytesWritten, len(eventsToSend), remotePeerID)
	} else {
		log.Printf("SYNC_HANDLER: Sent empty sync response (no new events, %d bytes) to peer %s", bytesWritten, remotePeerID)
	}
}

// --- Stream Handler Management ---

// RegisterStreamHandler registers a handler for a specific protocol ID.
// If a nil handler is provided, a warning is logged.
func (mn *ManagedNode) RegisterStreamHandler(protoID protocol.ID, handler network.StreamHandler) {
	if handler == nil {
		log.Printf("Warning: Attempted to register a nil stream handler for protocol %s", protoID)
		return
	}
	mn.streamHandlers[protoID] = handler // Store the handler internally.
	// If the host is already initialized, set the handler on the host immediately.
	if mn.host != nil {
		mn.host.SetStreamHandler(protoID, handler)
		log.Printf("Stream handler for %s has been set on the host.", protoID)
	} else {
		// If the host is not yet initialized, the handler will be set when the node starts (mn.Start()).
		log.Printf("Host not yet initialized; stream handler for %s will be set upon node Start().", protoID)
	}
}

// --- Sending Data via Stream ---

// SendRequest opens a new stream to a target peer, sends a message, and reads the response.
// ctx: Context for controlling the stream opening and request duration.
// targetPeerID: The ID of the peer to send the request to.
// protoID: The protocol ID to use for the new stream.
// message: The byte slice containing the message to send.
// Returns the response data and any error encountered.
func (mn *ManagedNode) SendRequest(ctx context.Context, targetPeerID peer.ID, protoID protocol.ID, message []byte) ([]byte, error) {
	if mn.host == nil {
		return nil, errors.New("host not initialized, cannot send request")
	}
	if targetPeerID == mn.host.ID() {
		return nil, errors.New("cannot send request to self")
	}

	// Check current connection status, but NewStream might attempt to connect if not already connected.
	if mn.host.Network().Connectedness(targetPeerID) != network.Connected {
		log.Printf("Warning: Not currently connected to peer %s for sending request via protocol %s. NewStream will attempt connection.", targetPeerID, protoID)
	}

	log.Printf("Opening new stream to %s for protocol %s", targetPeerID, protoID)
	stream, err := mn.host.NewStream(ctx, targetPeerID, protoID)
	if err != nil {
		return nil, fmt.Errorf("failed to open new stream to %s for protocol %s: %w", targetPeerID, protoID, err)
	}
	// Ensure the stream is closed fully when the function exits.
	defer func() {
		if errClose := stream.Close(); errClose != nil {
			log.Printf("Error closing stream (SendRequest defer) to %s: %v", targetPeerID, errClose)
		}
	}()

	log.Printf("Sending request to %s via %s (%d bytes)", targetPeerID, protoID, len(message))
	_, err = stream.Write(message)
	if err != nil {
		_ = stream.Reset() // Reset the stream if writing fails.
		return nil, fmt.Errorf("failed to write to stream to %s: %w", targetPeerID, err)
	}

	// Close the write side of the stream to signal the server that the client is done sending.
	err = stream.CloseWrite()
	if err != nil {
		_ = stream.Reset() // Reset if cannot close write side.
		return nil, fmt.Errorf("failed to close write side of stream to %s: %w", targetPeerID, err)
	}

	log.Printf("Waiting for response from %s for protocol %s...", targetPeerID, protoID)

	// Read the response from the server, limiting its size.
	limitedResponseReader := io.LimitReader(stream, int64(mn.config.MaxMessageSize))
	responseBuffer, err := io.ReadAll(limitedResponseReader)

	// Handle errors during response reading.
	// io.EOF is normal if the server closed the stream after sending its response.
	if err != nil && err != io.EOF {
		return responseBuffer, fmt.Errorf("error reading response from stream of %s: %w (read %d bytes)", targetPeerID, err, len(responseBuffer))
	}

	log.Printf("Received response from %s (%d bytes) for protocol %s", targetPeerID, len(responseBuffer), protoID)
	return responseBuffer, nil
}

/*
// Note: The following are stubs/reminders for methods that might be needed in ManagedNode or DagStore.
// Ensure these are implemented or available if the logic in this file depends on them.

// Example of how ManagedNode might access DagStore (assuming mn.dagStore is a *dag.DagStore).
func (mn *ManagedNode) GetDagStore() *dag.DagStore {
    return mn.dagStore
}
*/

/*
// Note: The following methods GetAllEventsSnapshot() and GetLatestEventsMapSnapshot()
// are assumed to exist in the DagStore (e.g., consensus/dag/dag.go).

// Example in consensus/dag/dag.go:
func (ds *DagStore) GetAllEventsSnapshot() []*Event {
    ds.mu.RLock()
    defer ds.mu.RUnlock()
    events := make([]*Event, 0, len(ds.events))
    for _, event := range ds.events {
        events = append(events, event)
    }
    // Sorting might be necessary here if the synchronization logic relies on a specific order.
    // Example: sort.Slice(events, func(i, j int) bool {
    //     if events[i].EventData.Frame != events[j].EventData.Frame {
    //         return events[i].EventData.Frame < events[j].EventData.Frame
    //     }
    //     return events[i].EventData.Timestamp < events[j].EventData.Timestamp
    // })
    return events
}

func (ds *DagStore) GetLatestEventsMapSnapshot() map[string]EventID {
    ds.mu.RLock()
    defer ds.mu.RUnlock()
    latestCopy := make(map[string]EventID, len(ds.latestEvents))
    for k, v := range ds.latestEvents {
        latestCopy[k] = v
    }
    return latestCopy
}
*/
