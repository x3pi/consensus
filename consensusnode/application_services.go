package consensusnode

import (
	"context"
	"errors"
	"log"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// --- Application-Specific Logic ---

// SendRequestToMasterNode finds a peer of type "master" and sends a request to it.
// It prioritizes the MasterNodeAddress from the configuration. If not available or
// connection fails, it searches for a connected peer explicitly marked as "master".
// The function waits for a response and logs it.
func (mn *ManagedNode) SendRequestToMasterNode(ctx context.Context, protoID protocol.ID, message []byte) ([]byte, error) {
	var targetPeerID peer.ID
	var masterAddrInfo *peer.AddrInfo // Declare here for potential reuse.

	// Prioritize using MasterNodeAddress from configuration.
	if mn.config.MasterNodeAddress != "" {
		addrInfo, err := peer.AddrInfoFromString(mn.config.MasterNodeAddress)
		if err != nil {
			log.Printf("Error: Could not parse MasterNodeAddress from config '%s': %v", mn.config.MasterNodeAddress, err)
			// Do not return error immediately; try finding a master in connected peers.
		} else {
			masterAddrInfo = addrInfo
			targetPeerID = masterAddrInfo.ID
			log.Printf("Using MasterNodeAddress from configuration: %s", targetPeerID)

			// Ensure this peer is marked as "master" in mn.peers.
			// AddKnownPeer (in peer_manager.go) adds or updates this peer's info with Type="master".
			// This helps selectConsensusPartner identify and potentially exclude this peer.
			if addPeerErr := mn.AddKnownPeer(mn.config.MasterNodeAddress, "master"); addPeerErr != nil { //
				log.Printf("Warning: Failed to add/update MasterNodeAddress '%s' as type 'master' in known peers: %v", mn.config.MasterNodeAddress, addPeerErr)
				// Continue, but selectConsensusPartner might not correctly identify this peer if it wasn't previously known.
			} else {
				log.Printf("Ensured MasterNodeAddress '%s' (PeerID: %s) is marked as 'master' in known peers.", mn.config.MasterNodeAddress, targetPeerID)
			}

			// Check if connected to the configured master node and attempt connection if not.
			if mn.host.Network().Connectedness(targetPeerID) != network.Connected {
				log.Printf("Warning: Not connected to configured master node (%s). Attempting connection...", targetPeerID)
				// Add address to peerstore so libp2p knows how to connect.
				mn.host.Peerstore().AddAddrs(masterAddrInfo.ID, masterAddrInfo.Addrs, peerstore.PermanentAddrTTL) //
				if err := mn.host.Connect(ctx, *masterAddrInfo); err != nil {
					log.Printf("Failed to connect to configured master node %s: %v", targetPeerID, err)
					// If connection fails, reset targetPeerID to try finding another master from connected peers.
					targetPeerID = ""
				} else {
					log.Printf("Successfully connected to configured master node: %s", targetPeerID)
					// After successful connection, ConnectedF in setupConnectionNotifier will be called.
					// Since AddKnownPeer was called above with type "master",
					// updatePeerStatus in ConnectedF should respect or use this type.
				}
			}
		}
	}

	// If MasterNodeAddress was not configured or connection failed,
	// find a connected "master" peer (already marked with type "master").
	if targetPeerID == "" {
		log.Printf("MasterNodeAddress not configured or connection failed. Searching for a master peer in connected list...")
		mn.peerMutex.RLock()
		for pid, pInfo := range mn.peers {
			if pInfo.Type == "master" && pInfo.Status == PeerConnected {
				targetPeerID = pid
				// Update masterAddrInfo if found this way and not set from config.
				if masterAddrInfo == nil && len(pInfo.Addresses) > 0 {
					// Use the first address as an example; better address selection logic might be needed.
					addrCopy := pInfo.Addresses[0]
					masterAddrInfo = &addrCopy
				}
				log.Printf("Found connected master peer: %s (Type: %s)", targetPeerID, pInfo.Type)
				break
			}
		}
		mn.peerMutex.RUnlock()
	}

	if targetPeerID == "" {
		return nil, errors.New("no master peer found (neither from config nor currently connected) to send request")
	}
	if masterAddrInfo == nil {
		// This should ideally not happen if targetPeerID was found, but as a safeguard:
		return nil, errors.New("no address information available for the selected master peer")
	}

	log.Printf("Sending request to master peer %s via protocol %s", targetPeerID, protoID)

	// Call mn.SendRequest (from stream_manager.go) to send the request and await response.
	responseData, err := mn.SendRequest(ctx, targetPeerID, protoID, message)

	if err != nil {
		log.Printf("Error sending request or receiving response from master node %s: %v", targetPeerID, err)
		return nil, err
	}

	if responseData != nil {
		log.Printf("Received response from master node %s (%d bytes): %s", targetPeerID, len(responseData), string(responseData))
	} else {
		log.Printf("Received nil response (no error) from master node %s", targetPeerID)
	}

	return responseData, nil
}

// --- Fee Address Management ---

// SetFeeAddresses safely updates the list of fee addresses.
func (mn *ManagedNode) SetFeeAddresses(addresses []string) {
	mn.feeAddressesMux.Lock()
	defer mn.feeAddressesMux.Unlock()

	mn.feeAddresses = make([]string, len(addresses))
	copy(mn.feeAddresses, addresses)
	log.Printf("Updated FeeAddresses: %v", mn.feeAddresses)
}

// GetFeeAddresses returns a copy of the current list of fee addresses.
func (mn *ManagedNode) GetFeeAddresses() []string {
	mn.feeAddressesMux.RLock()
	defer mn.feeAddressesMux.RUnlock()

	addressesCopy := make([]string, len(mn.feeAddresses))
	copy(addressesCopy, mn.feeAddresses)
	return addressesCopy
}

// --- Transaction Channel and Block Fetching Status ---

// GetTransactionChan returns the transaction channel (read-only).
// This prevents external components from closing the channel.
func (mn *ManagedNode) GetTransactionChan() <-chan []byte {
	return mn.transactionChan
}

// SendToTransactionChan safely sends data to the transaction channel.
// It returns true if the send was successful, false otherwise (e.g., channel full or node stopping).
func (mn *ManagedNode) SendToTransactionChan(data []byte) bool {
	select {
	case mn.transactionChan <- data:
		return true
	case <-mn.ctx.Done(): // Prevent blocking if the node is stopping.
		log.Println("Cannot send to transaction channel: node is stopping.")
		return false
	default:
		// Channel is full; log or handle as appropriate.
		log.Println("Warning: Transaction channel is full, dropping message.")
		return false
	}
}

// IsFetchingBlock checks if a block is currently being fetched.
func (mn *ManagedNode) IsFetchingBlock(blockNumber uint64) bool {
	_, ok := mn.fetchingBlocks.Load(blockNumber)
	return ok
}

// SetFetchingBlock marks a block as either being fetched or completed.
// Pass status=true if fetching starts, status=false if fetching ends or block is retrieved.
func (mn *ManagedNode) SetFetchingBlock(blockNumber uint64, status bool) {
	if status {
		mn.fetchingBlocks.Store(blockNumber, true)
	} else {
		mn.fetchingBlocks.Delete(blockNumber)
	}
}
