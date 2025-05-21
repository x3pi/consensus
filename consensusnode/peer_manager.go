package consensusnode

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"

	"github.com/blockchain/consensus/logger" // Custom logger
)

// --- Peer Information and Status ---

// PeerStatus represents the connection status with a peer.
type PeerStatus int

const (
	// PeerDisconnected indicates that the peer is not connected.
	PeerDisconnected PeerStatus = iota
	// PeerConnecting indicates that a connection attempt to the peer is in progress.
	PeerConnecting
	// PeerConnected indicates that the peer is successfully connected.
	PeerConnected
	// PeerFailed indicates that connection attempts to the peer have failed.
	PeerFailed
)

// String returns a string representation of the PeerStatus.
func (s PeerStatus) String() string {
	switch s {
	case PeerDisconnected:
		return "Disconnected"
	case PeerConnecting:
		return "Connecting"
	case PeerConnected:
		return "Connected"
	case PeerFailed:
		return "Failed"
	default:
		return "Unknown"
	}
}

// ManagedPeerInfo contains information about a known or connected peer.
type ManagedPeerInfo struct {
	ID        peer.ID
	Addresses []peer.AddrInfo // Use AddrInfo to store multiple addresses.
	Type      string          // Peer type, e.g., "master", "validator".
	Status    PeerStatus
	LastSeen  time.Time
	LastError error
	// ReconnectAttempts counts the number of consecutive failed reconnect attempts.
	ReconnectAttempts int
	// cancelReconnect is a function to cancel an ongoing reconnection goroutine for this peer.
	cancelReconnect context.CancelFunc
}

// --- Peer Management ---

// AddKnownPeer explicitly adds a peer to the managed list, typically before attempting a connection.
// This function also attempts to extract and store the peer's PubKeyHex in mn.connectedPeerPubKeys
// if it can be derived from the provided address string.
func (mn *ManagedNode) AddKnownPeer(peerAddrStr string, peerType string) error {
	addrInfo, err := peer.AddrInfoFromString(peerAddrStr)
	if err != nil {
		return fmt.Errorf("invalid peer address string %s: %w", peerAddrStr, err)
	}

	// Attempt to extract and store PubKeyHex if available from the AddrInfo.
	if addrInfo.ID != "" { // Ensure peer.ID exists.
		pubKey, pkErr := addrInfo.ID.ExtractPublicKey()
		if pkErr == nil && pubKey != nil {
			rawPubKey, rawErr := pubKey.Raw()
			if rawErr == nil {
				pubKeyHex := hex.EncodeToString(rawPubKey)
				mn.peerPubKeyMutex.Lock()
				mn.connectedPeerPubKeys[addrInfo.ID] = pubKeyHex
				mn.peerPubKeyMutex.Unlock()
				log.Printf("AddKnownPeer: Added/updated PubKeyHex for PeerID %s", addrInfo.ID)
			} else {
				log.Printf("AddKnownPeer: Error getting raw bytes from PublicKey for PeerID %s: %v", addrInfo.ID, rawErr)
			}
		} else {
			// This can happen if the AddrInfo doesn't contain enough info to extract a public key,
			// or if the key type is not supported for direct extraction.
			log.Printf("AddKnownPeer: Could not extract PublicKey from PeerID %s (address: %s): %v", addrInfo.ID, peerAddrStr, pkErr)
		}
	} else {
		log.Printf("AddKnownPeer: PeerID is empty in AddrInfo from address %s, cannot extract public key.", peerAddrStr)
	}

	mn.peerMutex.Lock()
	defer mn.peerMutex.Unlock()

	if pInfo, exists := mn.peers[addrInfo.ID]; exists {
		// Peer already exists, update information if necessary (e.g., type, addresses).
		pInfo.Type = peerType // Update Type.
		// Logic to merge addresses or replace with the latest known address.
		pInfo.Addresses = []peer.AddrInfo{*addrInfo} // Simplistic replacement of addresses.
		log.Printf("Updated information for known peer %s (Type: %s, Address: %s)", addrInfo.ID, peerType, addrInfo.Addrs)
	} else {
		// Peer does not exist, create a new entry.
		mn.peers[addrInfo.ID] = &ManagedPeerInfo{
			ID:        addrInfo.ID,
			Addresses: []peer.AddrInfo{*addrInfo},
			Type:      peerType,
			Status:    PeerDisconnected, // Initial status.
		}
		log.Printf("Added known peer %s (Type: %s, Address: %s)", addrInfo.ID, peerType, addrInfo.Addrs)
	}
	return nil
}

// connectToBootstrapPeers attempts to connect to the configured bootstrap peers.
func (mn *ManagedNode) connectToBootstrapPeers() error {
	var connectWg sync.WaitGroup
	bootstrapPeers := mn.config.BootstrapPeers
	if len(bootstrapPeers) == 0 {
		log.Println("No bootstrap peers configured.")
		return nil
	}

	log.Printf("Connecting to %d bootstrap peer(s)...", len(bootstrapPeers))
	for _, peerAddrStr := range bootstrapPeers {
		if peerAddrStr == "" {
			continue
		}
		addr, err := peer.AddrInfoFromString(peerAddrStr)
		if err != nil {
			log.Printf("Invalid bootstrap peer address %s: %v", peerAddrStr, err)
			continue
		}
		connectWg.Add(1)
		go func(addrInfo peer.AddrInfo) {
			defer connectWg.Done()
			logger.Error("connectToBootstrapPeers attempting to connect to: ", addrInfo) // Original code uses Error level.
			// Assume "bootstrap" type for peers from this list.
			if err := mn.ConnectToPeer(addrInfo, "bootstrap"); err != nil {
				log.Printf("Failed to connect to bootstrap peer %s: %v", addrInfo.ID, err)
			}
		}(*addr)
	}
	connectWg.Wait()
	logger.Info("Bootstrap peer connection process completed. Current peers map:", mn.peers)
	return nil
}

// ConnectToPeer attempts to establish a connection with a specific peer.
func (mn *ManagedNode) ConnectToPeer(peerInfo peer.AddrInfo, peerType string) error {
	if peerInfo.ID == mn.host.ID() {
		log.Printf("Skipping connection attempt to self (%s)", peerInfo.ID)
		return nil
	}

	// Attempt to extract and store PubKeyHex before locking, ensuring it's available early.
	// setupConnectionNotifier will also do this upon successful connection.
	if peerInfo.ID != "" {
		pubKey, pkErr := peerInfo.ID.ExtractPublicKey()
		if pkErr == nil && pubKey != nil {
			rawPubKey, rawErr := pubKey.Raw()
			if rawErr == nil {
				pubKeyHex := hex.EncodeToString(rawPubKey)
				mn.peerPubKeyMutex.Lock()
				mn.connectedPeerPubKeys[peerInfo.ID] = pubKeyHex
				mn.peerPubKeyMutex.Unlock()
				log.Printf("ConnectToPeer: Added/updated PubKeyHex for PeerID %s before connecting.", peerInfo.ID)
			} else {
				log.Printf("ConnectToPeer: Error getting raw bytes from PublicKey for PeerID %s: %v", peerInfo.ID, rawErr)
			}
		} else {
			log.Printf("ConnectToPeer: Could not extract PublicKey from PeerID %s: %v", peerInfo.ID, pkErr)
		}
	} else if len(peerInfo.Addrs) > 0 {
		log.Printf("ConnectToPeer: PeerID is empty in peerInfo for address %s; cannot extract public key beforehand.", peerInfo.Addrs[0])
	} else {
		log.Printf("ConnectToPeer: PeerID is empty and no addresses in peerInfo; cannot extract public key beforehand.")
	}

	mn.peerMutex.Lock()
	p, exists := mn.peers[peerInfo.ID]
	if exists {
		if p.Status == PeerConnecting || p.Status == PeerConnected {
			mn.peerMutex.Unlock()
			// log.Printf("Already connected or connecting to peer %s", peerInfo.ID)
			return nil // Already connected or connection in progress.
		}
		// If peer exists but not in Connecting/Connected state, update its info.
		p.Type = peerType
		p.Addresses = []peer.AddrInfo{peerInfo} // Update address.
		p.Status = PeerConnecting
		p.LastError = nil       // Clear previous error on new attempt.
		p.ReconnectAttempts = 0 // Reset reconnect attempts.
	} else {
		// Peer does not exist, create a new entry.
		mn.peers[peerInfo.ID] = &ManagedPeerInfo{
			ID:        peerInfo.ID,
			Addresses: []peer.AddrInfo{peerInfo},
			Type:      peerType,
			Status:    PeerConnecting,
		}
	}
	// Get a reference to the peer's info (either updated or newly created).
	currentPeerInfo := mn.peers[peerInfo.ID]
	mn.peerMutex.Unlock()

	log.Printf("Attempting to connect to peer %s (Type: %s, Address: %s)", currentPeerInfo.ID, currentPeerInfo.Type, currentPeerInfo.Addresses[0].Addrs)
	// Add all known addresses of the peer to the peerstore.
	// PermanentAddrTTL ensures addresses are not prematurely removed.
	mn.host.Peerstore().AddAddrs(peerInfo.ID, peerInfo.Addrs, peerstore.PermanentAddrTTL)

	// Use the node's context for the connection attempt.
	// A child context with a timeout can be created for finer control.
	// Example: using a timeout twice the PingTimeout.
	connectCtx, cancel := context.WithTimeout(mn.ctx, mn.config.PingTimeout*2)
	defer cancel()

	err := mn.host.Connect(connectCtx, peerInfo) // Use the full peerInfo.
	if err != nil {
		// Update status to failed. peerType is taken from currentPeerInfo.Type set above.
		mn.updatePeerStatus(peerInfo.ID, PeerFailed, err, currentPeerInfo.Type)
		log.Printf("Failed to connect to peer %s: %v", peerInfo.ID, err)
		// Attempt to reconnect if necessary.
		mn.tryReconnectToPeer(peerInfo.ID, currentPeerInfo.Type)
		return fmt.Errorf("failed to connect to peer %s: %w", peerInfo.ID, err)
	}
	// If no error, the connection request has been initiated.
	// The status will be updated to PeerConnected by the connection notifier (ConnectedF).
	// No need to call updatePeerStatus(PeerConnected) here.
	log.Printf("Connection request to peer %s initiated.", peerInfo.ID)
	return nil
}

// updatePeerStatus updates the status of a managed peer.
// peerTypeIfNew is used if the peer is being added to the map for the first time (e.g. inbound connection).
func (mn *ManagedNode) updatePeerStatus(peerID peer.ID, status PeerStatus, err error, peerTypeIfNew string) {
	mn.peerMutex.Lock()
	defer mn.peerMutex.Unlock()

	pInfo, exists := mn.peers[peerID]
	if !exists {
		// If peer is not known, only add it if it's a new successful connection.
		if status == PeerConnected {
			log.Printf("New peer %s connected.", peerID)
			var addrs []peer.AddrInfo
			// Get addresses from active connections to this peer.
			conns := mn.host.Network().ConnsToPeer(peerID)
			if len(conns) > 0 {
				// Use the address from the first connection.
				remoteMultiaddr := conns[0].RemoteMultiaddr()
				addrInfo, _ := peer.AddrInfoFromP2pAddr(remoteMultiaddr)
				if addrInfo != nil {
					addrs = append(addrs, *addrInfo)
				}
			}
			mn.peers[peerID] = &ManagedPeerInfo{
				ID:        peerID,
				Addresses: addrs,
				Type:      peerTypeIfNew, // Use the provided type.
				Status:    status,
				LastSeen:  time.Now(),
			}
		} else {
			// log.Printf("Status update for unknown peer %s ignored (unless it's a new 'Connected' status)", peerID)
		}
		return
	}

	// Only update if status changes or there's a new error.
	if pInfo.Status == status && err == pInfo.LastError {
		if status == PeerConnected { // Update LastSeen even if status is already Connected.
			pInfo.LastSeen = time.Now()
		}
		return
	}

	oldStatus := pInfo.Status
	pInfo.Status = status
	pInfo.LastError = err

	switch status {
	case PeerConnected:
		pInfo.LastSeen = time.Now()
		pInfo.ReconnectAttempts = 0 // Reset attempts on successful connection.
		if pInfo.cancelReconnect != nil {
			pInfo.cancelReconnect() // Cancel any ongoing reconnect attempt.
			pInfo.cancelReconnect = nil
		}
		log.Printf("Peer %s status: %s -> %s", peerID, oldStatus, status)
	case PeerFailed:
		// ReconnectAttempts is incremented in tryReconnectToPeer.
		log.Printf("Peer %s status: %s -> %s (Error: %v, Current Reconnect Attempts: %d)", peerID, oldStatus, status, err, pInfo.ReconnectAttempts)
	case PeerConnecting:
		// Reset reconnect attempts when a new connection cycle is actively started.
		if oldStatus == PeerDisconnected || oldStatus == PeerFailed {
			pInfo.ReconnectAttempts = 0
		}
		log.Printf("Peer %s status: %s -> %s", peerID, oldStatus, status)
	default: // PeerDisconnected or other statuses.
		log.Printf("Peer %s status: %s -> %s", peerID, oldStatus, status)
	}
}

// tryReconnectToPeer starts a new goroutine to attempt reconnection to a peer.
func (mn *ManagedNode) tryReconnectToPeer(peerID peer.ID, peerType string) {
	mn.peerMutex.Lock()
	pInfo, exists := mn.peers[peerID]
	if !exists {
		mn.peerMutex.Unlock()
		log.Printf("Peer %s not found for reconnection.", peerID)
		return
	}
	// If a reconnect attempt is already in progress, or peer is connected, do nothing.
	if pInfo.cancelReconnect != nil {
		mn.peerMutex.Unlock()
		// log.Printf("Reconnection process already active for peer %s.", peerID)
		return
	}
	if pInfo.Status == PeerConnected {
		mn.peerMutex.Unlock()
		return
	}

	reconnectCtx, cancel := context.WithCancel(mn.ctx) // Create a cancellable context for this attempt.
	pInfo.cancelReconnect = cancel
	// The goroutine itself will set status to PeerConnecting to avoid race conditions
	// if it doesn't start immediately.
	mn.peerMutex.Unlock()

	mn.reconnectWG.Add(1)
	go func(pID peer.ID, pType string, currentReconnectCtx context.Context) {
		defer mn.reconnectWG.Done()
		defer func() { // Cleanup when the goroutine exits.
			mn.peerMutex.Lock()
			if pi, ok := mn.peers[pID]; ok {
				pi.cancelReconnect = nil // Clear the cancel function.
				// If still 'Connecting' when goroutine ends, it means the overall attempt might have timed out or failed.
				if pi.Status == PeerConnecting {
					pi.Status = PeerDisconnected // Revert to Disconnected if stuck in Connecting.
					log.Printf("Reconnection goroutine for peer %s ended while status was Connecting; set to Disconnected.", pID)
				}
			}
			mn.peerMutex.Unlock()
		}()

		// Set status to Connecting when the goroutine begins its work.
		mn.updatePeerStatus(pID, PeerConnecting, nil, pType)

		delay := mn.config.InitialReconnectDelay
		maxAttempts := mn.config.MaxReconnectAttempts
		if maxAttempts <= 0 { // If 0 or negative, treat as (near) infinite attempts.
			maxAttempts = 1000 // A large number, or use math.MaxInt32 cautiously.
			log.Printf("Peer %s: MaxReconnectAttempts configured for (near) infinite retries (set to %d).", pID, maxAttempts)
		}

		// Get current attempts from peer info to resume, if any.
		currentAttempt := 0
		mn.peerMutex.RLock()
		if pi, ok := mn.peers[pID]; ok {
			currentAttempt = pi.ReconnectAttempts
		}
		mn.peerMutex.RUnlock()

		for attempt := currentAttempt; attempt < maxAttempts; attempt++ {
			select {
			case <-currentReconnectCtx.Done(): // Check if reconnection was cancelled (e.g., node stopping).
				log.Printf("Reconnection process for peer %s cancelled.", pID)
				return
			default:
			}

			mn.peerMutex.Lock()
			if pi, ok := mn.peers[pID]; ok {
				pi.ReconnectAttempts = attempt + 1 // Update actual attempt count.
			}
			mn.peerMutex.Unlock()

			log.Printf("Attempting to reconnect to peer %s (Type: %s, Attempt: %d/%d, Delay: %s)", pID, pType, attempt+1, maxAttempts, delay)

			// Get the target address info.
			mn.peerMutex.RLock()
			var targetAddrInfo *peer.AddrInfo
			if pi, ok := mn.peers[pID]; ok && len(pi.Addresses) > 0 {
				// Use a copy of the address info.
				addrCopy := pi.Addresses[0] // Assuming first address is primary.
				targetAddrInfo = &addrCopy
			}
			mn.peerMutex.RUnlock()

			if targetAddrInfo == nil {
				log.Printf("No address information available to reconnect to peer %s.", pID)
				mn.updatePeerStatus(pID, PeerFailed, errors.New("no address for reconnection"), pType)
				return
			}

			// Add addresses to peerstore again before connecting.
			mn.host.Peerstore().AddAddrs(targetAddrInfo.ID, targetAddrInfo.Addrs, peerstore.PermanentAddrTTL)

			// Use a timeout for the connection attempt.
			connectCtx, connectCancel := context.WithTimeout(currentReconnectCtx, mn.config.PingTimeout*3) // Increased timeout for connect.
			err := mn.host.Connect(connectCtx, *targetAddrInfo)
			connectCancel()

			if err == nil {
				log.Printf("Successfully reconnected to peer %s.", pID)
				// Connection notifier will update status and clear cancelReconnect.
				return
			}

			log.Printf("Reconnect attempt to peer %s failed: %v", pID, err)
			mn.updatePeerStatus(pID, PeerFailed, err, pType) // Update status to reflect this failure.

			// Wait for the delay period before next attempt, or if context is cancelled.
			select {
			case <-currentReconnectCtx.Done():
				log.Printf("Reconnection process for peer %s cancelled while waiting for delay.", pID)
				return
			case <-time.After(delay):
				// Apply exponential backoff with jitter.
				jitter := time.Duration(rand.Int63n(int64(delay) / 5)) // Jitter up to 20%.
				// Increase delay, ensuring it doesn't exceed MaxReconnectDelay.
				delay = time.Duration(math.Min(float64(mn.config.MaxReconnectDelay), float64(delay)*1.8)) + jitter
			}
		}
		log.Printf("Gave up reconnecting to peer %s after %d attempts.", pID, maxAttempts)
		mn.updatePeerStatus(pID, PeerDisconnected, fmt.Errorf("gave up after %d attempts", maxAttempts), pType)
	}(peerID, peerType, reconnectCtx)
}

// peerHealthMonitor periodically checks the health of connected peers.
func (mn *ManagedNode) peerHealthMonitor() {
	defer mn.wg.Done()
	if mn.config.PingInterval <= 0 {
		log.Println("Peer health monitor disabled (PingInterval <= 0).")
		return
	}
	ticker := time.NewTicker(mn.config.PingInterval)
	defer ticker.Stop()

	log.Println("Peer health monitor started.")
	for {
		select {
		case <-mn.ctx.Done(): // Node context cancelled.
			log.Println("Peer health monitor stopping.")
			return
		case <-ticker.C:
			mn.checkAllPeerConnections()
		}
	}
}

// checkAllPeerConnections iterates through managed peers and verifies their connectivity.
func (mn *ManagedNode) checkAllPeerConnections() {
	mn.peerMutex.RLock()
	// Create a slice of peers to check to avoid holding lock during network calls.
	var peersToCheck []struct {
		id         peer.ID
		ty         string // type
		stat       PeerStatus
		cancelFunc context.CancelFunc // To check if reconnect is already active
	}
	for pid, pinfo := range mn.peers {
		peersToCheck = append(peersToCheck, struct {
			id         peer.ID
			ty         string
			stat       PeerStatus
			cancelFunc context.CancelFunc
		}{pid, pinfo.Type, pinfo.Status, pinfo.cancelReconnect})
	}
	mn.peerMutex.RUnlock()

	// log.Printf("Performing health check for %d peer(s)...", len(peersToCheck))
	connectedCount := 0
	for _, p := range peersToCheck {
		if mn.host.Network().Connectedness(p.id) == network.Connected {
			// If libp2p says connected, but our status is different, update it.
			if p.stat != PeerConnected {
				mn.updatePeerStatus(p.id, PeerConnected, nil, p.ty)
			}
			connectedCount++
		} else {
			// If libp2p says not connected.
			// Only try to reconnect if not already connecting/reconnecting and not recently failed.
			if p.stat != PeerConnecting && p.cancelFunc == nil {
				log.Printf("Peer %s detected as disconnected during health check (current status: %s). Initiating reconnect.", p.id, p.stat)
				mn.updatePeerStatus(p.id, PeerDisconnected, errors.New("disconnected on health check"), p.ty)
				mn.tryReconnectToPeer(p.id, p.ty)
			}
		}
	}
	// log.Printf("Peer health check complete. Connected peers: %d/%d", connectedCount, len(peersToCheck))
}

// shouldReconnect determines if a reconnection attempt should be made for a peer of a given type.
func shouldReconnect(peerType string, config NodeConfig) bool {
	// Example: always reconnect to "master", "bootstrap", "validator" types.
	// This can be expanded based on configuration or specific application needs.
	switch peerType {
	case "master", "bootstrap", "validator":
		return true
	default:
		// Do not automatically reconnect to other peer types unless specified.
		return false
	}
}

// GetPeersInfo returns a snapshot of the current peer information.
// It creates a deep copy of the peer data to ensure thread safety for the caller.
func (mn *ManagedNode) GetPeersInfo() map[peer.ID]ManagedPeerInfo {
	mn.peerMutex.RLock()
	defer mn.peerMutex.RUnlock()

	peersCopy := make(map[peer.ID]ManagedPeerInfo, len(mn.peers))
	for id, infoPtr := range mn.peers {
		// Dereference the pointer to copy the struct.
		infoCopy := *infoPtr
		// Deep copy slices within the struct if any (e.g., Addresses).
		if len(infoPtr.Addresses) > 0 {
			infoCopy.Addresses = make([]peer.AddrInfo, len(infoPtr.Addresses))
			copy(infoCopy.Addresses, infoPtr.Addresses)
		}
		// Note: cancelReconnect is a func, cannot be easily deep-copied if needed by caller,
		// but GetPeersInfo is typically for informational display.
		peersCopy[id] = infoCopy
	}
	return peersCopy
}

// cancelAllReconnects cancels all active peer reconnection goroutines.
// This is typically called when the node is shutting down.
func (mn *ManagedNode) cancelAllReconnects() {
	mn.peerMutex.Lock() // Lock to safely iterate and modify pInfo.cancelReconnect.
	defer mn.peerMutex.Unlock()
	log.Println("Cancelling all pending peer reconnection attempts...")
	for _, pInfo := range mn.peers {
		if pInfo.cancelReconnect != nil {
			pInfo.cancelReconnect()
			// pInfo.cancelReconnect will be set to nil by the defer func in the goroutine itself.
		}
	}
}
