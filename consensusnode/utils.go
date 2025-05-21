package consensusnode

import (
	"encoding/base64"
	"fmt"
	"log"

	"github.com/libp2p/go-libp2p/core/crypto"
)

// The BlockRequestProtocol ID is defined in managed_node.go and should not be duplicated here.
// const BlockRequestProtocol protocol.ID = "/meta-node/block-request/1.0.0"

// loadPrivateKey is a helper function to load a private key from a base64 encoded string
// or generate a new one if the string is empty.
// It uses standard base64 decoding.
func loadPrivateKey(keyStr string) (crypto.PrivKey, error) {
	if keyStr == "" {
		log.Println("No private key found in configuration, generating a new key...")
		// Use Ed25519 as the default key type due to its efficiency and security.
		priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
		if err != nil {
			return nil, fmt.Errorf("could not generate Ed25519 key pair: %w", err)
		}
		// For development/testing purposes, you might want to log the new private key.
		// NEVER log private keys in a production environment.
		// Example:
		// encoded, _ := crypto.MarshalPrivateKey(priv)
		// log.Printf("New private key (base64): %s", base64.StdEncoding.EncodeToString(encoded))
		return priv, nil
	}

	keyBytes, err := base64.StdEncoding.DecodeString(keyStr)
	if err != nil {
		return nil, fmt.Errorf("could not decode base64 private key string: %w", err)
	}
	privKey, err := crypto.UnmarshalPrivateKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal private key: %w", err)
	}
	log.Println("Successfully loaded private key from configuration.")
	return privKey, nil
}

// displayNodeInfo logs essential information about the ManagedNode.
// This method requires the ManagedNode's host to be initialized.
func (mn *ManagedNode) displayNodeInfo() {
	if mn.host == nil {
		log.Println("Host not initialized, cannot display node information.")
		return
	}
	log.Printf("===== Node Information =====")
	log.Printf("Node ID: %s", mn.host.ID())
	log.Printf("Node Type: %s", mn.config.NodeType)
	log.Println("Listening Addresses:")
	if len(mn.host.Addrs()) == 0 {
		log.Println("  (No listening addresses configured or host not fully started)")
	}
	for _, addr := range mn.host.Addrs() {
		log.Printf("  %s/p2p/%s", addr, mn.host.ID())
	}
	log.Printf("==========================")
}
