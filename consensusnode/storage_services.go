package consensusnode

import (
	"fmt"
	"log"
	// The placeholder import "github.com/meta-node-blockchain/meta-node/pkg/storage"
	// has been removed as it was commented out and not actively used.
	// If a specific storage package is intended, it should be imported directly.
)

// --- Topic Storage Management ---

// AddTopicStorage adds or updates a storage instance for a specific topic.
// If a nil storageInstance is provided, a warning is logged, and no action is taken.
func (mn *ManagedNode) AddTopicStorage(topicName string, storageInstance YourStorageInterface) {
	if storageInstance == nil {
		log.Printf("Warning: Attempted to add nil storage for topic: %s", topicName)
		return
	}
	// mn.topicStorageMap is a sync.Map.
	mn.topicStorageMap.Store(topicName, storageInstance)
	log.Printf("Added/Updated storage for topic: %s", topicName)
}

// GetTopicStorage retrieves the storage instance associated with a topic.
// It returns the storage instance and a boolean indicating whether the instance was found.
// If a value is found but is not of the expected YourStorageInterface type, an error is logged,
// and (nil, false) is returned.
func (mn *ManagedNode) GetTopicStorage(topicName string) (YourStorageInterface, bool) {
	value, loaded := mn.topicStorageMap.Load(topicName)
	if !loaded {
		return nil, false // Storage for the topic not found.
	}
	storageInstance, ok := value.(YourStorageInterface)
	if !ok {
		// This indicates a programming error where a wrong type was stored in the map.
		log.Printf("Error: Invalid data type stored in topicStorageMap for topic %s (expected YourStorageInterface, got %T)", topicName, value)
		return nil, false
	}
	return storageInstance, true
}

// RemoveTopicStorage removes the storage instance reference associated with a topic.
// Note: This function DOES NOT close the storage instance's connection or resources.
// Closing must be managed externally by the component that provided the storage instance.
// Returns an error if no storage is found for the given topicName.
func (mn *ManagedNode) RemoveTopicStorage(topicName string) error {
	if _, loaded := mn.topicStorageMap.Load(topicName); !loaded {
		return fmt.Errorf("storage for topic '%s' not found", topicName)
	}
	mn.topicStorageMap.Delete(topicName)
	log.Printf("Removed storage reference for topic: %s", topicName)
	return nil
}

// --- Cache Management ---

// GetValueFromCache retrieves a value from the LRU cache using a key.
// It returns the value (as a byte slice) and a boolean indicating whether the key was found.
// If the cache is not initialized, it returns (nil, false).
func (mn *ManagedNode) GetValueFromCache(key string) ([]byte, bool) {
	if mn.keyValueCache == nil {
		// This indicates the ManagedNode might not have been fully initialized.
		log.Println("Warning: GetValueFromCache called but keyValueCache is nil.")
		return nil, false
	}
	return mn.keyValueCache.Get(key)
}

// AddValueToCache adds a key-value pair to the LRU cache.
// It returns true if the item was added successfully, false otherwise (e.g., if the cache is not initialized).
func (mn *ManagedNode) AddValueToCache(key string, value []byte) bool {
	if mn.keyValueCache == nil {
		log.Println("Warning: AddValueToCache called but keyValueCache is nil.")
		return false
	}
	// The return value of lru.Cache.Add indicates if an eviction occurred,
	// not necessarily if the add itself "failed" in a recoverable way other than cache being full.
	// For simplicity, we return its direct result, assuming `Add` itself doesn't error.
	return mn.keyValueCache.Add(key, value)
}

// PurgeCache removes all items from the LRU cache.
// If the cache is not initialized, this function does nothing.
func (mn *ManagedNode) PurgeCache() {
	if mn.keyValueCache != nil {
		mn.keyValueCache.Purge()
		log.Println("LRU cache has been purged.")
	} else {
		log.Println("Warning: PurgeCache called but keyValueCache is nil.")
	}
}
