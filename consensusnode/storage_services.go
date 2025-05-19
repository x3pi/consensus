package consensusnode

import (
	"fmt"
	"log"
	// "github.com/meta-node-blockchain/meta-node/pkg/storage" // Placeholder
)

// --- Quản lý Topic Storage ---

// AddTopicStorage thêm hoặc cập nhật một storage instance cho một topic cụ thể.
func (mn *ManagedNode) AddTopicStorage(topicName string, storageInstance YourStorageInterface) {
	if storageInstance == nil {
		log.Printf("Cảnh báo: Đã cố gắng thêm storage nil cho topic: %s", topicName)
		return
	}
	mn.topicStorageMap.Store(topicName, storageInstance)
	log.Printf("Đã thêm/cập nhật storage cho topic: %s", topicName)
}

// GetTopicStorage lấy storage instance liên kết với một topic.
func (mn *ManagedNode) GetTopicStorage(topicName string) (YourStorageInterface, bool) {
	value, loaded := mn.topicStorageMap.Load(topicName)
	if !loaded {
		return nil, false
	}
	storageInstance, ok := value.(YourStorageInterface)
	if !ok {
		log.Printf("Lỗi: Kiểu dữ liệu không hợp lệ được lưu trữ trong topicStorageMap cho topic %s (mong đợi YourStorageInterface, nhận được %T)", topicName, value)
		return nil, false
	}
	return storageInstance, true
}

// RemoveTopicStorage xóa storage instance liên kết với một topic.
// Lưu ý: Hàm này KHÔNG đóng kết nối của storage instance. Việc đóng phải được quản lý bên ngoài.
func (mn *ManagedNode) RemoveTopicStorage(topicName string) error {
	if _, loaded := mn.topicStorageMap.Load(topicName); !loaded {
		return fmt.Errorf("storage cho topic '%s' không tìm thấy", topicName)
	}
	mn.topicStorageMap.Delete(topicName)
	log.Printf("Đã xóa tham chiếu storage cho topic: %s", topicName)
	return nil
}

// --- Quản lý Cache ---

// GetValueFromCache lấy giá trị từ LRU cache.
func (mn *ManagedNode) GetValueFromCache(key string) ([]byte, bool) {
	if mn.keyValueCache == nil {
		return nil, false
	}
	return mn.keyValueCache.Get(key)
}

// AddValueToCache thêm giá trị vào LRU cache.
func (mn *ManagedNode) AddValueToCache(key string, value []byte) bool {
	if mn.keyValueCache == nil {
		return false
	}
	return mn.keyValueCache.Add(key, value)
}

// PurgeCache xóa tất cả các mục khỏi cache.
func (mn *ManagedNode) PurgeCache() {
	if mn.keyValueCache != nil {
		mn.keyValueCache.Purge()
		log.Println("LRU cache đã được xóa.")
	}
}
