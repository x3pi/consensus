package consensusnode

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// --- Pub/Sub ---

// SubscribeAndHandle đăng ký một topic và thiết lập một handler cho các message đến.
// Hàm này nên được gọi bởi người dùng của ManagedNode để đăng ký các topic cụ thể.
func (mn *ManagedNode) SubscribeAndHandle(topicName string, handler func(msg *pubsub.Message)) error {
	if handler == nil {
		return errors.New("pubsub message handler không được là nil")
	}

	mn.peerMutex.Lock() // Sử dụng peerMutex để bảo vệ topicSubscriptions và topicHandlers
	if _, exists := mn.topicSubscriptions[topicName]; exists {
		mn.peerMutex.Unlock()
		return fmt.Errorf("đã đăng ký topic %s rồi", topicName)
	}
	// Đánh dấu rằng chúng ta sẽ đăng ký topic này, để tránh race condition nếu gọi Start ngay sau đó
	mn.topicSubscriptions[topicName] = nil // Placeholder
	mn.topicHandlers[topicName] = handler  // Lưu trữ handler
	mn.peerMutex.Unlock()

	topic, err := mn.pubsub.Join(topicName)
	if err != nil {
		mn.peerMutex.Lock()
		delete(mn.topicSubscriptions, topicName) // Xóa placeholder nếu join thất bại
		delete(mn.topicHandlers, topicName)
		mn.peerMutex.Unlock()
		return fmt.Errorf("không thể tham gia topic %s: %w", topicName, err)
	}

	sub, err := topic.Subscribe()
	if err != nil {
		_ = topic.Close() // Cố gắng đóng topic nếu không subscribe được
		mn.peerMutex.Lock()
		delete(mn.topicSubscriptions, topicName)
		delete(mn.topicHandlers, topicName)
		mn.peerMutex.Unlock()
		return fmt.Errorf("không thể đăng ký topic %s: %w", topicName, err)
	}

	mn.peerMutex.Lock()
	mn.topicSubscriptions[topicName] = sub // Cập nhật subscription thực tế
	mn.peerMutex.Unlock()

	mn.wg.Add(1)
	go func(currentSub *pubsub.Subscription, currentTopic *pubsub.Topic, currentTopicName string, currentHandler func(msg *pubsub.Message)) {
		defer mn.wg.Done()
		defer func() { // Dọn dẹp khi goroutine thoát
			currentSub.Cancel() // Hủy subscription
			// Không đóng topic ở đây vì nó có thể vẫn đang được sử dụng bởi các publisher hoặc subscriber khác.
			// Topic sẽ được dọn dẹp khi PubSub service đóng hoặc không còn ai tham chiếu.
			// _ = currentTopic.Close() // Cân nhắc việc đóng topic ở đây có thể gây vấn đề nếu publish vẫn cần topic này.

			mn.peerMutex.Lock()
			delete(mn.topicSubscriptions, currentTopicName)
			// Không xóa topicHandlers ở đây, vì nó được dùng để khởi tạo lại nếu cần
			mn.peerMutex.Unlock()
			log.Printf("Đã dừng và dọn dẹp handler cho topic %s", currentTopicName)
		}()

		log.Printf("Đang lắng nghe message trên topic %s", currentTopicName)
		for {
			select {
			case <-mn.ctx.Done(): // Node context bị hủy
				log.Printf("Node context bị hủy, dừng message handler cho topic %s", currentTopicName)
				return
			default:
				// Sử dụng context của node (mn.ctx) cho Next() để đảm bảo nó bị hủy khi node dừng
				msg, err := currentSub.Next(mn.ctx)

				if err != nil {
					// Kiểm tra các lỗi cho thấy subscription hoặc context đã bị hủy
					if errors.Is(err, context.Canceled) || errors.Is(err, mn.ctx.Err()) || errors.Is(err, pubsub.ErrSubscriptionCancelled) || errors.Is(err, pubsub.ErrTopicClosed) {
						log.Printf("Subscription context cho topic '%s' đã đóng hoặc có lỗi: %v. Handler sẽ thoát.", currentTopicName, err)
						return
					}
					// Các lỗi khác có thể là tạm thời
					log.Printf("Lỗi khi nhận message từ topic %s: %v", currentTopicName, err)
					// Đối với các lỗi khác, có thể tạm dừng một chút để tránh vòng lặp quá nhanh
					select {
					case <-mn.ctx.Done(): // Kiểm tra lại trước khi sleep
						return
					case <-time.After(200 * time.Millisecond):
					}
					continue
				}
				if msg.ReceivedFrom == mn.host.ID() {
					continue // Bỏ qua message từ chính mình
				}
				currentHandler(msg) // Xử lý message
			}
		}
	}(sub, topic, topicName, handler)
	return nil
}

// PublishMessage xuất bản một message tới một topic cụ thể.
func (mn *ManagedNode) PublishMessage(topicName string, data []byte) error {
	if mn.pubsub == nil {
		return errors.New("pubsub service chưa được khởi tạo")
	}

	// Bước 1: Tham gia (Join) topic để lấy đối tượng Topic.
	// Join sẽ trả về topic đã tồn tại nếu đã tham gia trước đó.
	topic, err := mn.pubsub.Join(topicName)
	if err != nil {
		return fmt.Errorf("không thể tham gia topic %s để publish: %w", topicName, err)
	}
	// Không cần đóng topic ở đây vì nó có thể được sử dụng cho các lần publish hoặc subscribe khác.
	// PubSub sẽ quản lý vòng đời của topic.

	log.Printf("Đang xuất bản message tới topic %s (%d bytes)", topicName, len(data))

	// Sử dụng context của node với timeout cho Publish
	publishCtx, cancel := context.WithTimeout(mn.ctx, 10*time.Second) // Timeout 10 giây cho publish
	defer cancel()

	// Bước 2: Xuất bản message trên đối tượng Topic đã lấy được.
	return topic.Publish(publishCtx, data)
}

// UnsubscribeFromTopic hủy đăng ký khỏi một topic.
func (mn *ManagedNode) UnsubscribeFromTopic(topicName string) error {
	mn.peerMutex.Lock()
	defer mn.peerMutex.Unlock()

	sub, exists := mn.topicSubscriptions[topicName]
	if !exists {
		return fmt.Errorf("chưa đăng ký topic %s", topicName)
	}

	sub.Cancel() // Hủy subscription
	// Goroutine xử lý message sẽ tự động thoát và thực hiện dọn dẹp (bao gồm cả việc có thể đóng topic nếu cần).

	delete(mn.topicSubscriptions, topicName)
	// Cân nhắc việc xóa cả topicHandler nếu không muốn tự động đăng ký lại
	// delete(mn.topicHandlers, topicName)

	log.Printf("Đã hủy đăng ký khỏi topic %s", topicName)
	return nil
}
