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

// SubscribeAndHandle subscribes to a given topic and sets up a handler for incoming messages.
// This function should be called by users of ManagedNode to register for specific topics.
// It ensures that the node is not already subscribed to the topic and manages the lifecycle
// of the subscription and its handler goroutine.
func (mn *ManagedNode) SubscribeAndHandle(topicName string, handler func(msg *pubsub.Message)) error {
	if handler == nil {
		return errors.New("pubsub message handler cannot be nil")
	}

	mn.peerMutex.Lock() // Use peerMutex to protect topicSubscriptions and topicHandlers.
	if _, exists := mn.topicSubscriptions[topicName]; exists {
		mn.peerMutex.Unlock()
		return fmt.Errorf("already subscribed to topic %s", topicName)
	}
	// Mark that we are about to subscribe to this topic to prevent race conditions
	// if Start is called immediately after.
	mn.topicSubscriptions[topicName] = nil // Placeholder for the subscription.
	mn.topicHandlers[topicName] = handler  // Store the handler.
	mn.peerMutex.Unlock()

	// Join the topic. This will return an existing topic if already joined by the PubSub service.
	topic, err := mn.pubsub.Join(topicName)
	if err != nil {
		mn.peerMutex.Lock()
		delete(mn.topicSubscriptions, topicName) // Remove placeholder if join fails.
		delete(mn.topicHandlers, topicName)      // Remove handler if join fails.
		mn.peerMutex.Unlock()
		return fmt.Errorf("failed to join topic %s: %w", topicName, err)
	}

	// Create a new subscription to the topic.
	sub, err := topic.Subscribe()
	if err != nil {
		// Attempt to close the topic if subscription fails. This might not always be necessary
		// as PubSub manages topic lifecycle, but can help clean up resources if this node
		// was the one causing the topic to be joined.
		_ = topic.Close()
		mn.peerMutex.Lock()
		delete(mn.topicSubscriptions, topicName) // Clean up on failure.
		delete(mn.topicHandlers, topicName)
		mn.peerMutex.Unlock()
		return fmt.Errorf("failed to subscribe to topic %s: %w", topicName, err)
	}

	mn.peerMutex.Lock()
	mn.topicSubscriptions[topicName] = sub // Store the actual subscription.
	mn.peerMutex.Unlock()

	mn.wg.Add(1) // Increment WaitGroup counter for the message handling goroutine.
	go func(currentSub *pubsub.Subscription, currentTopic *pubsub.Topic, currentTopicName string, currentHandler func(msg *pubsub.Message)) {
		defer mn.wg.Done() // Decrement counter when goroutine exits.
		defer func() {     // Cleanup logic for when this goroutine exits.
			currentSub.Cancel() // Cancel the subscription.

			// Do not close the topic here (currentTopic.Close()) as it might still be in use
			// by other publishers or subscribers on this node or across the network.
			// The PubSub service manages the lifecycle of topics; they are cleaned up
			// when the PubSub service closes or when they are no longer referenced.
			// Deliberately not closing `currentTopic` here to avoid issues if publishes still need it.

			mn.peerMutex.Lock()
			delete(mn.topicSubscriptions, currentTopicName)
			// Do not delete topicHandlers here; it's used for potential re-initialization if needed.
			mn.peerMutex.Unlock()
			log.Printf("Stopped and cleaned up handler for topic %s", currentTopicName)
		}()

		log.Printf("Listening for messages on topic %s", currentTopicName)
		for {
			select {
			case <-mn.ctx.Done(): // Node context has been cancelled.
				log.Printf("Node context cancelled, stopping message handler for topic %s", currentTopicName)
				return
			default:
				// Use the node's context (mn.ctx) for Next() to ensure it's cancellable
				// when the node stops.
				msg, err := currentSub.Next(mn.ctx)

				if err != nil {
					// Check for errors indicating the subscription or context was cancelled.
					if errors.Is(err, context.Canceled) || errors.Is(err, mn.ctx.Err()) ||
						errors.Is(err, pubsub.ErrSubscriptionCancelled) || errors.Is(err, pubsub.ErrTopicClosed) {
						log.Printf("Subscription context for topic '%s' closed or encountered an error: %v. Handler will exit.", currentTopicName, err)
						return
					}
					// Other errors might be transient.
					log.Printf("Error receiving message from topic %s: %v", currentTopicName, err)
					// For other errors, pause briefly to avoid tight error loops.
					select {
					case <-mn.ctx.Done(): // Re-check context before sleeping.
						return
					case <-time.After(200 * time.Millisecond):
						// Continue to the next iteration after a short delay.
					}
					continue
				}

				// Ignore messages sent by self.
				if msg.ReceivedFrom == mn.host.ID() {
					continue
				}
				currentHandler(msg) // Process the message using the provided handler.
			}
		}
	}(sub, topic, topicName, handler)
	return nil
}

// PublishMessage publishes a message to a specific topic.
func (mn *ManagedNode) PublishMessage(topicName string, data []byte) error {
	if mn.pubsub == nil {
		return errors.New("pubsub service is not initialized")
	}

	// Step 1: Join the topic to get the Topic object.
	// Join will return an existing topic if already joined by this node, or create it.
	topic, err := mn.pubsub.Join(topicName)
	if err != nil {
		return fmt.Errorf("failed to join topic %s for publishing: %w", topicName, err)
	}
	// Do not close the topic here; it might be used for other publishes or subscriptions.
	// PubSub service manages the lifecycle of the topic.

	log.Printf("Publishing message to topic %s (%d bytes)", topicName, len(data))

	// Use the node's context with a timeout for the Publish operation.
	publishCtx, cancel := context.WithTimeout(mn.ctx, 10*time.Second) // 10-second timeout for publish.
	defer cancel()

	// Step 2: Publish the message on the obtained Topic object.
	return topic.Publish(publishCtx, data)
}

// UnsubscribeFromTopic cancels the subscription to a specific topic.
// The message handling goroutine associated with this subscription will detect the cancellation
// and perform cleanup.
func (mn *ManagedNode) UnsubscribeFromTopic(topicName string) error {
	mn.peerMutex.Lock()
	defer mn.peerMutex.Unlock()

	sub, exists := mn.topicSubscriptions[topicName]
	if !exists {
		return fmt.Errorf("not subscribed to topic %s", topicName)
	}

	sub.Cancel() // Cancel the subscription.
	// The message handling goroutine will automatically exit and perform cleanup.

	delete(mn.topicSubscriptions, topicName)
	// Consider whether to also delete the topicHandler if automatic re-subscription
	// upon restart or other conditions is not desired.
	// delete(mn.topicHandlers, topicName)

	log.Printf("Unsubscribed from topic %s", topicName)
	return nil
}
