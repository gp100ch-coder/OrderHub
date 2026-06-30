package events

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

// Event types
const (
	EventOrderCreated   = "order.created"
	EventOrderConfirmed = "order.confirmed"
	EventOrderCancelled = "order.cancelled"
	EventOrderFailed    = "order.failed"
	EventPaymentProcessed = "payment.processed"
	EventPaymentFailed  = "payment.failed"
	EventInventoryReserved = "inventory.reserved"
	EventInventoryReleased = "inventory.released"
)

// OrderEvent represents an order-related event
type OrderEvent struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	OrderID   string                 `json:"order_id"`
	UserID    string                 `json:"user_id"`
	Data      map[string]interface{} `json:"data,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
	Version   int                    `json:"version"`
}

// EventPublisher handles publishing events to Kafka
type EventPublisher struct {
	producer *kafka.Producer
	logger   *zap.Logger
	topic    string
}

// NewEventPublisher creates a new EventPublisher instance
func NewEventPublisher(brokers []string, topic string, logger *zap.Logger) (*EventPublisher, error) {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":  brokers[0],
		"acks":               "all",
		"retries":            5,
		"retry.backoff.ms":   100,
		"delivery.timeout.ms": 30000,
		"linger.ms":          5,
		"batch.num.messages": 1000,
		"compression.type":   "snappy",
	}

	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	publisher := &EventPublisher{
		producer: producer,
		logger:   logger,
		topic:    topic,
	}

	// Start delivery report handler
	go publisher.handleDeliveryReports()

	return publisher, nil
}

// Publish sends an event to Kafka with at-least-once delivery guarantee
func (p *EventPublisher) Publish(ctx context.Context, event *OrderEvent) error {
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	headers := []kafka.Header{
		{Key: "event_type", Value: []byte(event.Type)},
		{Key: "event_id", Value: []byte(event.ID)},
		{Key: "correlation_id", Value: []byte(event.OrderID)},
	}

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &p.topic,
			Partition: kafka.PartitionAny,
		},
		Value:   eventBytes,
		Headers: headers,
		Key:     []byte(event.OrderID), // Use order ID as key for ordering
	}

	deliveryChan := make(chan kafka.Event, 1)
	err = p.producer.Produce(message, deliveryChan)
	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	// Wait for delivery confirmation
	select {
	case ev := <-deliveryChan:
		m, ok := ev.(*kafka.Message)
		if !ok {
			return fmt.Errorf("unexpected event type: %T", ev)
		}
		if m.TopicPartition.Error != nil {
			return fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
		}
		p.logger.Debug("Event published successfully",
			zap.String("event_id", event.ID),
			zap.String("event_type", event.Type),
			zap.String("order_id", event.OrderID),
			zap.Int32("partition", m.TopicPartition.Partition),
			zap.Int64("offset", m.TopicPartition.Offset),
		)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(10 * time.Second):
		return fmt.Errorf("delivery confirmation timeout")
	}
}

// PublishBatch publishes multiple events atomically
func (p *EventPublisher) PublishBatch(ctx context.Context, events []*OrderEvent) error {
	for _, event := range events {
		if err := p.Publish(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

// Close gracefully closes the producer
func (p *EventPublisher) Close() {
	p.logger.Info("Closing Kafka producer...")
	p.producer.Flush(10000) // Wait up to 10 seconds for messages to be delivered
	p.producer.Close()
}

func (p *EventPublisher) handleDeliveryReports() {
	for e := range p.producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				p.logger.Error("Message delivery failed",
					zap.Error(ev.TopicPartition.Error),
					zap.String("topic", *ev.TopicPartition.Topic),
					zap.Int32("partition", ev.TopicPartition.Partition),
				)
			}
		case kafka.Error:
			p.logger.Error("Kafka error", zap.Error(ev))
		}
	}
}

// CreateOrderCreatedEvent creates an order created event
func CreateOrderCreatedEvent(orderID, userID string, data map[string]interface{}) *OrderEvent {
	return &OrderEvent{
		ID:        generateEventID(),
		Type:      EventOrderCreated,
		OrderID:   orderID,
		UserID:    userID,
		Data:      data,
		Timestamp: time.Now(),
		Version:   1,
	}
}

// CreateOrderConfirmedEvent creates an order confirmed event
func CreateOrderConfirmedEvent(orderID, userID string, data map[string]interface{}) *OrderEvent {
	return &OrderEvent{
		ID:        generateEventID(),
		Type:      EventOrderConfirmed,
		OrderID:   orderID,
		UserID:    userID,
		Data:      data,
		Timestamp: time.Now(),
		Version:   1,
	}
}

// CreateOrderCancelledEvent creates an order cancelled event
func CreateOrderCancelledEvent(orderID, userID string, data map[string]interface{}) *OrderEvent {
	return &OrderEvent{
		ID:        generateEventID(),
		Type:      EventOrderCancelled,
		OrderID:   orderID,
		UserID:    userID,
		Data:      data,
		Timestamp: time.Now(),
		Version:   1,
	}
}

// CreateOrderFailedEvent creates an order failed event
func CreateOrderFailedEvent(orderID, userID string, reason string) *OrderEvent {
	return &OrderEvent{
		ID:      generateEventID(),
		Type:    EventOrderFailed,
		OrderID: orderID,
		UserID:  userID,
		Data: map[string]interface{}{
			"reason": reason,
		},
		Timestamp: time.Now(),
		Version:   1,
	}
}

func generateEventID() string {
	return fmt.Sprintf("evt_%d", time.Now().UnixNano())
}
