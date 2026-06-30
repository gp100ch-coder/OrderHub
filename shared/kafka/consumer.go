package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Producer wraps Kafka producer with additional functionality
type Producer struct {
	producer *kafka.Producer
}

// NewProducer creates a new Kafka producer
func NewProducer(brokers []string) (*Producer, error) {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":  brokers[0],
		"acks":               "all",
		"retries":            5,
		"retry.backoff.ms":   100,
		"delivery.timeout.ms": 30000,
		"compression.type":   "snappy",
	}

	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	return &Producer{
		producer: producer,
	}, nil
}

// Produce sends a message to Kafka
func (p *Producer) Produce(topic string, key, value []byte, headers map[string]string) error {
	kafkaHeaders := make([]kafka.Header, 0, len(headers))
	for k, v := range headers {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{
			Key:   k,
			Value: []byte(v),
		})
	}

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value:   value,
		Key:     key,
		Headers: kafkaHeaders,
	}

	deliveryChan := make(chan kafka.Event, 1)
	err := p.producer.Produce(msg, deliveryChan)
	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	ev := <-deliveryChan
	m, ok := ev.(*kafka.Message)
	if !ok {
		return fmt.Errorf("unexpected event type")
	}

	if m.TopicPartition.Error != nil {
		return fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
	}

	return nil
}

// Close gracefully closes the producer
func (p *Producer) Close() {
	p.producer.Flush(10000)
	p.producer.Close()
}

// Consumer wraps Kafka consumer with retry and dead-letter queue support
type Consumer struct {
	consumer    *kafka.Consumer
	groupID     string
	topics      []string
	maxRetries  int
	dlqTopic    string
}

// ConsumerConfig holds consumer configuration
type ConsumerConfig struct {
	Brokers    []string
	GroupID    string
	Topics     []string
	MaxRetries int
	DLQTopic   string
}

// NewConsumer creates a new Kafka consumer
func NewConsumer(brokers []string, groupID string, topics []string) (*Consumer, error) {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":  brokers[0],
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
		"session.timeout.ms": 30000,
		"max.poll.interval.ms": 300000,
	}

	consumer, err := kafka.NewConsumer(configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	c := &Consumer{
		consumer:   consumer,
		groupID:    groupID,
		topics:     topics,
		maxRetries: 3,
		dlqTopic:   "dead-letter-queue",
	}

	// Subscribe to topics
	if err := consumer.SubscribeTopics(topics, nil); err != nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	return c, nil
}

// Consume reads a message from Kafka
func (c *Consumer) Consume(ctx context.Context) (*kafka.Message, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			ev := c.consumer.Poll(1000)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				return e, nil
			case kafka.Error:
				return nil, fmt.Errorf("Kafka error: %w", e)
			default:
				// Ignore other events
			}
		}
	}
}

// Commit commits the offset for a message
func (c *Consumer) Commit(msg *kafka.Message) error {
	_, err := c.consumer.CommitMessage(msg)
	return err
}

// SendToDLQ sends a failed message to the dead-letter queue
func (c *Consumer) SendToDLQ(msg *kafka.Message, reason string) error {
	// Add error header
	headers := msg.Headers
	headers = append(headers, kafka.Header{
		Key:   "dlq_reason",
		Value: []byte(reason),
	})

	dlqMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &c.dlqTopic,
			Partition: kafka.PartitionAny,
		},
		Value:   msg.Value,
		Key:     msg.Key,
		Headers: headers,
	}

	err := c.consumer.Produce(dlqMsg, nil)
	if err != nil {
		return fmt.Errorf("failed to send to DLQ: %w", err)
	}

	return nil
}

// Close gracefully closes the consumer
func (c *Consumer) Close() error {
	return c.consumer.Close()
}
