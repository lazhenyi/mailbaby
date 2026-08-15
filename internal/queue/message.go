package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Message represents a generic queue message across all MQ drivers.
type Message struct {
	// ID is the unique identifier of the message.
	ID string `json:"id"`

	// Topic represents the destination queue/topic/routing key/stream.
	Topic string `json:"topic"`

	// Payload is the raw byte payload of the message.
	Payload []byte `json:"payload"`

	// Headers contains metadata key-value pairs associated with the message.
	Headers map[string]string `json:"headers,omitempty"`

	// Key is an optional partition key / sharding key.
	Key string `json:"key,omitempty"`

	// Delay specifies the duration after which the message becomes eligible for delivery.
	Delay time.Duration `json:"delay,omitempty"`

	// Timestamp records when the message was generated or received.
	Timestamp time.Time `json:"timestamp"`

	// Attempts records how many times this message has been delivered/attempted.
	Attempts int `json:"attempts"`

	// Raw holds the original driver-specific message object (e.g. amqp.Delivery, redis.XMessage).
	Raw any `json:"-"`

	mu           sync.RWMutex
	acknowledged bool
	ackFn        func(ctx context.Context) error
	nackFn       func(ctx context.Context, requeue bool) error
}

// NewMessage creates a new Message with a randomly generated ID and the current timestamp.
func NewMessage(payload []byte) *Message {
	return NewMessageWithID(generateUUID(), payload)
}

// NewMessageWithID creates a Message with a specific ID and payload.
func NewMessageWithID(id string, payload []byte) *Message {
	if id == "" {
		id = generateUUID()
	}
	return &Message{
		ID:        id,
		Payload:   payload,
		Headers:   make(map[string]string),
		Timestamp: time.Now(),
		Attempts:  1,
	}
}

// NewJSONMessage serializes an object into JSON and creates a Message.
func NewJSONMessage(v any) (*Message, error) {
	return NewJSONMessageWithID(generateUUID(), v)
}

// NewJSONMessageWithID serializes an object into JSON and creates a Message with a specific ID.
func NewJSONMessageWithID(id string, v any) (*Message, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("queue: failed to marshal message payload to JSON: %w", err)
	}
	msg := NewMessageWithID(id, data)
	msg.SetHeader("Content-Type", "application/json")
	return msg, nil
}

// BindJSON deserializes the Message payload into the provided pointer.
func (m *Message) BindJSON(v any) error {
	if m == nil || len(m.Payload) == 0 {
		return ErrInvalidMessage
	}
	return json.Unmarshal(m.Payload, v)
}

// SetHeader sets a key-value header in the message.
func (m *Message) SetHeader(key, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Headers == nil {
		m.Headers = make(map[string]string)
	}
	m.Headers[key] = value
}

// GetHeader retrieves a header value by key.
func (m *Message) GetHeader(key string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.Headers == nil {
		return ""
	}
	return m.Headers[key]
}

// SetAckFunc sets the acknowledgment callback for this message.
func (m *Message) SetAckFunc(fn func(ctx context.Context) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ackFn = fn
}

// SetNackFunc sets the negative acknowledgment callback for this message.
func (m *Message) SetNackFunc(fn func(ctx context.Context, requeue bool) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nackFn = fn
}

// Ack acknowledges that the message was processed successfully.
// The ack callback is invoked without holding the message lock, so callbacks
// may safely call clone/read methods on the message itself.
func (m *Message) Ack(ctx context.Context) error {
	m.mu.Lock()
	if m.acknowledged {
		m.mu.Unlock()
		return nil
	}
	if m.ackFn == nil {
		m.mu.Unlock()
		return ErrAckNotSupported
	}
	m.acknowledged = true
	m.mu.Unlock()

	if err := m.ackFn(ctx); err != nil {
		m.mu.Lock()
		m.acknowledged = false
		m.mu.Unlock()
		return err
	}
	return nil
}

// Nack rejects the message, optionally requesting it to be requeued.
// The nack callback is invoked without holding the message lock.
func (m *Message) Nack(ctx context.Context, requeue bool) error {
	m.mu.Lock()
	if m.acknowledged {
		m.mu.Unlock()
		return nil
	}
	if m.nackFn == nil {
		m.mu.Unlock()
		return ErrNackNotSupported
	}
	m.acknowledged = true
	m.mu.Unlock()

	if err := m.nackFn(ctx, requeue); err != nil {
		m.mu.Lock()
		m.acknowledged = false
		m.mu.Unlock()
		return err
	}
	return nil
}

// IsAcknowledged returns whether this message has already been Acked or Nacked.
func (m *Message) IsAcknowledged() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.acknowledged
}

// Clone creates a shallow clone of the message with copied headers.
func (m *Message) Clone() *Message {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	headers := make(map[string]string, len(m.Headers))
	for k, v := range m.Headers {
		headers[k] = v
	}

	payload := make([]byte, len(m.Payload))
	copy(payload, m.Payload)

	return &Message{
		ID:           m.ID,
		Topic:        m.Topic,
		Payload:      payload,
		Headers:      headers,
		Key:          m.Key,
		Delay:        m.Delay,
		Timestamp:    m.Timestamp,
		Attempts:     m.Attempts,
		Raw:          m.Raw,
		acknowledged: m.acknowledged,
		ackFn:        m.ackFn,
		nackFn:       m.nackFn,
	}
}

// String provides a human-readable representation of the message.
func (m *Message) String() string {
	if m == nil {
		return "<nil message>"
	}
	return fmt.Sprintf("Message{ID:%s, Topic:%s, PayloadLen:%d, Attempts:%d, Timestamp:%s}",
		m.ID, m.Topic, len(m.Payload), m.Attempts, m.Timestamp.Format(time.RFC3339))
}

func generateUUID() string {
	var buf [16]byte
	_, _ = rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}
