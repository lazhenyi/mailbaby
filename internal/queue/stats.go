package queue

import (
	"fmt"
	"mailbaby/internal/config"
)

// Stats contains metrics and operational statistics for a queue.
type Stats struct {
	// Driver indicates the message queue driver (e.g. rabbitmq, kafka, redis, memory).
	Driver config.QueueDriver `json:"driver"`

	// Name is the queue or topic name.
	Name string `json:"name"`

	// Ready is the approximate number of messages waiting to be consumed.
	Ready int64 `json:"ready"`

	// InFlight is the number of messages currently delivered and awaiting Ack/Nack.
	InFlight int64 `json:"in_flight"`

	// Delayed is the number of scheduled delayed messages.
	Delayed int64 `json:"delayed,omitempty"`

	// Total is the total lifetime or cumulative message count if supported.
	Total int64 `json:"total,omitempty"`

	// Consumers is the number of active consumer connections/workers.
	Consumers int `json:"consumers"`

	// Extra provides driver-specific stats.
	Extra map[string]any `json:"extra,omitempty"`
}

// String returns a formatted representation of queue statistics.
func (s Stats) String() string {
	return fmt.Sprintf("QueueStats[driver=%s, name=%s, ready=%d, in_flight=%d, consumers=%d]",
		s.Driver, s.Name, s.Ready, s.InFlight, s.Consumers)
}
