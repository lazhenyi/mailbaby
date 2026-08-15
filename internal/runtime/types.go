package runtime

import (
	"context"
	"errors"
	"time"

	"mailbaby/internal/queue"
	"mailbaby/internal/sender"
)

// Common runtime errors.
var (
	ErrNilQueue             = errors.New("runtime: queue cannot be nil")
	ErrNilSender            = errors.New("runtime: sender cannot be nil")
	ErrEngineAlreadyRunning = errors.New("runtime: engine is already running")
	ErrEngineNotRunning     = errors.New("runtime: engine is not running")
	ErrEngineStopping       = errors.New("runtime: engine is stopping")
	ErrInvalidPayload       = errors.New("runtime: invalid email payload JSON")
)

// EngineState represents the lifecycle state of the Runtime Engine.
type EngineState int32

const (
	StateStopped EngineState = iota
	StateStarting
	StateRunning
	StateStopping
)

func (s EngineState) String() string {
	switch s {
	case StateStopped:
		return "STOPPED"
	case StateStarting:
		return "STARTING"
	case StateRunning:
		return "RUNNING"
	case StateStopping:
		return "STOPPING"
	default:
		return "UNKNOWN"
	}
}

// ProcessFunc is the handler signature for processing a deserialized email message.
type ProcessFunc func(ctx context.Context, msg *queue.Message, email *sender.Email) error

// Middleware intercepts the execution of a ProcessFunc.
type Middleware func(next ProcessFunc) ProcessFunc

// ErrorHandler is called when a message processing encounters an unrecoverable failure or exceeds retries.
type ErrorHandler func(ctx context.Context, msg *queue.Message, email *sender.Email, err error)

// RuntimeStats reports live runtime metrics and counters.
type RuntimeStats struct {
	State           string        `json:"state"`
	TotalReceived   int64         `json:"total_received"`
	TotalSuccess    int64         `json:"total_success"`
	TotalFailed     int64         `json:"total_failed"`
	TotalRetried    int64         `json:"total_retried"`
	TotalDeadLetter int64         `json:"total_dead_letter"`
	InFlight        int64         `json:"in_flight"`
	Uptime          time.Duration `json:"uptime"`
	StartTime       time.Time     `json:"start_time"`
}
