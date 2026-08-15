package sender

import (
	"errors"
)

var (
	// ErrNilEmail is returned when a nil *Email is passed to Send.
	ErrNilEmail = errors.New("sender: email cannot be nil")

	// ErrNoRecipients is returned when an email has no To, Cc, or Bcc recipients.
	ErrNoRecipients = errors.New("sender: at least one recipient (To, Cc, or Bcc) is required")

	// ErrInvalidFrom is returned when the sender's From address is invalid or empty.
	ErrInvalidFrom = errors.New("sender: invalid from email address")

	// ErrInvalidRecipient is returned when any recipient email address is malformed.
	ErrInvalidRecipient = errors.New("sender: invalid recipient email address")

	// ErrMaxRecipientsExceeded is returned when the number of recipients exceeds the configured rate limit.
	ErrMaxRecipientsExceeded = errors.New("sender: number of recipients exceeds account limit")

	// ErrAccountNotFound is returned when the specified SMTP account name is not found.
	ErrAccountNotFound = errors.New("sender: smtp account not found")

	// ErrPoolClosed is returned when an operation is attempted on a closed connection pool.
	ErrPoolClosed = errors.New("sender: connection pool is closed")

	// ErrPoolExhausted is returned when no connection is available in the pool within the timeout.
	ErrPoolExhausted = errors.New("sender: connection pool exhausted")

	// ErrAuthFailed is returned when SMTP SASL authentication fails.
	ErrAuthFailed = errors.New("sender: smtp authentication failed")

	// ErrRateLimitExceeded is returned when sending rate exceeds account rate limit.
	ErrRateLimitExceeded = errors.New("sender: rate limit exceeded")

	// ErrEmailTooLarge is returned when the email payload exceeds the configured size limit.
	ErrEmailTooLarge = errors.New("sender: email payload exceeds configured size limit")
)
