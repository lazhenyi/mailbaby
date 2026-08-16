# MailBaby Error Code Reference

This document is the single source of truth for the error codes surfaced by
the MailBaby HTTP API, gRPC API, and queue runtime. SDKs across Go, Java,
Python, and Rust map each server-side code to a language-idiomatic
exception type so users can write the same `try/except` / `errors.Is` /
`instanceof` checks against any transport.

## HTTP error envelope

```json
{
  "code": 400,
  "error": "validation_error",
  "message": "...",
  "details": "trace_id=..."
}
```

| `error` | HTTP status | When raised |
| --- | --- | --- |
| `method_not_allowed` | 405 | Non-POST verb on a send endpoint |
| `invalid_request` | 400 | Body could not be read or exceeded 32 MiB |
| `invalid_json` | 400 | JSON unmarshal failure |
| `empty_batch` | 400 | Batch request with zero emails |
| `validation_error` | 400 | Email failed `Email.Validate()` (e.g. no recipients) |
| `unauthorized` | 401 | Missing / invalid `Authorization` or `X-API-Key` |
| `rate_limited` | 429 | Per-key rate cap exceeded; `Retry-After` set |
| `internal_error` | 500 | Unhandled panic or runtime failure |
| `enqueue_failed` | 500 | Queue publish failed (no DLQ fallback succeeded) |
| `delivery_failed` | 500 | Synchronous SMTP send failed |

## gRPC status mapping

| gRPC `code` | Server meaning |
| --- | --- |
| `InvalidArgument` | Validation failure on the request payload |
| `Unauthenticated` | Missing / invalid metadata secret key |
| `Unavailable` | SMTP sender not initialized (no transport configured) |
| `Internal` | Synchronous send or queue publish failed |

## Queue runtime sentinels (`internal/queue`)

These are returned directly by driver code; SDKs treat them as opaque
transport failures and surface them verbatim.

| Sentinel | Meaning |
| --- | --- |
| `ErrDriverNotFound` | Driver is not registered (typo or missing `_ "all"` import) |
| `ErrQueueClosed` | Operation on a closed queue / producer / consumer |
| `ErrInvalidMessage` | Nil message or empty payload |
| `ErrInvalidConfig` | Driver constructor received a nil `*config.Config` |
| `ErrPublishFailed` | Wraps the underlying broker's publish error |
| `ErrConsumeFailed` | Consumer encountered an unrecoverable failure |
| `ErrTimeout` | Operation exceeded its deadline |
| `ErrAckNotSupported` | `msg.Ack` called without `SetAckFunc` |
| `ErrNackNotSupported` | `msg.Nack` called without `SetNackFunc` |
| `ErrNilHandler` | `Consume` called with a nil handler |

## SDK exception mapping

| Server code | Go (`errors.Is`) | Python (`isinstance`) | Java | Rust |
| --- | --- | --- | --- | --- |
| `401 unauthorized` | `ErrUnauthorized` | `AuthenticationError` | `AuthenticationException` | `MailBabyError::Unauthorized` |
| `400 validation_error` | `ErrValidation` | `ValidationError` | `ValidationException` | `MailBabyError::Validation` |
| `500 delivery_failed` | `ErrDeliveryFailed` | `DeliveryError` | `DeliveryException` | `MailBabyError::Delivery` |
| `500 enqueue_failed` | `ErrDeliveryFailed` | `EnqueueError` | `EnqueueException` | `MailBabyError::Delivery` |
| `429 rate_limited` | `ErrUnauthorized` (until 0.4.0) | `RequestFailedError` | `RateLimitedException` | `MailBabyError::RateLimited` |
| other `5xx` | wrap `*APIError` | `RequestFailedError` | `ServerException` | `MailBabyError::Server` |

> SDKs MUST expose a per-transport `APIError` (or equivalent) that retains
> the server's `code`, `error`, `message`, and `details` so operators can
> correlate client-side failures with server logs via the `trace_id`
> returned in `details`.