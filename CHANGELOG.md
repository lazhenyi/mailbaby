# Changelog

All notable changes to mailbaby are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Security (breaking-behavior, non-API)
- **Fail-closed auth default** — `auth.enabled` now defaults to `true` so an
  empty `MAILBABY_AUTH_SECRET_KEY` causes the server to refuse to start
  instead of silently running unauthenticated. Set
  `MAILBABY_AUTH_ENABLED=false` to opt out (only recommended for trusted
  local dev).
- **HTTP/gRPC TLS** — both the unified HTTP server and the gRPC server can
  now bind TLS via `server.tls_enabled` / `grpc.tls_enabled`. Certificates
  must be supplied via `tls_cert_path` / `tls_key_path`.
- **SMTP plaintext downgrade rejected** — `security.encryption` no longer
  silently downgrades to `none` when `auto` is selected with `InsecureSkipVerify`
  or when the server greeting doesn't advertise STARTTLS.
- **CRLF injection in MIME headers** — `From`, `To`, `Cc`, `Reply-To`,
  `Subject`, and attachment filenames are now run through `sanitizeHeaderValue`
  before being written to the MIME envelope. Test coverage in
  `internal/sender/mime_injection_test.go`.
- **Query-string token fallback removed** — HTTP auth now only accepts
  secrets via `Authorization: Bearer …`, `X-API-Key`, or the configured
  custom header. Query-string `?api_key=` / `?token=` is rejected so secrets
  no longer leak into proxy/access logs.
- **Automatic log secret redaction** — `logger.RedactSecrets` scrubs URL
  userinfo, SMTP AUTH base64 blobs, and `Authorization` / `X-API-Key`
  headers from log messages and field values before they reach the writer,
  so credentials no longer leak into log files.

### Reliability
- **DLQ publish failure no longer acks** — when DLQ publish fails, the
  pipeline leaves the original message un-acknowledged so the message broker
  re-delivers it instead of silently dropping the payload.
- **Exponential retry backoff with jitter and 5-minute cap** — retry interval
  doubles per attempt up to `5 * time.Minute`, replacing the previous fixed
  interval that thundered against recovering brokers.
- **SMTP pool deadlock fix** — `pool.Acquire` no longer returns a phantom
  acquisition when both the idle channel and the open-connection semaphore
  are saturated simultaneously; it now waits via a single `select` so all
  goroutines progress deterministically.
- **Per-key sliding-window rate limit** — `auth.rate_per_key_per_minute`
  (default 600) caps requests per authenticated key. Buckets are keyed by
  `sha256(secret)` so raw keys never live in the limiter map.
- **gRPC default deadline** — unary calls without a caller-supplied
  deadline are bounded by `rpc.DefaultRPCTimeout` (30s) via the new
  `UnaryDeadlineInterceptor`, which runs after auth so unauthenticated
  traffic is rejected before consuming a deadline slot.
- **Memory queue drain on close** — `MemoryQueue.Close` now waits for
  in-flight handlers to complete (bounded by `queue.drain_timeout`,
  default 30s) so callers no longer lose messages mid-process; consumer
  drain runs after the receive-loop exits.
- **SMTP pool idle semantics** — `pool.MaxIdleConns == -1` now disables
  connection caching entirely; `0` falls back to a safe default of 5; the
  pool never returns a stale `nil` client when both the idle channel and
  the open-connection semaphore are saturated.
- **SMTP graceful close bounded** — `SmtpClient.Close` no longer blocks
  indefinitely on a non-responsive server QUIT; it falls back to a raw TCP
  close after 2 seconds so the engine shutdown timeout can actually fire.
- **StatsD Close race fixed** — `StatsDClient.Close` now waits for the
  background flush goroutine to drain before closing the underlying UDP
  socket, eliminating `use of closed network connection` errors during
  shutdown.
- **PushGateway exponential backoff** — `PushGatewayPusher` doubles the
  retry interval on consecutive failures (capped at 5 minutes) and only
  warns every 5th failure, so a long outage does not flood the log.
- **HTTP forced close on shutdown timeout** — `handler.Server.Stop` now
  calls `http.Server.Close` if `Shutdown` returns an error, guaranteeing
  the process never blocks on a misbehaving long-lived request.
- **Shutdown timeout default raised to 60s** — `app.shutdown_timeout`
  now defaults to `60s` (was `5s`) so graceful shutdown tolerates
  SMTP connections in flight at the moment of a SIGTERM. The
  `Engine.Stop` deadline uses this value when no explicit timeout is
  supplied.
- **Memory queue delayed publish tracking** — `MemoryQueue.Close` now
  waits for outstanding `delay > 0` publish goroutines so timers cannot
  outlive the queue.

### Observability
- **Prometheus label cardinality guard** — all metric helpers now run
  user-controlled labels through `sanitizeLabel`, which truncates strings
  longer than 64 bytes and hashes them with SHA-256 to bound label cardinality.
- **Per-driver `BaseStats`** — every queue driver now embeds the shared
  `common.BaseStats` for `inFlight`, `totalSent`, and `activeCons` counters,
  eliminating ~80 duplicated `atomic.LoadInt64/AddInt64` sites.
- **Tracing provider routing** — `tracing.provider` now selects between
  `stdout` and `none`; `otlp` / `jaeger` / `zipkin` fall back to stdout
  with a warning rather than silently dropping spans.
- **Per-driver receive-loop backoff** — all 8 drivers now use a shared
  jittered exponential backoff (`common.NewBackoff`) instead of fixed
  `time.Sleep(50ms … 200ms)` on broker errors, eliminating busy-loops when
  brokers are unreachable.

### Architecture
- **`core.SendService`** — both the HTTP `EmailHandler` and the gRPC
  `Server` now delegate to `internal/core.SendService`, removing the
  duplicate `tracing`/`metrics`/`producer` plumbing in each transport.
- **`queue.ErrInvalidConfig`** — driver constructors now return
  `ErrInvalidConfig` when the supplied `*config.Config` is nil, replacing
  the previous misuse of `ErrInvalidMessage` for config errors.
- **HTTP CORS** — the unified HTTP server now emits CORS headers when
  `server.cors_allowed_origin` is set, including a preflight short-circuit
  for `OPTIONS` requests.
- **Trusted proxy headers opt-in** — `server.trust_proxy_headers` (default
  false) gates whether the server honors `X-Forwarded-For` / `X-Real-IP`
  for client IP reporting, preventing spoofed IPs from contaminating logs
  and metrics.
- **Webhook subsystem (optional)** — `runtime.WebhookConfig` delivers
  per-message status events to a customer URL with HMAC-SHA256 signature
  (`X-Mailbaby-Signature: sha256=...`), bounded retries, and per-call
  timeout. Disabled by default.

### SDKs
- **Version alignment** — Go, Java, Python, and Rust SDKs all report
  `0.3.0`.
- **`X-API-Key` is the Python REST default** — `MailBabyREST` now constructs
  the default header as `X-API-Key` to match the Go SDK and server default.
- **Rust `default-features`** — `mailbaby-rs` now defaults to
  `["rest", "grpc", "mq"]` so the user can simply `use mailbaby` and get the
  full surface area.
- **Rust unit tests** — `mailbaby-rs/tests/{model,auth}.rs` cover the
  fluent `EmailBuilder`, attachment MIME inference, base64 wire format,
  `Auth` schemes, and `validate_address` corner cases.
- **SDK consistency matrix** — `docs/SDK_CONSISTENCY.md` registers the
  error class mapping, auth header conventions, metric labels,
  retry/timeout defaults, and wire format guarantees across all four SDKs.
- **Cross-SDK wire-level test matrix** — `internal/compat/sdk_matrix_test.go`
  implements 14 wire-level compatibility tests (`TestSDKMatrix_*`) covering
  auth, error envelope, attachment JSON, 429 Retry-After, 5xx retry, no
  query-token fallback, empty-field omission, proxy header opt-in, and
  concurrent-safety guarantees. All 14 tests pass; 4 SDK clients must run
  the same matrix against this server to validate cross-language
  parity.

## [0.2.0] — Initial public preview

- 8 queue drivers: RabbitMQ, Kafka, Redis, RocketMQ, NATS, Pulsar, SQS,
  in-memory.
- HTTP REST + gRPC transports with shared `SendService`.
- Prometheus + StatsD + OpenTelemetry observability.
- 4 SDK clients: Go, Java, Python, Rust.