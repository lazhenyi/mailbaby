<div align="center">

# 📬 MailBaby

**High-Performance, Cloud-Native, Multi-Queue Email Delivery Microservice in Go**

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go&logoColor=white)](go.mod)
[![Build Status](https://img.shields.io/badge/Build-Passing-brightgreen.svg)]()
[![Go Report Card](https://img.shields.io/badge/Go%20Report-A%2B-brightgreen.svg)]()
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white)](build/Dockerfile)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-Helm%20Chart-326CE5?logo=kubernetes&logoColor=white)](charts/)
[![OpenTelemetry](https://img.shields.io/badge/Tracing-OpenTelemetry-F5A800?logo=opentelemetry&logoColor=white)]()
[![Prometheus](https://img.shields.io/badge/Metrics-Prometheus-E6522C?logo=prometheus&logoColor=white)]()
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)]()

[English](README.md) • [简体中文](README.zh-CN.md)

</div>

---

**MailBaby** is an enterprise-ready, high-throughput email sending service driven by message queues. It decouples your primary application backends from synchronous SMTP latency by consuming email dispatch jobs from any major message broker (RabbitMQ, Kafka, Redis, RocketMQ, NATS, Pulsar, AWS SQS, or In-Memory) and reliably executing delivery over SMTP.

MailBaby features **multi-account SMTP routing**, **per-account connection pooling & token-bucket rate limiting**, **at-least-once delivery with exponential retries and Dead Letter Queue (DLQ) support**, **multi-protocol ingestion (REST / gRPC / MQ)**, and **cloud-native observability (Prometheus, OpenTelemetry, K8s probes, pprof)**.

> [!NOTE]
> **Project Status: Active Development**
> MailBaby is under rapid evolution. While fully functional and thoroughly tested, configuration specifications and internal interfaces may still evolve prior to the v1.0.0 stable milestone.

---

## 📑 Table of Contents

- [✨ Key Features](#-key-features)
- [🏗️ System Architecture](#️-system-architecture)
- [🔌 Supported Queue Drivers](#-supported-queue-drivers)
- [🚀 Quick Start](#-quick-start)
  - [Prerequisites](#prerequisites)
  - [Option 1: Docker (Fastest)](#option-1-docker-fastest)
  - [Option 2: Build & Run from Source](#option-2-build--run-from-source)
  - [Option 3: Kubernetes via Helm](#option-3-kubernetes-via-helm)
- [📬 Dispatch Interfaces](#-dispatch-interfaces)
  - [1. HTTP RESTful API](#1-http-restful-api)
  - [2. gRPC RPC Service](#2-grpc-rpc-service)
  - [3. Message Queue Ingestion](#3-message-queue-ingestion)
  - [4. Command-Line Interface (CLI)](#4-command-line-interface-cli)
- [⚙️ Configuration Reference](#️-configuration-reference)
- [📊 Observability & Operations](#-observability--operations)
  - [Prometheus Metrics](#prometheus-metrics)
  - [Distributed Tracing (OpenTelemetry)](#distributed-tracing-opentelemetry)
  - [Container Health Probes](#container-health-probes)
  - [Runtime Profiling (pprof)](#runtime-profiling-pprof)
- [🛠️ Makefile & Tooling](#️-makefile--tooling)
- [🗺️ Roadmap](#️-roadmap)
- [🤝 Contributing](#-contributing)
- [📄 License](#-license)

---

## ✨ Key Features

- **8 Queue Backends Under One Abstraction**: Seamlessly switch between RabbitMQ, Apache Kafka, Redis (Stream/List/PubSub), Apache RocketMQ, NATS/JetStream, Apache Pulsar, AWS SQS, and In-Memory queue without touching application code.
- **Multi-Account SMTP & Smart Routing**: Declare multiple SMTP providers (e.g., Transactional, Marketing, Ops Alerts). Each account maintains isolated credentials, bounded connection pools, TLS encryption modes, SASL mechanisms, and token bucket rate limits.
- **Multi-Protocol Ingestion**: Send emails asynchronously via message brokers or trigger synchronous/asynchronous delivery via HTTP REST API (`/v1/email/send`, `/v1/email/batch`) and high-performance gRPC (`mailbaby.v1.MailService`).
- **Reliability & Zero Message Loss**: Built-in exponential backoff retries, per-message attempt counters, manual ACK/NACK semantics, and automatic Dead Letter Queue (DLQ) routing.
- **Connection Pooling & Rate Limiting**: Bounded idle/active SMTP connection pools with keep-alive management; per-account token bucket rate limiting to prevent provider throttling (e.g., Gmail/SendGrid/SES quotas).
- **MIME & Rich Content Engine**: Full support for HTML bodies, plaintext fallbacks, multipart attachments, and inline CID embedded media (`<img src="cid:xxx">`).
- **Cloud-Native Observability**:
  - **Metrics**: Prometheus OpenMetrics exporter (`/metrics`), StatsD client, Prometheus Pushgateway, and Go `expvar`.
  - **Tracing**: Distributed tracing via OpenTelemetry (OTLP gRPC/HTTP, Jaeger, Zipkin) with cross-queue context propagation.
  - **Health Probes**: Kubernetes-standard `/livez` and deep `/readyz` dependency checks.
  - **Profiling**: Integrated Go runtime `pprof` endpoints.
- **Security & Multi-Tenancy**: Optional API token / Secret Key authentication for REST and gRPC endpoints (`X-API-Key` or `Bearer` tokens).
- **Lightweight & Container-Ready**: CGO-free static binary, non-root minimal Docker image (`uid: 10001`), and production-grade Helm Chart.

---

## 🏗️ System Architecture

```
                                  [ Client Applications ]
                                             │
      ┌──────────────────────────────────────┼──────────────────────────────────────┐
      │                                      │                                      │
  [ HTTP REST API ]                  [ gRPC Service ]                       [ Direct MQ Publish ]
(POST /v1/email/send)            (mailbaby.v1.MailService)              (Kafka/RabbitMQ/Redis/...)
      │                                      │                                      │
      └───────────────────┬──────────────────┴──────────────────────────────────────┘
                          ▼
            ┌───────────────────────────┐
            │   Auth & Middleware Guard │ (Token Auth / Panic Recovery / OTel Tracing / Metrics)
            └─────────────┬─────────────┘
                          │
                          ▼
             [ Message Queue Subsystem ]
             ┌─────────────────────────────────────────────────────────────┐
             │ RabbitMQ │ Kafka │ Redis │ RocketMQ │ NATS │ Pulsar │ SQS │ │
             └─────────────────────────────┬───────────────────────────────┘
                                           │
                          ┌────────────────┴────────────────┐
                          ▼                                 ▼
               [ Concurrent Worker Pool ]       [ Dead Letter Queue (DLQ) ]
               (QoS Prefetch / Retry Backoff)   (Unrecoverable Failures)
                          │
                          ▼
             [ Multi-Account SMTP Router ]
        ┌─────────────────┼─────────────────┐
        ▼                 ▼                 ▼
 ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
 │   Account:   │  │   Account:   │  │   Account:   │
 │   default    │  │  marketing   │  │    alert     │
 ├──────────────┤  ├──────────────┤  ├──────────────┤
 │ Rate Limiter │  │ Rate Limiter │  │ Rate Limiter │
 │ Conn Pool    │  │ Conn Pool    │  │ Conn Pool    │
 └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
        │                 │                 │
        ▼                 ▼                 ▼
 ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
 │ SMTP Relay   │  │ SendGrid /   │  │ Local Postfix│
 │ (Office365)  │  │ Mailgun API  │  │ / Internal   │
 └──────────────┘  └──────────────┘  └──────────────┘
```

---

## 🔌 Supported Queue Drivers

MailBaby abstracts queue providers behind a clean, unified Go interface (`queue.Queue`, `queue.Producer`, `queue.Consumer`):

| Driver | Protocol / Backend | Ingestion Mode | Reliability / ACK | Dead Letter Queue | Recommended Scenario |
|---|---|---|:---:|:---:|---|
| **`memory`** | Go Channels | In-process Buffer | In-memory ACK | Optional Memory DLQ | Local testing, unit tests, standalone instances |
| **`rabbitmq`** | AMQP 0-9-1 | Exchange & Queue Binding | Explicit Manual ACK/NACK | Supported (AMQP DLX) | Enterprise microservices, flexible routing keys |
| **`kafka`** | Apache Kafka | Partitioned Topics & CG | Offset Commit on Success | Supported (Kafka DLQ Topic) | Massive throughput, event streaming architectures |
| **`redis`** | Redis 5.0+ | Streams / List / PubSub | XACK (Stream mode) | Supported | Lightweight setups, existing Redis infrastructure |
| **`rocketmq`** | Apache RocketMQ | Topics & Consumer Groups | ACK / ReconsumeLater | Supported (RocketMQ %DLQ%) | Financial-grade messaging, ordered message queues |
| **`nats`** | NATS / JetStream | JetStream Durable Consumer | JetStream Explicit ACK | Supported (NATS Subject) | Ultra-low latency, cloud-edge distributed systems |
| **`pulsar`** | Apache Pulsar | Persistent Multi-Tenant Topics | Individual / Cumulative ACK | Supported (Pulsar DLQ Policy)| Multi-tenant cloud platforms, geo-replication |
| **`sqs`** | AWS SQS | Standard / FIFO Queues | DeleteMessage on ACK | Supported (AWS SQS Redrive Policy) | AWS Serverless & Cloud-native deployments |

---

## 🚀 Quick Start

### Prerequisites

- **Go**: Version 1.26 or higher (if compiling from source)
- **SMTP Account**: An active SMTP server/relay (e.g. Gmail, SendGrid, Amazon SES, Postfix)
- **Message Broker**: (Optional) RabbitMQ, Kafka, Redis, or use built-in `memory` driver for zero-dependency local runs.

---

### Option 1: Docker (Fastest)

1. **Prepare configuration**:
   ```bash
   cp config.yaml.example config.yaml
   # Edit config.yaml with your SMTP credentials
   ```

2. **Run container**:
   ```bash
   docker run -d \
     --name mailbaby \
     -p 8080:8080 \
     -p 8081:8081 \
     -v "$(pwd)/config.yaml":/app/config.yaml:ro \
     mailbaby:latest
   ```

3. **Check health**:
   ```bash
   curl http://localhost:8080/readyz
   ```

---

### Option 2: Build & Run from Source

1. **Clone & Compile**:
   ```bash
   git clone https://github.com/mailbabys/mailbaby.git
   cd mailbaby
   make build
   ```

2. **Validate Configuration & Connectivity**:
   ```bash
   ./build/bin/mailbaby check -c config.yaml
   ```

3. **Start Daemon**:
   ```bash
   ./build/bin/mailbaby server -c config.yaml
   ```

4. **Send a Test Email**:
   ```bash
   ./build/bin/mailbaby send -c config.yaml \
     --to "developer@example.com" \
     --subject "MailBaby Quickstart" \
     --body "Hello from MailBaby!"
   ```

---

### Option 3: Kubernetes via Helm

Deploy production-ready MailBaby onto any Kubernetes cluster using the included Helm chart:

```bash
# Install with custom values
helm install mailbaby ./charts/mailbaby \
  -n mailbaby \
  --create-namespace \
  -f my-values.yaml
```

*For detailed Helm configuration options, refer to [charts/README.md](charts/README.md).*

---

## 📬 Dispatch Interfaces

### 1. HTTP RESTful API

MailBaby exposes a high-performance HTTP REST API for synchronous or asynchronous email dispatch.

#### Send Single Email (`POST /v1/email/send`)

> **Headers**: `Content-Type: application/json`, `X-API-Key: <your_secret_key>` (if auth is enabled)

**Synchronous Delivery Request** (Blocks until SMTP acknowledges):
```bash
curl -X POST http://localhost:8080/v1/email/send \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your_secret_key" \
  -d '{
    "account": "default",
    "from": "noreply@example.com",
    "from_name": "MailBaby System",
    "to": ["alice@example.com"],
    "cc": ["manager@example.com"],
    "subject": "Order Confirmation #10024",
    "text_body": "Thank you for your order! Your tracking number is 987654.",
    "html_body": "<h2>Order Confirmed</h2><p>Tracking number: <b>987654</b></p>",
    "tags": ["order", "receipt"]
  }'
```

**Response (`200 OK`)**:
```json
{
  "id": "e8a93bf84c379a20",
  "status": "sent",
  "message": "email sent successfully",
  "sent_at": 1771142400000000000
}
```

**Asynchronous Queue Ingestion** (Appends to queue and returns `202 Accepted` immediately):
```bash
curl -X POST "http://localhost:8080/v1/email/send?async=true" \
  -H "Content-Type: application/json" \
  -d '{
    "to": ["bob@example.com"],
    "subject": "Welcome to Our Platform!",
    "html_body": "<h1>Welcome aboard!</h1>"
  }'
```

---

#### Batch Email Delivery (`POST /v1/email/batch`)

Send multiple emails in a single request with parallel worker execution:

```bash
curl -X POST http://localhost:8080/v1/email/batch \
  -H "Content-Type: application/json" \
  -d '{
    "async": false,
    "emails": [
      {
        "to": ["user1@example.com"],
        "subject": "Monthly Statement",
        "text_body": "Here is your monthly invoice summary."
      },
      {
        "to": ["user2@example.com"],
        "subject": "Monthly Statement",
        "text_body": "Here is your monthly invoice summary."
      }
    ]
  }'
```

**Response (`200 OK`)**:
```json
{
  "total": 2,
  "succeeded": 2,
  "failed": 0,
  "results": [
    { "id": "4f9b2...", "status": "sent", "message": "email sent successfully", "sent_at": 1771142400000000000 },
    { "id": "6a8c1...", "status": "sent", "message": "email sent successfully", "sent_at": 1771142400000000000 }
  ]
}
```

---

### 2. gRPC RPC Service

MailBaby provides a full gRPC service defined in [`proto/mailbaby.proto`](proto/mailbaby.proto).

```protobuf
service MailService {
  rpc Send(SendMailRequest) returns (SendMailResponse);
  rpc SendBatch(BatchSendMailRequest) returns (BatchSendMailResponse);
  rpc Ping(PingRequest) returns (PingResponse);
  rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
}
```

**Call via `grpcurl`**:
```bash
grpcurl -plaintext \
  -H "authorization: Bearer your_secret_key" \
  -d '{"to": ["dev@example.com"], "subject": "gRPC Email", "text_body": "Sent via MailService.Send"}' \
  localhost:8081 mailbaby.v1.MailService/Send
```

---

### 3. Message Queue Ingestion

Any backend service (Node.js, Python, Java, Go, PHP, Rust, etc.) can publish an email job directly to your configured message broker (RabbitMQ queue, Kafka topic, Redis Stream, etc.).

**Payload Schema (`application/json`)**:
```json
{
  "id": "optional-custom-uuid-12345",
  "account": "default",
  "from": "alerts@example.com",
  "from_name": "DevOps Alerts",
  "reply_to": "support@example.com",
  "to": ["oncall@example.com"],
  "cc": ["lead@example.com"],
  "subject": "[CRITICAL] High CPU Load on prod-db-01",
  "text_body": "CPU usage exceeded 95% for 5 minutes.",
  "html_body": "<h2 style='color:red;'>System Alert</h2><p>CPU usage exceeded <b>95%</b>.</p>",
  "headers": {
    "X-Priority": "1",
    "X-Environment": "production"
  },
  "attachments": [
    {
      "filename": "metrics.png",
      "content_type": "image/png",
      "data": "<base64_encoded_data>",
      "inline": true,
      "content_id": "chart_img"
    }
  ],
  "tags": ["alert", "cpu"]
}
```

---

### 4. Command-Line Interface (CLI)

```bash
# Start consumer daemon (default mode)
mailbaby server -c config.yaml

# Validate configuration syntax and test connectivity to external dependencies
mailbaby check -c config.yaml

# Send an instant email from the command line
mailbaby send -c config.yaml \
  --to "recipient@example.com" \
  --subject "Urgent System Notification" \
  --body "All services are operational." \
  --account "default"

# Show binary version, Git commit SHA, and Go runtime build information
mailbaby version
```

---

## ⚙️ Configuration Reference

MailBaby is configured via a single, self-explanatory YAML file (`config.yaml`). Below is an overview of the core sections:

```yaml
# 1. Base Application Settings
app:
  name: "mailbaby"
  env: "production"             # development | test | staging | production
  debug: false
  shutdown_timeout: "15s"       # In-flight task drain timeout

# 2. Unified HTTP Server (Metrics, Health, REST API)
server:
  host: "0.0.0.0"
  port: 8080
  read_timeout: "10s"
  write_timeout: "10s"

# 3. gRPC Service Server
grpc:
  enabled: true
  host: "0.0.0.0"
  port: 8081
  max_recv_msg_size: 16777216  # 16MB payload limit

# 4. Security & Authentication
auth:
  enabled: true
  secret_key: "your-strong-api-token"
  header_name: "X-API-Key"     # Also accepts Authorization: Bearer <token>

# 5. Multi-Account SMTP Configuration
smtp:
  default:
    host: "smtp.example.com"
    port: 587
    username: "noreply@example.com"
    password: "your_smtp_password"
    from: "noreply@example.com"
    from_name: "MailBaby Alerts"
    encryption: "Auto"          # Auto | SSL | TLS | STARTTLS | None
    auth_type: "Auto"           # Auto | PLAIN | LOGIN | CRAM-MD5 | None
    connect_timeout: "10s"
    send_timeout: "30s"
    pool:
      max_idle_conns: 5         # Idle connection cache
      max_open_conns: 25        # Concurrency limit per account
      idle_timeout: "60s"
    rate_limit:
      emails_per_second: 50     # Token bucket rate cap
      max_recipients_per_email: 50
      email_size_limit: 15728640 # 15MB size limit

  # Secondary Account for Marketing campaigns
  marketing:
    host: "smtp.sendgrid.net"
    port: 465
    username: "apikey"
    password: "sendgrid_api_key"
    from: "news@marketing.example.com"
    encryption: "SSL"
    rate_limit:
      emails_per_second: 20

# 6. Message Queue Subsystem
queue:
  driver: "rabbitmq"            # memory | rabbitmq | kafka | redis | rocketmq | nats | pulsar | sqs
  concurrency: 20               # Number of concurrent consumer worker goroutines
  max_retries: 3                # Retry attempts before routing to DLQ
  retry_interval: "5s"          # Exponential/constant backoff
  prefetch_count: 10            # QoS message prefetch buffer

  rabbitmq:
    url: "amqp://guest:guest@127.0.0.1:5672/"
    queue: "mailbaby_tasks"
    exchange: "mailbaby_exchange"
    routing_key: "mail.send.#"
    durable: true

# 7. Cloud-Native Observability
metrics:
  enabled: true
  provider: "prometheus"        # prometheus | statsd | expvar
  path: "/metrics"
  collect_runtime: true
  collect_queue_stats: true
  collect_smtp_stats: true

observability:
  tracing:
    enabled: true
    provider: "otlp"            # otlp | jaeger | zipkin | stdout
    endpoint: "otel-collector:4317"
    sample_rate: 1.0
  health:
    enabled: true
    live_path: "/livez"
    ready_path: "/readyz"
  pprof:
    enabled: false
    path: "/debug/pprof"
```

*For a full template containing all 8 queue driver settings, see [`config.yaml.example`](config.yaml.example).*

---

## 📊 Observability & Operations

### Prometheus Metrics

MailBaby exports detailed operational metrics out-of-the-box on `GET /metrics`:

| Metric Name | Type | Description | Key Labels |
|---|---|---|---|
| `mailbaby_emails_sent_total` | Counter | Total email delivery attempts | `account`, `status` (`success`/`failed`) |
| `mailbaby_email_duration_seconds` | Histogram | Latency of SMTP delivery transactions | `account`, `le` |
| `mailbaby_queue_messages_total` | Counter | Total queue messages processed | `driver`, `topic`, `status` |
| `mailbaby_queue_duration_seconds` | Histogram | Processing duration per queue message | `driver`, `topic`, `le` |
| `mailbaby_queue_depth` | Gauge | Current pending message depth in queue | `driver`, `topic` |
| `mailbaby_smtp_pool_active_connections` | Gauge | Active connections currently in use | `account` |
| `mailbaby_smtp_pool_idle_connections` | Gauge | Idle connections waiting in pool | `account` |
| `mailbaby_http_requests_total` | Counter | Inbound HTTP REST request volume | `handler`, `method`, `code` |
| `mailbaby_http_request_duration_seconds` | Histogram | Latency of HTTP REST operations | `handler`, `method`, `le` |
| `mailbaby_app_uptime_seconds` | Gauge | Service uptime duration in seconds | - |

---

### Distributed Tracing (OpenTelemetry)

MailBaby automatically instruments the entire delivery path with OpenTelemetry spans:
- **`http.server_request` / `grpc.send_email_sync`**: Incoming API request capture.
- **`queue.consume_message`**: Message dequeue with W3C `traceparent` carrier extraction.
- **`smtp.deliver_email`**: SMTP handshake, SASL auth, and MIME transmission trace.

Export spans to any OTLP-compliant collector (OpenTelemetry Collector, Jaeger, Tempo, Datadog) by setting `observability.tracing.enabled: true`.

---

### Container Health Probes

- **Liveness Probe (`/livez`)**: Returns `200 OK` if the process is responsive.
- **Readiness Probe (`/readyz`)**: Returns `200 OK` only when:
  - The runtime consumer engine is running.
  - The configured message broker is reachable via ping.
  - The SMTP connection pools are healthy.

---

### Runtime Profiling (pprof)

Enable `observability.pprof.enabled: true` to access Go runtime diagnostics:

```bash
# Capture 30-second CPU profile
go tool pprof http://localhost:8080/debug/pprof/profile?seconds=30

# Inspect heap memory allocation
go tool pprof http://localhost:8080/debug/pprof/heap
```

---

## 🛠️ Makefile & Tooling

| Make Target | Description |
|---|---|
| `make build` | Generate protobuf stubs and compile static binary to `build/bin/mailbaby` |
| `make proto` | Recompile Protocol Buffer and gRPC Go stubs from `proto/mailbaby.proto` |
| `make test` | Run complete unit and integration test suites across all packages |
| `make check` | Run configuration validation and network connectivity check |
| `make run` | Compile and launch the consumer daemon with `config.yaml` |
| `make docker` | Build production-ready, non-root Docker container image |
| `make clean` | Clean up build artifacts and compiled binaries |
| `make help` | Display list of all available Makefile commands |

---

## 🗺️ Roadmap

- [x] Multi-account SMTP connection pooling & rate limiting
- [x] 8 Queue drivers unified abstraction
- [x] RESTful HTTP API & gRPC MailService
- [x] OpenTelemetry distributed tracing & Prometheus metrics
- [x] Helm Chart & Docker deployment
- [ ] Webhook delivery callbacks (success / bounce / drop notifications)
- [ ] Template rendering engine (Mustache / Go template support with remote storage)
- [ ] Redis Cluster mode & Sentinel auto-failover enhancements
- [ ] Admin Web Dashboard for real-time queue monitoring and manual retry trigger

---

## 🤝 Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

1. **Fork the Project**
2. **Create your Feature Branch** (`git checkout -b feature/AmazingFeature`)
3. **Commit your Changes** (`git commit -m 'Add some AmazingFeature'`)
4. **Push to the Branch** (`git push origin feature/AmazingFeature`)
5. **Open a Pull Request**

Please ensure tests pass before submitting PRs:
```bash
make test
```

---

## 📄 License

Distributed under the **Apache License 2.0**. See [`LICENSE`](LICENSE) for more information.

Copyright (c) 2026 The MailBaby Authors.
