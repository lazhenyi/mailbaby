# MailBaby

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/Go-1.26-blue)](go.mod)
[![CI](https://img.shields.io/badge/CI-github%20actions-lightgrey)](.github/workflows/ci.yml)

[简体中文](README.zh-CN.md)
[English](README.md)

**MailBaby** is a message-queue-driven email sending service. It consumes email tasks from a message broker (RabbitMQ, Kafka, Redis, RocketMQ, NATS, Pulsar, AWS SQS, or an in-memory queue) and delivers them through SMTP — with retries, dead-letter handling, rate limiting, multi-account SMTP support, and built-in observability (metrics, tracing, health probes).

> **Status: not production-ready**
>
> 
> MailBaby is under active development. It is **not yet recommended for production use** — features, configuration, and behavior may change at any time.

## Features

- **8 queue drivers** behind one unified interface: RabbitMQ, Kafka, Redis (Stream/List/PubSub), RocketMQ, NATS/JetStream, Apache Pulsar, AWS SQS, and in-memory
- **Multi-account SMTP** — each account owns an independent connection pool, encryption mode (Auto/SSL/TLS/STARTTLS/None), auth mechanism (Auto/PLAIN/LOGIN/CRAM-MD5/None), and rate limit
- **Reliable delivery** — bounded retries with backoff, per-message attempt tracking, dead-letter queue (DLQ) routing, at-least-once semantics
- **Middleware pipeline** — recovery, distributed tracing, metrics, and structured logging around every consumed message
- **Observability** — Prometheus / StatsD / Pushgateway / expvar metrics, OTLP tracing, `/livez` & `/readyz` health probes, pprof profiling
- **Graceful shutdown** — drains in-flight tasks within a configurable timeout
- **SMTP connection pool** — bounded idle/active connections with idle timeout; rate limiter with per-second token bucket
- **CLI** — one binary for daemon, config check, and manual test send; version & build metadata
- **Static binary & Docker** — CGO-free build, non-root container, HEALTHCHECK wired to `/livez`

## Quick Start

### Prerequisites

- Go 1.26 or newer (see `go.mod`)
- A running message broker (or use the built-in `memory` driver for local testing)
- An SMTP relay account

### Build

```sh
make build              # binary → build/bin/mailbaby
# or manually:
CGO_ENABLED=0 go build -trimpath -o build/bin/mailbaby .
```

### Run the daemon

```sh
cp config.yaml.example config.yaml   # fully commented config template
make run                             # = ./build/bin/mailbaby server -c config.yaml
```

### Validate configuration & connectivity

```sh
./build/bin/mailbaby check -c config.yaml
```

### Send a test email via CLI

```sh
./build/bin/mailbaby send -c config.yaml --to you@example.com --subject "Hello" --body "MailBaby works!"
```

### Docker

```sh
make docker            # mailbaby:1.0.0 / mailbaby:latest
docker run -d --name mailbaby \
  -p 8080:8080 \
  -v "$PWD/config.yaml":/app/config.yaml:ro \
  mailbaby:latest
```

## License

Licensed under the [Apache License 2.0](LICENSE). Copyright 2026 The MailBaby Authors.
