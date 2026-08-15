# MailBaby

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/Go-1.26-blue)](go.mod)
[![CI](https://img.shields.io/badge/CI-github%20actions-lightgrey)](.github/workflows/ci.yml)

[简体中文](README.zh-CN.md)
[English](README.md)

**MailBaby** 是一个基于消息队列驱动的邮件发送服务。它从消息中间件（RabbitMQ、Kafka、Redis、RocketMQ、NATS、Pulsar、AWS SQS 或内存队列）消费邮件任务，并通过 SMTP 完成投递——内置重试、死信处理、限流、多账号 SMTP 支持，以及开箱即用的可观测性（指标、追踪、健康探针）。

> **状态：不建议上生产**
>
> MailBaby 正处于活跃开发阶段，**目前不建议用于生产环境**——功能、配置与行为随时可能发生变化。

## 特性

- **统一接口下的 8 种队列驱动**：RabbitMQ、Kafka、Redis（Stream/List/PubSub）、RocketMQ、NATS/JetStream、Apache Pulsar、AWS SQS、内存队列
- **多账号 SMTP**：每个账号拥有独立的连接池、加密模式（Auto/SSL/TLS/STARTTLS/None）、认证机制（Auto/PLAIN/LOGIN/CRAM-MD5/None）与速率限制
- **可靠投递**：带退避的有界重试、按消息跟踪尝试次数、死信队列（DLQ）路由、at-least-once 语义
- **中间件流水线**：每条消费消息都依次经过恢复（recovery）、分布式追踪、指标采集、结构化日志
- **可观测性**：Prometheus / StatsD / Pushgateway / expvar 指标，OTLP 追踪，`/livez` 与 `/readyz` 健康探针，pprof 性能剖析
- **优雅停机**：在可配置超时内排空处理中的任务
- **SMTP 连接池**：有界空闲/活跃连接与空闲超时；按秒的令牌桶限流
- **CLI**：单个二进制即可完成守护进程、配置检查与手动测试发送；内置版本与构建信息
- **静态二进制与 Docker**：无 CGO、非 root 容器、HEALTHCHECK 绑定 `/livez`

## 快速开始

### 环境要求

- Go 1.26 或更高版本（见 `go.mod`）
- 一个可用的消息中间件（本地测试可选用内置 `memory` 驱动）
- 一个 SMTP 中继账号

### 编译

```sh
make build              # 产物 → build/bin/mailbaby
# 或手动执行：
CGO_ENABLED=0 go build -trimpath -o build/bin/mailbaby .
```

### 启动守护进程

```sh
cp config.yaml.example config.yaml   # 带完整注释的配置模板
make run                             # = ./build/bin/mailbaby server -c config.yaml
```

### 校验配置与连通性

```sh
./build/bin/mailbaby check -c config.yaml
```

### 手动发送测试邮件

```sh
./build/bin/mailbaby send -c config.yaml --to you@example.com --subject "你好" --body "MailBaby 可以正常发送了！"
```

### Docker

```sh
make docker            # 镜像 mailbaby:1.0.0 / mailbaby:latest
docker run -d --name mailbaby \
  -p 8080:8080 \
  -v "$PWD/config.yaml":/app/config.yaml:ro \
  mailbaby:latest
```

## 许可证

本项目使用 [Apache License 2.0](LICENSE) 授权。Copyright 2026 The MailBaby Authors。
