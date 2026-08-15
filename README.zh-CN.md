<div align="center">

<img src="assets/logo.png" alt="MailBaby Logo" width="180" />

# 📬 MailBaby

**基于 Go 语言的高性能、云原生、多队列驱动邮件投递微服务**

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

**MailBaby** 是一个企业级、高吞吐量的消息队列驱动邮件发送微服务。它旨在将业务系统从缓慢且脆弱的同步 SMTP 网络通信中彻底解耦，支持从主流消息中间件（RabbitMQ、Kafka、Redis、RocketMQ、NATS、Pulsar、AWS SQS 或内存队列）异步消费邮件任务，并通过 SMTP 完成高可用、高可靠的邮件投递。

MailBaby 具备 **多账号 SMTP 路由**、**独立连接池与令牌桶限流**、**At-Least-Once 语义与死信队列（DLQ）**、**多协议接入（REST / gRPC / 消息队列）** 以及 **云原生全栈可观测性（Prometheus、OpenTelemetry、K8s 探针、pprof）**。

> [!NOTE]
> **项目状态：活跃开发中**
> MailBaby 正处于快速迭代阶段。尽管核心功能完备且经过严格测试，在发布 v1.0.0 正式版之前，部分配置项与内部接口可能仍会发生演进。

---

## 📑 目录

- [✨ 核心特性](#-核心特性)
- [🏗️ 系统架构设计](#️-系统架构设计)
- [🔌 支持的队列驱动矩阵](#-支持的队列驱动矩阵)
- [🚀 快速开始](#-快速开始)
  - [环境准备](#环境准备)
  - [方式一：Docker 运行（最快捷）](#方式一docker-运行最快捷)
  - [方式二：源码编译与本地运行](#方式二源码编译与本地运行)
  - [方式三：Kubernetes Helm 部署](#方式三kubernetes-helm-部署)
- [📬 邮件投递接口](#-邮件投递接口)
  - [1. HTTP RESTful API](#1-http-restful-api)
  - [2. gRPC RPC 服务](#2-grpc-rpc-服务)
  - [3. 消息队列直接投递](#3-消息队列直接投递)
  - [4. CLI 命令行工具](#4-cli-命令行工具)
- [⚙️ 配置详解](#️-配置详解)
- [📊 可观测性与运维指南](#-可观测性与运维指南)
  - [Prometheus 指标监控](#prometheus-指标监控)
  - [分布式链路追踪 (OpenTelemetry)](#分布式链路追踪-opentelemetry)
  - [容器健康探针 (Health Probes)](#容器健康探针-health-probes)
  - [运行时性能分析 (pprof)](#运行时性能分析-pprof)
- [🛠️ Makefile 与开发指令](#️-makefile-与开发指令)
- [🗺️ 发展路线图 (Roadmap)](#️-发展路线图-roadmap)
- [🤝 参与贡献](#-参与贡献)
- [📄 开源许可证](#-开源许可证)

---

## ✨ 核心特性

- **统一抽象下的 8 种主流队列驱动**：支持一键无缝切换 RabbitMQ、Apache Kafka、Redis (Stream/List/PubSub)、Apache RocketMQ、NATS/JetStream、Apache Pulsar、AWS SQS 与内置内存队列，无需修改任何业务代码。
- **多账号 SMTP 路由与隔离**：支持声明多个 SMTP 发件账号（如验证码通道、营销通道、告警通道等）。每个账号拥有独立的凭据、连接池配额、TLS 加密模式、SASL 认证协议以及令牌桶速率限制。
- **多协议接入支持**：既可通过 MQ 异步解耦投递，也可通过高性能 HTTP REST API（`/v1/email/send`、`/v1/email/batch`）或 gRPC 服务（`mailbaby.v1.MailService`）进行同步/异步调用。
- **零丢失与高可靠保证**：具备带退避的有界重试、消息尝试计数、显式 Manual ACK/NACK 机制以及自动死信队列（DLQ）流转。
- **智能连接池与防封限流**：内置有界空闲/活跃 SMTP 连接池管理与自动保活机制；支持账号级令牌桶限流（如 50 封/秒），从容应对主流邮件服务商（Gmail、SendGrid、AWS SES 等）的频控限制。
- **完整 MIME 与富文本支持**：支持 HTML 富文本、纯文本 fallback、多附件上传以及基于 Content-ID 的内联资源嵌入（`<img src="cid:xxx">`）。
- **云原生可观测体系**：
  - **指标监控**：Prometheus OpenMetrics 标准接口（`/metrics`）、StatsD 客户端、Prometheus Pushgateway 与 Go 原生 `expvar`。
  - **分布式追踪**：基于 OpenTelemetry（OTLP gRPC/HTTP、Jaeger、Zipkin）跨越消息队列透传 W3C Trace Context。
  - **健康探针**：标准 Kubernetes `/livez` 存活探针与深度 `/readyz` 依赖就绪检查。
  - **性能诊断**：内置 Go 运行时 `pprof` 性能剖析接口。
- **安全与权限控制**：支持 Link Secret / API Key 访问鉴权（`X-API-Key` 或 `Authorization: Bearer <key>`）。
- **极简部署与轻量化**：无 CGO 依赖的静态二进制文件、极简非 root 容器镜像（`uid: 10001`）及生产就绪的 Helm Chart。

---

## 🏗️ 系统架构设计

```
                                  [ 业务客户端应用 ]
                                          │
      ┌───────────────────────────────────┼───────────────────────────────────┐
      │                                   │                                   │
 [ HTTP REST API ]                 [ gRPC 服务 ]                       [ 消息中间件直接投递 ]
(POST /v1/email/send)          (mailbaby.v1.MailService)           (Kafka/RabbitMQ/Redis/...)
      │                                   │                                   │
      └─────────────────┬─────────────────┴───────────────────────────────────┘
                        ▼
          ┌───────────────────────────┐
          │   鉴权与中间件拦截器流水线 │ (API Key 鉴权 / Panic 恢复 / OTel 追踪 / 指标记录)
          └─────────────┬─────────────┘
                        │
                        ▼
            [ 统一消息队列抽象层 ]
            ┌─────────────────────────────────────────────────────────────┐
            │ RabbitMQ │ Kafka │ Redis │ RocketMQ │ NATS │ Pulsar │ SQS │ │
            └─────────────────────────────┬───────────────────────────────┘
                                          │
                        ┌─────────────────┴─────────────────┐
                        ▼                                   ▼
             [ 高并发 Worker 消费池 ]              [ 死信队列 (DLQ) ]
             (QoS 预取 / 指数退避重试)             (不可恢复消息归档)
                        │
                        ▼
             [ 多账号 SMTP 智能路由器 ]
        ┌───────────────┼───────────────┐
        ▼               ▼               ▼
 ┌──────────────┐┌──────────────┐┌──────────────┐
 │   账号配置:   ││   账号配置:   ││   账号配置:   │
 │   default    ││  marketing   ││    alert     │
 ├──────────────┤├──────────────┤├──────────────┤
 │ 令牌桶限流器 ││ 令牌桶限流器 ││ 令牌桶限流器 │
 │ SMTP 连接池  ││ SMTP 连接池  ││ SMTP 连接池  │
 └──────┬───────┘└──────┬───────┘└──────┬───────┘
        │               │               │
        ▼               ▼               ▼
 ┌──────────────┐┌──────────────┐┌──────────────┐
 │ SMTP 服务器  ││ SendGrid /   ││ 自建 Postfix │
 │ (Office365)  ││ Mailgun 网关 ││ / 内网中继   │
 └──────────────┘└──────────────┘└──────────────┘
```

---

## 🔌 支持的队列驱动矩阵

MailBaby 将所有底层队列抽象为统一的 Go 接口（`queue.Queue`、`queue.Producer`、`queue.Consumer`）：

| 驱动名称 | 底层协议 / 组件 | 消费工作模式 | 确认机制 (ACK) | 死信队列 (DLQ) | 推荐生产场景 |
|---|---|---|:---:|:---:|---|
| **`memory`** | Go 原生 Channel | 进程内内存缓冲 | 内存级 ACK | 支持内存 DLQ | 本地开发、单元测试、独立单机应用 |
| **`rabbitmq`** | AMQP 0-9-1 | Exchange/Queue 绑定 | 显式手动 ACK/NACK | 支持 (AMQP DLX) | 企业级微服务、复杂路由投递场景 |
| **`kafka`** | Apache Kafka | 分区 Topic + 消费组 | 成功投递后提交 Offset | 支持 (DLQ Topic) | 超大吞吐量、事件驱动与流式架构 |
| **`redis`** | Redis 5.0+ | Streams / List / PubSub | XACK (Stream 模式) | 支持 | 轻量化部署、已有 Redis 基础设施 |
| **`rocketmq`** | Apache RocketMQ | Topic + 消费组 | ACK / ReconsumeLater | 支持 (RocketMQ %DLQ%) | 金融级消息传递、严格有序消息需求 |
| **`nats`** | NATS / JetStream | JetStream 持久化消费者 | JetStream 显式 ACK | 支持 (NATS Subject) | 极低延迟通信、云边协同分布式系统 |
| **`pulsar`** | Apache Pulsar | 持久化多租户 Topic | 单条 / 累积 ACK | 支持 (Pulsar DLQ Policy)| 云原生多租户平台、跨地域复制 |
| **`sqs`** | AWS SQS | 标准 / FIFO 队列 | 成功后 DeleteMessage | 支持 (SQS Redrive Policy) | AWS Serverless 及云原生托管环境 |

---

## 🚀 快速开始

### 环境准备

- **Go 语言环境**：Go 1.26 或更高版本（若从源码编译）
- **SMTP 服务凭据**：任意可用的 SMTP 服务（如 企业微信邮箱、SendGrid、AWS SES、自建 Postfix、Gmail 等）
- **消息中间件**：（可选）RabbitMQ、Kafka、Redis 等；本地调试可直接选用内置的 `memory` 内存驱动。

---

### 方式一：Docker 运行（最快捷）

1. **生成配置文件**：
   ```bash
   cp config.yaml.example config.yaml
   # 根据实际情况修改 config.yaml 中的 SMTP 连接凭据
   ```

2. **启动 Docker 容器**：
   ```bash
   docker run -d \
     --name mailbaby \
     -p 8080:8080 \
     -p 8081:8081 \
     -v "$(pwd)/config.yaml":/app/config.yaml:ro \
     mailbaby:latest
   ```

3. **检查就绪状态**：
   ```bash
   curl http://localhost:8080/readyz
   ```

---

### 方式二：源码编译与本地运行

1. **克隆项目并编译**：
   ```bash
   git clone https://github.com/mailbabys/mailbaby.git
   cd mailbaby
   make build
   ```

2. **验证配置与外部连通性**：
   ```bash
   ./build/bin/mailbaby check -c config.yaml
   ```

3. **启动守护进程**：
   ```bash
   ./build/bin/mailbaby server -c config.yaml
   ```

4. **通过命令行发送测试邮件**：
   ```bash
   ./build/bin/mailbaby send -c config.yaml \
     --to "developer@example.com" \
     --subject "MailBaby 快速上手" \
     --body "来自 MailBaby 的第一封测试邮件！"
   ```

---

### 方式三：Kubernetes Helm 部署

借助官方 Helm Chart，将 MailBaby 一键部署至任何 Kubernetes 集群：

```bash
# 使用自定义 values 部署
helm install mailbaby ./charts/mailbaby \
  -n mailbaby \
  --create-namespace \
  -f my-values.yaml
```

*详细的 Helm 配置参数请参考 [charts/README.md](charts/README.md)。*

---

## 📬 邮件投递接口

### 1. HTTP RESTful API

MailBaby 内置统一的 HTTP REST 服务，支持同步直接投递与异步入队投递。

#### 发送单封邮件 (`POST /v1/email/send`)

> **请求头**：`Content-Type: application/json`，`X-API-Key: <your_secret_key>`（如果开启了鉴权）

**同步发送请求**（等待 SMTP 握手完成并返回投递结果）：
```bash
curl -X POST http://localhost:8080/v1/email/send \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your_secret_key" \
  -d '{
    "account": "default",
    "from": "noreply@example.com",
    "from_name": "系统通知",
    "to": ["alice@example.com"],
    "cc": ["manager@example.com"],
    "subject": "订单确认通知 #10024",
    "text_body": "您的订单已支付成功，物流单号：987654。",
    "html_body": "<h2>订单支付成功</h2><p>物流单号：<b>987654</b></p>",
    "tags": ["order", "receipt"]
  }'
```

**响应结果 (`200 OK`)**：
```json
{
  "id": "e8a93bf84c379a20",
  "status": "sent",
  "message": "email sent successfully",
  "sent_at": 1771142400000000000
}
```

**异步入队请求**（写入消息队列后立即返回 `202 Accepted`，由后台 Worker 异步发送）：
```bash
curl -X POST "http://localhost:8080/v1/email/send?async=true" \
  -H "Content-Type: application/json" \
  -d '{
    "to": ["bob@example.com"],
    "subject": "欢迎注册平台！",
    "html_body": "<h1>欢迎加入我们！</h1>"
  }'
```

---

#### 批量发送邮件 (`POST /v1/email/batch`)

在单个 HTTP 请求中并发处理多封邮件投递：

```bash
curl -X POST http://localhost:8080/v1/email/batch \
  -H "Content-Type: application/json" \
  -d '{
    "async": false,
    "emails": [
      {
        "to": ["user1@example.com"],
        "subject": "月度账单通知",
        "text_body": "请查收您的本月账单汇总。"
      },
      {
        "to": ["user2@example.com"],
        "subject": "月度账单通知",
        "text_body": "请查收您的本月账单汇总。"
      }
    ]
  }'
```

**响应结果 (`200 OK`)**：
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

### 2. gRPC RPC 服务

MailBaby 提供严格规范的 gRPC 定义，位于 [`proto/mailbaby.proto`](proto/mailbaby.proto)：

```protobuf
service MailService {
  rpc Send(SendMailRequest) returns (SendMailResponse);
  rpc SendBatch(BatchSendMailRequest) returns (BatchSendMailResponse);
  rpc Ping(PingRequest) returns (PingResponse);
  rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
}
```

**通过 `grpcurl` 工具调用示例**：
```bash
grpcurl -plaintext \
  -H "authorization: Bearer your_secret_key" \
  -d '{"to": ["dev@example.com"], "subject": "gRPC 测试邮件", "text_body": "通过 MailService.Send 发送"}' \
  localhost:8081 mailbaby.v1.MailService/Send
```

---

### 3. 消息队列直接投递

任何编程语言（Python、Java、Node.js、Go、PHP、Rust、C# 等）构建的业务服务，均可直接向消息中间件（Kafka Topic、RabbitMQ Queue、Redis Stream 等）发布 JSON 格式的邮件任务：

**消息体 JSON Schema 规范**：
```json
{
  "id": "optional-custom-uuid-12345",
  "account": "default",
  "from": "alerts@example.com",
  "from_name": "运维监控告警",
  "reply_to": "support@example.com",
  "to": ["oncall@example.com"],
  "cc": ["lead@example.com"],
  "subject": "【严重告警】prod-db-01 服务器 CPU 负载过高",
  "text_body": "CPU 负载持续 5 分钟超过 95%，请及时处理。",
  "html_body": "<h2 style='color:red;'>系统异常告警</h2><p>CPU 负载已达到 <b>95%</b>。</p>",
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

### 4. CLI 命令行工具

```bash
# 启动邮件消费守护进程 (默认子命令)
mailbaby server -c config.yaml

# 校验配置文件语法，并主动探测外部依赖连接性 (MQ / SMTP)
mailbaby check -c config.yaml

# 命令行直接发送单封即时测试邮件
mailbaby send -c config.yaml \
  --to "recipient@example.com" \
  --subject "重要通知" \
  --body "服务巡检正常。" \
  --account "default"

# 输出构建版本号、Git Commit Hash 及 Go 运行平台环境信息
mailbaby version
```

---

## ⚙️ 配置详解

MailBaby 采用结构清晰、自带详细注释的 YAML 格式配置文件（`config.yaml`）：

```yaml
# 1. 基础应用信息
app:
  name: "mailbaby"
  env: "production"             # development | test | staging | production
  debug: false
  shutdown_timeout: "15s"       # 优雅停机等待未完成任务排空的超时时间

# 2. 统一 HTTP 服务 (Prometheus 指标 / 健康检查 / REST API)
server:
  host: "0.0.0.0"
  port: 8080
  read_timeout: "10s"
  write_timeout: "10s"

# 3. gRPC 服务配置
grpc:
  enabled: true
  host: "0.0.0.0"
  port: 8081
  max_recv_msg_size: 16777216  # 最大接收消息大小 (16MB)

# 4. API 鉴权机制
auth:
  enabled: true
  secret_key: "your-strong-api-token"
  header_name: "X-API-Key"     # 同时也支持 Authorization: Bearer <token>

# 5. 多账号 SMTP 发信配置
smtp:
  default:
    host: "smtp.example.com"
    port: 587
    username: "noreply@example.com"
    password: "your_smtp_password"
    from: "noreply@example.com"
    from_name: "MailBaby 系统邮件"
    encryption: "Auto"          # Auto | SSL | TLS | STARTTLS | None
    auth_type: "Auto"           # Auto | PLAIN | LOGIN | CRAM-MD5 | None
    connect_timeout: "10s"
    send_timeout: "30s"
    pool:
      max_idle_conns: 5         # 空闲连接缓存池大小
      max_open_conns: 25        # 单账号最大并发连接数
      idle_timeout: "60s"
    rate_limit:
      emails_per_second: 50     # 令牌桶每秒发信速率上限
      max_recipients_per_email: 50
      email_size_limit: 15728640 # 单封邮件最大字节数 (15MB)

  # 备用营销发信通道
  marketing:
    host: "smtp.sendgrid.net"
    port: 465
    username: "apikey"
    password: "sendgrid_api_key"
    from: "news@marketing.example.com"
    encryption: "SSL"
    rate_limit:
      emails_per_second: 20

# 6. 消息队列配置
queue:
  driver: "rabbitmq"            # memory | rabbitmq | kafka | redis | rocketmq | nats | pulsar | sqs
  concurrency: 20               # 并发 Worker Goroutine 数量
  max_retries: 3                # 投递失败流转死信队列前的最大重试次数
  retry_interval: "5s"          # 重试退避间隔
  prefetch_count: 10            # 队列 QoS 预取缓冲消息数

  rabbitmq:
    url: "amqp://guest:guest@127.0.0.1:5672/"
    queue: "mailbaby_tasks"
    exchange: "mailbaby_exchange"
    routing_key: "mail.send.#"
    durable: true

# 7. 全栈可观测性配置
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

*包含全部 8 大队列驱动详细参数的模板，请查阅 [`config.yaml.example`](config.yaml.example)。*

---

## 📊 可观测性与运维指南

### Prometheus 指标监控

MailBaby 在 `GET /metrics` 端点原生暴露生产级 Prometheus 指标：

| 指标名称 | 类型 | 作用描述 | 核心标签 (Labels) |
|---|---|---|---|
| `mailbaby_emails_sent_total` | Counter | 累计邮件投递次数 | `account`, `status` (`success`/`failed`) |
| `mailbaby_email_duration_seconds` | Histogram | SMTP 实际投递耗时分布 | `account`, `le` |
| `mailbaby_queue_messages_total` | Counter | 累计消费的消息总量 | `driver`, `topic`, `status` |
| `mailbaby_queue_duration_seconds` | Histogram | 队列单条消息处理耗时分布 | `driver`, `topic`, `le` |
| `mailbaby_queue_depth` | Gauge | 队列当前积压消息深度 | `driver`, `topic` |
| `mailbaby_smtp_pool_active_connections` | Gauge | 当前正在使用的 SMTP 活跃连接数 | `account` |
| `mailbaby_smtp_pool_idle_connections` | Gauge | SMTP 连接池内的空闲连接数 | `account` |
| `mailbaby_http_requests_total` | Counter | 接收到的 HTTP API 请求总量 | `handler`, `method`, `code` |
| `mailbaby_http_request_duration_seconds` | Histogram | HTTP API 请求处理延迟分布 | `handler`, `method`, `le` |
| `mailbaby_app_uptime_seconds` | Gauge | 服务的持续运行时间（秒） | - |

---

### 分布式链路追踪 (OpenTelemetry)

MailBaby 全链路内嵌 OpenTelemetry 追踪埋点：
- **`http.server_request` / `grpc.send_email_sync`**：记录入站 API 请求。
- **`queue.consume_message`**：消费消息时自动提取并延续上游的 W3C `traceparent` 链路上下文。
- **`smtp.deliver_email`**：记录 SMTP 连接拨号、认证及正文传输耗时与异常。

只需设置 `observability.tracing.enabled: true`，即可无缝对接 OpenTelemetry Collector、Jaeger、Tempo 或 SkyWalking。

---

### 容器健康探针 (Health Probes)

- **存活探针 (`/livez`)**：进程响应正常即返回 `200 OK`。
- **就绪探针 (`/readyz`)**：仅当以下关键子系统均健康时返回 `200 OK`：
  - 核心消费引擎正常运行中；
  - 目标消息中间件 Ping 探测可达；
  - SMTP 连接池初始化就绪。

---

### 运行时性能分析 (pprof)

启用 `observability.pprof.enabled: true` 即可进行在线性能诊断：

```bash
# 采集 30 秒 CPU 剖析采样
go tool pprof http://localhost:8080/debug/pprof/profile?seconds=30

# 查看堆内存分配情况
go tool pprof http://localhost:8080/debug/pprof/heap
```

---

## 🛠️ Makefile 与开发指令

| 指令 | 说明 |
|---|---|
| `make build` | 编译生成静态二进制文件至 `build/bin/mailbaby` |
| `make proto` | 根据 `proto/mailbaby.proto` 重新生成 gRPC 与 Protobuf Go 代码 |
| `make test` | 执行全工程自动化单元测试与集成测试 |
| `make check` | 校验配置文件合法性并检查依赖组件连通性 |
| `make run` | 编译并直接启动基于 `config.yaml` 的守护进程 |
| `make docker` | 构建生产环境安全极简的 Docker 容器镜像 |
| `make clean` | 清理编译产物与临时文件 |
| `make help` | 输出 Makefile 支持的全部指令说明 |

---

## 🗺️ 发展路线图 (Roadmap)

- [x] 多账号 SMTP 连接池管理与令牌桶限流
- [x] 8 大消息队列驱动统一抽象层
- [x] HTTP REST API 与 gRPC MailService 多协议接入
- [x] OpenTelemetry 链路追踪与 Prometheus 监控指标
- [x] Docker 容器化与 Kubernetes Helm Chart 支持
- [ ] Webhook 投递结果异步回调通知（成功 / 弹信 / 拒收）
- [ ] 模板渲染引擎（支持 Mustache / Go Template 与远程模板存储）
- [ ] Redis Cluster 集群模式与 Sentinel 故障自动切换深度适配
- [ ] 队列可视化 Web 管理面板（实时积压监控与死信一键重试）

---

## 🤝 参与贡献

我们非常欢迎来自开源社区的任何形式的贡献与建议！

1. **Fork 本仓库**
2. **创建你的功能分支** (`git checkout -b feature/AmazingFeature`)
3. **提交你的改动** (`git commit -m 'feat: add some amazing feature'`)
4. **推送到远程分支** (`git push origin feature/AmazingFeature`)
5. **提交 Pull Request**

提交 PR 前请确保所有自动化测试均通过：
```bash
make test
```

---

## 📄 开源许可证

本项目基于 **Apache License 2.0** 许可证开源。详情请参阅 [`LICENSE`](LICENSE) 文件。

Copyright (c) 2026 The MailBaby Authors.
