# SDK 一致性矩阵 (SDK Consistency Matrix)

本文档登记 MailBaby 4 种语言 SDK（Go、Java、Python、Rust）的行为对齐情况。
所有 SDK 都对接同一组 MailBaby 服务器端 API，因此必须对**错误模型**、
**认证方式**、**指标标签**、**重试/超时策略**保持一致。

## 1. Error Class Mapping

| HTTP code | machine code | Go (`error`)     | Java exception    | Python exception  | Rust `Error::Api`             |
| --------- | ------------ | ---------------- | ----------------- | ----------------- | ----------------------------- |
| 400       | invalid_json | `ErrInvalidJSON` | `InvalidJsonException`     | `MailBabyInvalidJsonError`  | `Error::Api{code, message}` |
| 400       | validation_error | `ErrValidation` | `ValidationException`     | `MailBabyValidationError` | `Error::Api{code, message}` |
| 401       | unauthorized | `ErrUnauthorized` | `UnauthorizedException`    | `MailBabyAuthError`     | `Error::Api{code, message}` |
| 403       | forbidden    | `ErrForbidden`   | `ForbiddenException`       | `MailBabyForbiddenError`   | `Error::Api{code, message}` |
| 404       | not_found    | `ErrNotFound`    | `NotFoundException`         | `MailBabyNotFoundError`    | `Error::Api{code, message}` |
| 429       | rate_limited | `ErrRateLimited` | `RateLimitedException`      | `MailBabyRateLimitError`  | `Error::Api{code, message}` |
| 5xx       | internal_error / delivery_failed | `ErrDeliveryFailed` / `ErrInternal` | `InternalException` / `DeliveryFailedException` | `MailBabyServerError` / `MailBabyDeliveryError` | `Error::Api{code, message}` |

**约定**：
- 所有 SDK 必须把 HTTP `code`、`message`、`details` 三个字段暴露给调用方。
- 4xx 必须映射到业务异常类（不重试）；5xx 视客户端策略可重试（建议带指数退避）。
- `details` 缺失时 Rust 用 `None`，Python 用 `None`，Java 用 `null`，Go 用空字符串。

## 2. Auth Header Conventions

| 渠道 | 客户端发送方式 | 服务器接收方式 |
| ---- | -------------- | -------------- |
| REST | `X-API-Key: <key>`（默认）或 `Authorization: Bearer <key>` | 优先 X-API-Key（`auth.header_name`），回退 Bearer |
| gRPC | `authorization: Bearer <key>` 或 `x-api-key: <key>` metadata | 同时支持两个 metadata 头 |
| MQ   | 不适用（凭 broker SASL/TLS 鉴权） | 不适用 |

**约定**：
- 4 个 SDK 都暴露 `AuthScheme::{Header, Bearer, Query}`（或等价枚举）供调用方选择。
- **Query 参数方式仅作向后兼容**，所有 SDK 的 README/文档需提示其会进入 URL 日志。
- 服务器在 `auth.enabled=false` 时跳过认证检查；SDK 不应在 disabled 模式下拒绝发送。

## 3. Metric / Tracing Labels

所有 SDK 在调用 MailBaby 时应该上报的客户端指标（label 一致性）：

| 指标                 | 类型       | labels                                  | 单位     |
| -------------------- | ---------- | --------------------------------------- | -------- |
| `mailbaby_client_requests_total` | counter | `sdk=<lang>`, `channel=<rest|grpc|mq>`, `status=<code>` | reqs |
| `mailbaby_client_request_duration_seconds` | histogram | `sdk=<lang>`, `channel`, `status` | seconds |
| `mailbaby_client_errors_total`   | counter | `sdk=<lang>`, `channel`, `error=<class>` | errors   |

`channel` 枚举：`rest` / `grpc` / `mq-rabbitmq` / `mq-redis` / `mq-kafka` / `mq-rocketmq` / `mq-nats` / `mq-pulsar` / `mq-sqs` / `mq-memory`。

**约定**：
- `sdk` 取值：`go` / `java` / `python` / `rust`。
- `error` 取值必须与服务端错误类对齐（`validation` / `auth` / `rate_limit` / `delivery` / `internal`）。
- 所有客户端指标通过 Prometheus client library 上报，与服务端指标 namespace (`mailbaby_*`) 区分。

## 4. Retry / Timeout Defaults

| 参数               | 默认值          | 备注 |
| ------------------ | --------------- | ---- |
| 单次请求超时        | 30s             | SDK 必须可配置 |
| 重试最大次数        | 3               | 0 = 不重试 |
| 重试退避            | 指数 + ±20% 抖动 | 基础 500ms，最大 5s |
| 可重试错误          | 5xx, 429, 网络错误 | 4xx 业务错误不重试 |
| 不重试错误          | 401, 403, 400 (除 429) | |

**约定**：4 个 SDK 都提供 `RetryPolicy` (或等价) 配置对象；不重试策略下，退避必须仍以 jitter 实现，避免多副本 thundering herd。

## 5. Wire Format

所有 SDK 发出的 JSON 体（仅 REST/MQ）必须与 Go `sender.Email.ToJSON()` 一致：

```json
{
  "id": "",
  "account": "default",
  "from": "noreply@example.com",
  "to": ["alice@example.com"],
  "subject": "hi",
  "text_body": "",
  "html_body": "",
  "headers": {},
  "attachments": [
    {
      "filename": "logo.png",
      "content_type": "image/png",
      "data": "<base64>",
      "inline": false,
      "content_id": ""
    }
  ],
  "tags": [],
  "metadata": {}
}
```

约定：
- `snake_case` 字段名（所有 SDK 一致）。
- `omitempty` 字段：所有 SDK 都必须省略空字符串/空数组字段（避免服务端误判）。
- 附件 `data` 使用标准 base64 + padding（与 Go `encoding/json` 一致）。
- `tags`、`metadata`、`headers` 顺序无要求（`HashMap`/`dict`/`HashMap`/`LinkedHashMap` 均可）。

## 6. gRPC Field Naming

所有 SDK 调用 `MailService.SendMail` / `SendBatchMail` 时，metadata 字段命名：
- `authorization` / `x-api-key`
- `x-request-id`（可选，用于追踪）
- `x-mailbaby-traceparent`（可选，W3C traceparent）

## 7. Version Alignment

| SDK       | 当前版本 |
| --------- | -------- |
| Go        | 0.3.0    |
| Java      | 0.3.0    |
| Python    | 0.3.0    |
| Rust      | 0.3.0    |

**发布规则**：服务端 minor 版本号提升时，4 个 SDK 必须在同一次 release 中对齐 version。patch 版本（bugfix）允许错位发布，但必须在 CHANGELOG 中标注。

## 8. Cross-SDK Smoke Tests

跨 SDK 集成测试矩阵以 **wire-level 兼容层** 形式落地在服务端仓库：

- `internal/compat/sdk_matrix_test.go` — 服务端的 14 个 wire-level 用例
  （`TestSDKMatrix_*`），覆盖鉴权、错误信封、附件 JSON、429 Retry-After、
  5xx 重试、无查询 token 回退、空字段省略、并发安全。SDK 测试套件只需
  启动服务并驱动 4 个客户端 SDK 运行同一组请求，对照服务端 wire-level
  套件验证响应一致即可。
- 4 个 SDK 的 `tests/cross_sdk/` 入口分别调用 `compat.SDKMatrixCases`
  （占位 API），未来按 SDK 添加 driver；当前由服务端 wire-level 套件
  保证契约。

矩阵覆盖：

1. 缺鉴权 → 401 → `error=unauthorized` 信封。
2. 错误 key → 401，且响应 body 不回显凭据。
3. `Authorization: Bearer` 与 `X-API-Key` 两种鉴权都被接受。
4. 空收件人 → 400 `validation_error` 信封。
5. 非法 JSON → 400 `invalid_json` 信封。
6. 合法负载 → 200 + 非空 `id`。
7. 服务端 5xx → SDK 重试 ≥3 次（默认 `MaxRetries=3`）。
8. 服务端 429 → 响应携带 `Retry-After: <整数秒>`。
9. 成功响应字段 `id` / `status` 在所有 SDK 解析时一致。
10. `?api_key=` 查询参数被拒绝（防止凭据落入代理/access 日志）。
11. 空可选字段（`cc` / `bcc` / `tags` / `metadata` / `headers`）保持省略。
12. 默认不信任 `X-Forwarded-For`（防止 IP 伪造污染日志与指标）。
13. 同一 key 8 个并发请求不发生死锁或竞态。

## 9. 偏离项追踪

> 当前无偏离项。如发现新偏离请在本节登记并同步修复。