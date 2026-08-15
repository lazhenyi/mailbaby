# MailBaby Helm Chart

Production-ready Kubernetes Helm Chart for **MailBaby** — high-performance, multi-protocol (Message Queue / HTTP / gRPC) email sending microservice written in Go.

---

## Features

- **Multi-Protocol Delivery**:
  - Message Queue consumer daemon (RabbitMQ, Kafka, Redis, RocketMQ, NATS, Pulsar, SQS, Memory).
  - HTTP REST API (`/v1/email/send`, `/v1/email/batch`).
  - gRPC RPC Service (`mailbaby.v1.MailService`).
- **Security & Hardening**:
  - Non-root user execution (`uid: 10001`, `gid: 10001`).
  - Read-only root filesystem (`readOnlyRootFilesystem: true`).
  - Dropped all Linux capabilities.
  - Link Secret Key / API Token authentication for HTTP & gRPC endpoints.
  - Kubernetes Secret management for sensitive credentials.
- **High Availability & Observability**:
  - Rolling update strategy with zero downtime (`maxUnavailable: 0`).
  - Pod anti-affinity for multi-node distribution.
  - Kubernetes Native Liveness (`/livez`), Readiness (`/readyz`), and Startup probes.
  - Built-in Prometheus Metrics exporter (`/metrics`) and optional `ServiceMonitor` for Prometheus Operator.
  - Horizontal Pod Autoscaler (HPA) and Pod Disruption Budget (PDB) support.
  - Automatic rolling restart when ConfigMap or Secret changes via SHA256 checksum annotations.

---

## Quick Start

### 1. Install the Chart

```bash
# Add chart and install with default settings
helm install mailbaby ./charts/mailbaby -n mailbaby --create-namespace
```

### 2. Install with Custom Configuration

Create a custom `my-values.yaml`:

```yaml
replicaCount: 3

image:
  repository: your-registry.com/mailbaby
  tag: "1.0.0"

secret:
  authSecretKey: "your_strong_secret_key"
  smtpPasswords:
    default: "your_smtp_app_password"

config:
  auth:
    enabled: true
    header_name: "X-API-Key"

  smtp:
    default:
      host: "smtp.office365.com"
      port: 587
      username: "noreply@company.com"
      from: "noreply@company.com"
      from_name: "Company Notification Service"

  queue:
    driver: "rabbitmq"
    concurrency: 20
    rabbitmq:
      host: "rabbitmq.default.svc"
      port: 5672
      username: "guest"
      password: "guest_password"
      queue: "email_dispatch_queue"
```

Apply the deployment:

```bash
helm install mailbaby ./charts/mailbaby -f my-values.yaml -n mailbaby --create-namespace
```

---

## Configuration Parameters

### Core Deployment Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of pod replicas | `2` |
| `image.repository` | Container image repository | `mailbaby/mailbaby` |
| `image.tag` | Container image tag | `"latest"` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `strategy` | Deployment rollout strategy | `RollingUpdate` |
| `serviceAccount.create` | Whether to create ServiceAccount | `true` |
| `podSecurityContext` | Pod-level security context (non-root) | `fsGroup: 10001` |
| `securityContext` | Container security context | `readOnlyRootFilesystem: true` |

### Service & Ingress Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `service.type` | Kubernetes Service type | `ClusterIP` |
| `service.httpPort` | HTTP Service listening port | `8080` |
| `service.grpcPort` | gRPC Service listening port | `8081` |
| `ingress.enabled` | Enable HTTP Ingress resource | `false` |
| `grpcIngress.enabled` | Enable dedicated gRPC Ingress | `false` |

### Probes & Autoscaling

| Parameter | Description | Default |
|-----------|-------------|---------|
| `probes.liveness.enabled` | Enable Kubernetes Liveness probe | `true` (`/livez`) |
| `probes.readiness.enabled` | Enable Kubernetes Readiness probe | `true` (`/readyz`) |
| `probes.startup.enabled` | Enable Kubernetes Startup probe | `true` (`/livez`) |
| `autoscaling.enabled` | Enable HorizontalPodAutoscaler | `false` |
| `podDisruptionBudget.enabled` | Enable PodDisruptionBudget | `false` |
| `serviceMonitor.enabled` | Enable Prometheus Operator ServiceMonitor | `false` |

---

## Verifying Deployment

```bash
# Forward HTTP API
kubectl port-forward svc/mailbaby 8080:8080 -n mailbaby

# Check health
curl http://127.0.0.1:8080/readyz

# Send test email
curl -X POST http://127.0.0.1:8080/v1/email/send \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your_strong_secret_key" \
  -d '{"to":["user@example.com"],"subject":"Hello","text_body":"Test"}'
```
