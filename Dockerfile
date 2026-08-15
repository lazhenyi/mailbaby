# ==============================================================================
# Build Stage: Compile statically-linked Go binary
# ==============================================================================
FROM golang:alpine AS builder

# Install build prerequisites and system certificates
RUN apk --no-cache add ca-certificates git tzdata

WORKDIR /src

# Leverage Docker layer caching for Go modules
COPY go.mod go.sum ./
RUN go mod download

# Copy application source code
COPY . .

# Build-time injection arguments
ARG VERSION="1.0.0"
ARG COMMIT="dev"
ARG BUILD_DATE="unknown"

# Compile static, stripped binary
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w \
      -X mailbaby/internal/cmd.Version=${VERSION} \
      -X mailbaby/internal/cmd.Commit=${COMMIT} \
      -X mailbaby/internal/cmd.BuildDate=${BUILD_DATE}" \
    -o /bin/mailbaby .

# ==============================================================================
# Final Stage: Minimal, secure runtime container
# ==============================================================================
FROM alpine:3.20 AS final

# Install runtime SSL certificates and timezone data
RUN apk --no-cache add ca-certificates tzdata

# Create dedicated non-root user and group
RUN addgroup -g 10001 -S appgroup && \
    adduser -u 10001 -S appuser -G appgroup

WORKDIR /app

# Copy binary from builder stage
COPY --from=builder /bin/mailbaby /app/mailbaby

# Copy configuration template only (actual config.yaml is mounted at runtime)
COPY config.yaml.example /app/config.yaml.example

# Create directories and set proper ownership
RUN mkdir -p /app/logs && chown -R appuser:appgroup /app

# Switch to non-root user
USER appuser:appgroup

# Expose unified HTTP server port (Metrics / Health / Pprof)
EXPOSE 8080

# Kubernetes / Docker native health check probe
HEALTHCHECK --interval=15s --timeout=5s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/livez || exit 1

# Default execution entrypoint and arguments
ENTRYPOINT ["/app/mailbaby"]
CMD ["server", "-c", "/app/config.yaml"]
