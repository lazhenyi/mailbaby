package metrics

import (
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"mailbaby/internal/config"
)

// Metrics manages the definition, registration, and recording of all system metrics.
type Metrics struct {
	cfg      config.MetricsConfig
	registry *prometheus.Registry
	statsd   *StatsDClient

	// Email Metrics
	emailsSentTotal         *prometheus.CounterVec
	emailSendDuration       *prometheus.HistogramVec
	emailRecipientsTotal    *prometheus.CounterVec
	emailPayloadBytesTotal  *prometheus.CounterVec
	smtpPoolActiveConns     *prometheus.GaugeVec
	smtpPoolIdleConns       *prometheus.GaugeVec

	// Queue Metrics
	queueReceivedTotal   *prometheus.CounterVec
	queueProcessedTotal  *prometheus.CounterVec
	queueRetriedTotal    *prometheus.CounterVec
	queueProcessDuration *prometheus.HistogramVec
	queueDepth           *prometheus.GaugeVec
	queueInFlight        *prometheus.GaugeVec

	// App Info
	appInfo *prometheus.GaugeVec
}

// NewMetrics initializes a new Metrics instance with Prometheus collectors and optional StatsD/PushGateway.
func NewMetrics(cfg config.MetricsConfig) (*Metrics, error) {
	cfg.ApplyDefaults()

	registry := prometheus.NewRegistry()

	// 1. Register Go runtime & process collectors if enabled
	if cfg.CollectRuntime {
		registry.MustRegister(
			collectors.NewGoCollector(),
			collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		)
	}

	buckets := cfg.HistogramBuckets
	if len(buckets) == 0 {
		buckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	}

	m := &Metrics{
		cfg:      cfg,
		registry: registry,

		// Emails
		emailsSentTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "mailbaby",
				Subsystem: "email",
				Name:      "sent_total",
				Help:      "Total count of email sending attempts partitioned by account and delivery status.",
			},
			[]string{"account", "status"},
		),
		emailSendDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "mailbaby",
				Subsystem: "email",
				Name:      "send_duration_seconds",
				Help:      "Histogram of email delivery durations in seconds partitioned by account.",
				Buckets:   buckets,
			},
			[]string{"account"},
		),
		emailRecipientsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "mailbaby",
				Subsystem: "email",
				Name:      "recipients_total",
				Help:      "Total count of recipients addressed partitioned by account and recipient type (to/cc/bcc).",
			},
			[]string{"account", "type"},
		),
		emailPayloadBytesTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "mailbaby",
				Subsystem: "email",
				Name:      "payload_bytes_total",
				Help:      "Total bytes of email messages delivered partitioned by account.",
			},
			[]string{"account"},
		),
		smtpPoolActiveConns: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "mailbaby",
				Subsystem: "smtp",
				Name:      "pool_active_connections",
				Help:      "Current active open connections in the SMTP connection pool.",
			},
			[]string{"account"},
		),
		smtpPoolIdleConns: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "mailbaby",
				Subsystem: "smtp",
				Name:      "pool_idle_connections",
				Help:      "Current idle available connections in the SMTP connection pool.",
			},
			[]string{"account"},
		),

		// Queue
		queueReceivedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "mailbaby",
				Subsystem: "queue",
				Name:      "messages_received_total",
				Help:      "Total count of messages received from queue partitioned by driver and topic.",
			},
			[]string{"driver", "topic"},
		),
		queueProcessedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "mailbaby",
				Subsystem: "queue",
				Name:      "messages_processed_total",
				Help:      "Total count of queue messages processed partitioned by driver, topic, and status.",
			},
			[]string{"driver", "topic", "status"},
		),
		queueRetriedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "mailbaby",
				Subsystem: "queue",
				Name:      "messages_retried_total",
				Help:      "Total count of queue messages retried due to failure.",
			},
			[]string{"driver", "topic"},
		),
		queueProcessDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "mailbaby",
				Subsystem: "queue",
				Name:      "process_duration_seconds",
				Help:      "Histogram of message processing latency in seconds partitioned by driver and topic.",
				Buckets:   buckets,
			},
			[]string{"driver", "topic"},
		),
		queueDepth: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "mailbaby",
				Subsystem: "queue",
				Name:      "depth",
				Help:      "Current estimated queue backlog/depth partitioned by driver and topic.",
			},
			[]string{"driver", "topic"},
		),
		queueInFlight: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "mailbaby",
				Subsystem: "queue",
				Name:      "in_flight",
				Help:      "Current number of messages in-flight/being processed concurrently.",
			},
			[]string{"driver", "topic"},
		),

		// App Info
		appInfo: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "mailbaby",
				Subsystem: "app",
				Name:      "info",
				Help:      "Constant informational gauge with build version and environment details.",
			},
			[]string{"name", "env", "version"},
		),
	}

	// Register collectors to Prometheus registry
	registry.MustRegister(
		m.emailsSentTotal,
		m.emailSendDuration,
		m.emailRecipientsTotal,
		m.emailPayloadBytesTotal,
		m.smtpPoolActiveConns,
		m.smtpPoolIdleConns,
		m.queueReceivedTotal,
		m.queueProcessedTotal,
		m.queueRetriedTotal,
		m.queueProcessDuration,
		m.queueDepth,
		m.queueInFlight,
		m.appInfo,
	)

	// Set default app info metric
	m.appInfo.WithLabelValues("mailbaby", "production", "1.0.0").Set(1)

	// Optional StatsD initialization
	if strings.EqualFold(string(cfg.Provider), string(config.MetricsProviderStatsD)) && cfg.StatsD.Address != "" {
		sd, err := NewStatsDClient(cfg.StatsD.Address, cfg.StatsD.Prefix, cfg.StatsD.FlushInterval)
		if err != nil {
			return nil, err
		}
		m.statsd = sd
	}

	return m, nil
}

// Registry returns the Prometheus Gatherer/Registerer registry.
func (m *Metrics) Registry() *prometheus.Registry {
	return m.registry
}

// IncEmailsSent records a sent email attempt.
func (m *Metrics) IncEmailsSent(account, status string) {
	if m == nil || m.emailsSentTotal == nil {
		return
	}
	if account == "" {
		account = "default"
	}
	m.emailsSentTotal.WithLabelValues(account, status).Inc()

	if m.statsd != nil {
		m.statsd.Count("email.sent."+account+"."+status, 1)
	}
}

// ObserveEmailDuration records the delivery duration of an email.
func (m *Metrics) ObserveEmailDuration(account string, d time.Duration) {
	if m == nil || m.emailSendDuration == nil {
		return
	}
	if account == "" {
		account = "default"
	}
	m.emailSendDuration.WithLabelValues(account).Observe(d.Seconds())

	if m.statsd != nil {
		m.statsd.Timing("email.duration."+account, d)
	}
}

// AddEmailRecipients records the count of recipients for an email.
func (m *Metrics) AddEmailRecipients(account, rcptType string, count int) {
	if m == nil || m.emailRecipientsTotal == nil || count <= 0 {
		return
	}
	if account == "" {
		account = "default"
	}
	m.emailRecipientsTotal.WithLabelValues(account, rcptType).Add(float64(count))

	if m.statsd != nil {
		m.statsd.Count("email.recipients."+account+"."+rcptType, int64(count))
	}
}

// AddEmailBytes records the byte size of an email payload.
func (m *Metrics) AddEmailBytes(account string, bytes int64) {
	if m == nil || m.emailPayloadBytesTotal == nil || bytes <= 0 {
		return
	}
	if account == "" {
		account = "default"
	}
	m.emailPayloadBytesTotal.WithLabelValues(account).Add(float64(bytes))

	if m.statsd != nil {
		m.statsd.Count("email.bytes."+account, bytes)
	}
}

// SetSmtpPoolStats records the active and idle connections in an SMTP pool.
func (m *Metrics) SetSmtpPoolStats(account string, active int64, idle int) {
	if m == nil {
		return
	}
	if account == "" {
		account = "default"
	}
	if m.smtpPoolActiveConns != nil {
		m.smtpPoolActiveConns.WithLabelValues(account).Set(float64(active))
	}
	if m.smtpPoolIdleConns != nil {
		m.smtpPoolIdleConns.WithLabelValues(account).Set(float64(idle))
	}

	if m.statsd != nil {
		m.statsd.Gauge("smtp.pool.active."+account, float64(active))
		m.statsd.Gauge("smtp.pool.idle."+account, float64(idle))
	}
}

// IncQueueReceived increments the counter when a message is received from a queue.
func (m *Metrics) IncQueueReceived(driver, topic string) {
	if m == nil || m.queueReceivedTotal == nil {
		return
	}
	m.queueReceivedTotal.WithLabelValues(driver, topic).Inc()

	if m.statsd != nil {
		m.statsd.Count("queue.received."+driver+"."+topic, 1)
	}
}

// IncQueueProcessed increments the counter when a message processing finishes.
func (m *Metrics) IncQueueProcessed(driver, topic, status string) {
	if m == nil || m.queueProcessedTotal == nil {
		return
	}
	m.queueProcessedTotal.WithLabelValues(driver, topic, status).Inc()

	if m.statsd != nil {
		m.statsd.Count("queue.processed."+driver+"."+topic+"."+status, 1)
	}
}

// IncQueueRetried increments the counter when a message is retried.
func (m *Metrics) IncQueueRetried(driver, topic string) {
	if m == nil || m.queueRetriedTotal == nil {
		return
	}
	m.queueRetriedTotal.WithLabelValues(driver, topic).Inc()

	if m.statsd != nil {
		m.statsd.Count("queue.retried."+driver+"."+topic, 1)
	}
}

// ObserveQueueProcessDuration records the duration taken to process a queue message.
func (m *Metrics) ObserveQueueProcessDuration(driver, topic string, d time.Duration) {
	if m == nil || m.queueProcessDuration == nil {
		return
	}
	m.queueProcessDuration.WithLabelValues(driver, topic).Observe(d.Seconds())

	if m.statsd != nil {
		m.statsd.Timing("queue.duration."+driver+"."+topic, d)
	}
}

// SetQueueDepth records current queue depth.
func (m *Metrics) SetQueueDepth(driver, topic string, depth float64) {
	if m == nil || m.queueDepth == nil {
		return
	}
	m.queueDepth.WithLabelValues(driver, topic).Set(depth)

	if m.statsd != nil {
		m.statsd.Gauge("queue.depth."+driver+"."+topic, depth)
	}
}

// SetQueueInFlight records current number of concurrent workers processing queue messages.
func (m *Metrics) SetQueueInFlight(driver, topic string, inFlight float64) {
	if m == nil || m.queueInFlight == nil {
		return
	}
	m.queueInFlight.WithLabelValues(driver, topic).Set(inFlight)

	if m.statsd != nil {
		m.statsd.Gauge("queue.in_flight."+driver+"."+topic, inFlight)
	}
}

// Close closes optional network clients such as StatsD.
func (m *Metrics) Close() error {
	if m == nil {
		return nil
	}
	if m.statsd != nil {
		return m.statsd.Close()
	}
	return nil
}
