package metrics

import (
	"net"
	"strings"
	"testing"
	"time"

	"mailbaby/internal/config"
)

func TestMetricsRecording(t *testing.T) {
	cfg := config.MetricsConfig{
		Enabled:        true,
		Provider:       config.MetricsProviderPrometheus,
		CollectRuntime: false,
	}

	m, err := NewMetrics(cfg)
	if err != nil {
		t.Fatalf("failed to create metrics: %v", err)
	}
	defer m.Close()

	// 1. Record Email metrics
	m.IncEmailsSent("default", "success")
	m.IncEmailsSent("default", "failed")
	m.IncEmailsSent("marketing", "success")
	m.ObserveEmailDuration("default", 150*time.Millisecond)
	m.AddEmailRecipients("default", "to", 3)
	m.AddEmailBytes("default", 4096)
	m.SetSmtpPoolStats("default", 2, 5)
	m.ObserveSmtpPoolWait("default", 10*time.Millisecond)
	m.IncSmtpPoolExhausted("default")
	m.ObserveSmtpDial("default", 20*time.Millisecond)
	m.ObserveSmtpTLSHandshake("default", 30*time.Millisecond)
	m.ObserveSmtpAuth("default", 15*time.Millisecond)

	// 2. Record Queue metrics
	m.IncQueueReceived("memory", "email_queue")
	m.IncQueueProcessed("memory", "email_queue", "success")
	m.IncQueueRetried("memory", "email_queue")
	m.IncQueueDeadLetter("memory", "email_queue")
	m.ObserveQueueProcessDuration("memory", "email_queue", 50*time.Millisecond)
	m.ObserveQueuePublish("memory", "email_queue", "success", 5*time.Millisecond)
	m.ObserveQueueLag("memory", "email_queue", 100*time.Millisecond)
	m.SetQueueDepth("memory", "email_queue", 10)
	m.SetQueueInFlight("memory", "email_queue", 2)

	// 3. Record HTTP metrics
	m.ObserveHTTPRequest("/metrics", "GET", 200, 2*time.Millisecond)
	m.UpdateAppUptime()

	// 4. Gather metric families
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	foundMap := make(map[string]bool)
	for _, fam := range families {
		foundMap[fam.GetName()] = true
	}

	expectedMetrics := []string{
		"mailbaby_email_sent_total",
		"mailbaby_email_send_duration_seconds",
		"mailbaby_email_recipients_total",
		"mailbaby_email_payload_bytes_total",
		"mailbaby_smtp_pool_active_connections",
		"mailbaby_smtp_pool_idle_connections",
		"mailbaby_smtp_pool_wait_duration_seconds",
		"mailbaby_smtp_pool_exhausted_total",
		"mailbaby_smtp_dial_duration_seconds",
		"mailbaby_smtp_tls_handshake_duration_seconds",
		"mailbaby_smtp_auth_duration_seconds",
		"mailbaby_queue_messages_received_total",
		"mailbaby_queue_messages_processed_total",
		"mailbaby_queue_messages_retried_total",
		"mailbaby_queue_messages_deadletter_total",
		"mailbaby_queue_process_duration_seconds",
		"mailbaby_queue_publish_duration_seconds",
		"mailbaby_queue_publish_total",
		"mailbaby_queue_lag_seconds",
		"mailbaby_queue_depth",
		"mailbaby_queue_in_flight",
		"mailbaby_http_requests_total",
		"mailbaby_http_request_duration_seconds",
		"mailbaby_app_info",
		"mailbaby_app_uptime_seconds",
	}

	for _, name := range expectedMetrics {
		if !foundMap[name] {
			t.Errorf("expected metric family %q to be registered in Prometheus", name)
		}
	}
}

func TestStatsDClient(t *testing.T) {
	// Setup UDP mock listener
	udpLn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create UDP listener: %v", err)
	}
	defer udpLn.Close()

	udpAddr := udpLn.LocalAddr().String()

	client, err := NewStatsDClient(udpAddr, "test.", 20*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to create StatsD client: %v", err)
	}

	client.Count("email.sent", 5)
	client.Gauge("queue.depth", 42.5)
	client.Timing("email.duration", 150*time.Millisecond)
	client.Flush()
	_ = client.Close()

	buf := make([]byte, 1024)
	_ = udpLn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	n, _, err := udpLn.ReadFrom(buf)
	if err != nil {
		t.Fatalf("failed to receive UDP packets: %v", err)
	}

	data := string(buf[:n])
	if !strings.Contains(data, "test.email.sent:5|c") {
		t.Errorf("expected counter line, got:\n%s", data)
	}
	if !strings.Contains(data, "test.queue.depth:42.5") {
		t.Errorf("expected gauge line, got:\n%s", data)
	}
	if !strings.Contains(data, "test.email.duration:150.00|ms") {
		t.Errorf("expected timing line, got:\n%s", data)
	}
}

func TestGlobalInitAndNoop(t *testing.T) {
	// Disabled metrics should return noop
	disabledCfg := config.MetricsConfig{Enabled: false}
	m, err := Init(disabledCfg)
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	if m == nil {
		t.Fatal("expected non-nil noop metrics instance")
	}

	globalM := Get()
	if globalM == nil {
		t.Fatal("expected Get() to return non-nil noop instance")
	}

	// Calling methods on noop must not panic
	globalM.IncEmailsSent("default", "success")
	globalM.IncQueueReceived("memory", "topic")
	globalM.SetSmtpPoolStats("default", 1, 2)
	globalM.ObserveSmtpDial("default", 10*time.Millisecond)
	globalM.ObserveHTTPRequest("/metrics", "GET", 200, time.Millisecond)

	_ = Close()
}
