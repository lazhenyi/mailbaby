package sender

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/smtp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mailbaby/internal/config"
)

func TestEmailBuilderAndValidation(t *testing.T) {
	e := NewEmail().
		SetAccount("marketing").
		SetFrom("sender@example.com", "Marketing Team").
		SetReplyTo("reply@example.com").
		AddTo("user1@example.com", "User Two <user2@example.com>").
		AddCc("manager@example.com").
		AddBcc("audit@example.com").
		SetSubject("Hello World").
		SetTextBody("Plain text body").
		SetHTMLBody("<p>HTML body</p>").
		SetHeader("X-Campaign-ID", "12345").
		Attach("doc.pdf", []byte("PDF CONTENT"), "application/pdf").
		AttachInline("logo.png", "logo-img", []byte("PNG CONTENT"), "image/png")

	if err := e.Validate(); err != nil {
		t.Fatalf("expected valid email, got: %v", err)
	}

	if e.Account != "marketing" {
		t.Errorf("expected account 'marketing', got %q", e.Account)
	}

	recipients := e.AllRecipients()
	if len(recipients) != 4 {
		t.Errorf("expected 4 consolidated recipients, got %d: %v", len(recipients), recipients)
	}

	// Test JSON serialization
	jsonData, err := e.ToJSON()
	if err != nil {
		t.Fatalf("failed to serialize email to JSON: %v", err)
	}

	var decoded Email
	if err := decoded.FromJSON(jsonData); err != nil {
		t.Fatalf("failed to deserialize email from JSON: %v", err)
	}

	if decoded.Subject != "Hello World" {
		t.Errorf("expected decoded subject 'Hello World', got %q", decoded.Subject)
	}
	if len(decoded.Attachments) != 2 {
		t.Errorf("expected 2 attachments, got %d", len(decoded.Attachments))
	}
}

func TestEmailValidationErrors(t *testing.T) {
	var nilEmail *Email
	if err := nilEmail.Validate(); !errors.Is(err, ErrNilEmail) {
		t.Errorf("expected ErrNilEmail, got %v", err)
	}

	noRcpt := NewEmail().SetFrom("test@example.com").SetSubject("No recipients")
	if err := noRcpt.Validate(); !errors.Is(err, ErrNoRecipients) {
		t.Errorf("expected ErrNoRecipients, got %v", err)
	}

	invalidFrom := NewEmail().SetFrom("invalid-from").AddTo("user@example.com")
	if err := invalidFrom.Validate(); !errors.Is(err, ErrInvalidFrom) {
		t.Errorf("expected ErrInvalidFrom, got %v", err)
	}

	invalidTo := NewEmail().SetFrom("test@example.com").AddTo("bad-email-address")
	if err := invalidTo.Validate(); !errors.Is(err, ErrInvalidRecipient) {
		t.Errorf("expected ErrInvalidRecipient, got %v", err)
	}
}

func TestMIMEBuilding(t *testing.T) {
	t.Run("plain text email", func(t *testing.T) {
		e := NewEmail().
			SetFrom("sender@example.com").
			AddTo("user@example.com").
			SetSubject("Simple Text").
			SetTextBody("Hello, this is plain text.")

		raw, err := BuildMIME(e, "fallback@example.com", "")
		if err != nil {
			t.Fatalf("BuildMIME failed: %v", err)
		}

		rawStr := string(raw)
		if !strings.Contains(rawStr, "From: sender@example.com") {
			t.Errorf("expected From header, got: %s", rawStr)
		}
		if !strings.Contains(rawStr, "To: user@example.com") {
			t.Errorf("expected To header, got: %s", rawStr)
		}
		if !strings.Contains(rawStr, "Content-Type: text/plain; charset=UTF-8") {
			t.Errorf("expected plain text content type, got: %s", rawStr)
		}
	})

	t.Run("multipart alternative with UTF-8 subject", func(t *testing.T) {
		e := NewEmail().
			SetFrom("admin@example.com", "System Administrator").
			AddTo("user@example.com").
			SetSubject("Welcome to MailBaby — Café & Services").
			SetTextBody("Welcome to MailBaby plain text body.").
			SetHTMLBody("<h1>Welcome to MailBaby HTML body</h1>").
			SetHeader("X-Mailer-Tag", "reg-service")

		raw, err := BuildMIME(e, "", "")
		if err != nil {
			t.Fatalf("BuildMIME failed: %v", err)
		}

		rawStr := string(raw)
		if !strings.Contains(rawStr, "multipart/alternative") {
			t.Errorf("expected multipart/alternative, got: %s", rawStr)
		}
		if !strings.Contains(rawStr, "X-Mailer-Tag: reg-service") {
			t.Errorf("expected custom header, got: %s", rawStr)
		}
		if !strings.Contains(strings.ToUpper(rawStr), "=?UTF-8?B?") {
			t.Errorf("expected RFC 2047 encoded subject, got: %s", rawStr)
		}
	})

	t.Run("multipart mixed with attachments and inline cid", func(t *testing.T) {
		e := NewEmail().
			SetFrom("info@example.com").
			AddTo("user@example.com").
			SetSubject("Invoice").
			SetHTMLBody(`<p>Hello <img src="cid:logo123"></p>`).
			Attach("invoice.pdf", []byte("%PDF-1.4 file content"), "application/pdf").
			AttachInline("logo.png", "logo123", []byte("FAKE PNG BYTES"), "image/png")

		raw, err := BuildMIME(e, "", "")
		if err != nil {
			t.Fatalf("BuildMIME failed: %v", err)
		}

		rawStr := string(raw)
		if !strings.Contains(rawStr, "multipart/mixed") {
			t.Errorf("expected multipart/mixed, got: %s", rawStr)
		}
		if !strings.Contains(rawStr, "multipart/related") {
			t.Errorf("expected nested multipart/related, got: %s", rawStr)
		}
		if !strings.Contains(strings.ToLower(rawStr), "content-id: <logo123>") {
			t.Errorf("expected inline Content-ID header, got: %s", rawStr)
		}
		if !strings.Contains(rawStr, `filename="invoice.pdf"`) {
			t.Errorf("expected invoice.pdf attachment, got: %s", rawStr)
		}
	})
}

func TestAuthBuilderAndLoginAuth(t *testing.T) {
	// 1. Plain Auth
	plain := BuildAuth(config.SmtpAuthTypePlain, "user", "pass", "smtp.example.com")
	if plain == nil {
		t.Error("expected plain auth not nil")
	}

	// 2. Login Auth
	login := BuildAuth(config.SmtpAuthTypeLogin, "user@qq.com", "authcode", "smtp.qq.com")
	if login == nil {
		t.Fatal("expected login auth not nil")
	}

	// Test LoginAuth state machine
	serverInfo := &smtp.ServerInfo{Name: "smtp.qq.com", TLS: true}
	mech, resp, err := login.Start(serverInfo)
	if err != nil || mech != "LOGIN" || string(resp) != "user@qq.com" {
		t.Fatalf("login.Start failed: mech=%s, resp=%s, err=%v", mech, string(resp), err)
	}

	// Server asks for password
	nextResp, err := login.Next([]byte("Password:"), true)
	if err != nil || string(nextResp) != "authcode" {
		t.Fatalf("login.Next failed: resp=%s, err=%v", string(nextResp), err)
	}

	// 3. None Auth
	none := BuildAuth(config.SmtpAuthTypeNone, "user", "pass", "smtp.example.com")
	if none != nil {
		t.Error("expected nil for None auth")
	}
}

func TestMIMECustomHeaderInjectionRejected(t *testing.T) {
	base := NewEmail().SetFrom("sender@example.com").AddTo("user@example.com").SetSubject("OK")
	base.Headers["X-Evil\r\nBcc: victim@example.com"] = "v"

	if _, err := BuildMIME(base, "fallback@example.com", ""); err == nil {
		t.Fatal("expected BuildMIME to reject a header name containing CRLF")
	}

	base2 := NewEmail().SetFrom("sender@example.com").AddTo("user@example.com").SetSubject("OK")
	base2.Headers["X-Bad:Key"] = "v"
	if _, err := BuildMIME(base2, "fallback@example.com", ""); err == nil {
		t.Fatal("expected BuildMIME to reject a header name containing ':'")
	}
}

func TestMIMECustomHeaderValueSanitized(t *testing.T) {
	e := NewEmail().SetFrom("sender@example.com").AddTo("user@example.com").SetSubject("OK")
	e.SetHeader("X-Note", "line1\r\nBcc: victim@example.com")

	raw, err := BuildMIME(e, "fallback@example.com", "")
	if err != nil {
		t.Fatalf("BuildMIME failed: %v", err)
	}
	if bytes.Contains(raw, []byte("\r\nBcc:")) {
		t.Errorf("CRLF-injected header leaked into message:\n%s", raw)
	}
}

func TestPoolAcquireAfterCloseReturnsError(t *testing.T) {
	pool := NewSmtpConnPool(config.SmtpAccountConfig{})
	_ = pool.Close()

	if _, err := pool.Acquire(context.Background()); !errors.Is(err, ErrPoolClosed) {
		t.Errorf("expected ErrPoolClosed, got %v", err)
	}
}

func TestPoolAcquireConcurrentCloseNoPanic(t *testing.T) {
	pool := NewSmtpConnPool(config.SmtpAccountConfig{
		Host: "127.0.0.1", Port: 1,
		ConnectTimeout: 50 * time.Millisecond,
		Pool: config.SmtpPoolConfig{
			MaxIdleConns: 2,
			MaxOpenConns: 2,
		},
	})

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = pool.Acquire(context.Background())
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = pool.Close()
	}()
	wg.Wait()
	// Test passes if no panic occurred (nil-client deref / double close).
}

func TestDialExplicitSTARTTLSRejectedWhenUnsupported(t *testing.T) {
	mockServer := newMockSMTPServer(t)
	defer mockServer.close()

	host, portStr, _ := net.SplitHostPort(mockServer.addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	cfg := config.SmtpAccountConfig{
		Host:       host,
		Port:       port,
		Encryption: config.SmtpEncryptionSTARTTLS,
	}

	_, err := Dial(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected Dial to fail when explicit STARTTLS is not supported by the server")
	}
	if !strings.Contains(err.Error(), "STARTTLS") {
		t.Errorf("expected a STARTTLS-related error, got %v", err)
	}
}

func TestDialAutoFallsBackWithoutStarttls(t *testing.T) {
	mockServer := newMockSMTPServer(t)
	defer mockServer.close()

	host, portStr, _ := net.SplitHostPort(mockServer.addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	cfg := config.SmtpAccountConfig{
		Host:       host,
		Port:       port,
		Encryption: config.SmtpEncryptionAuto, // non-465 port infers STARTTLS, server lacks it
	}

	client, err := Dial(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Auto mode should keep working without STARTTLS, got: %v", err)
	}
	_ = client.Close()
}

// mockSMTPServer implements a lightweight mock SMTP server for integration tests.
type mockSMTPServer struct {
	listener     net.Listener
	addr         string
	receivedMsgs [][]byte
	mu           sync.Mutex
	authCount    int32
	mailFroms    []string
	rcptTos      []string
	quitChan     chan struct{}
}

func newMockSMTPServer(t *testing.T) *mockSMTPServer {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start mock smtp server: %v", err)
	}

	s := &mockSMTPServer{
		listener: ln,
		addr:     ln.Addr().String(),
		quitChan: make(chan struct{}),
	}

	go s.serve()
	return s
}

func (s *mockSMTPServer) serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quitChan:
				return
			default:
				return
			}
		}
		go s.handleConn(conn)
	}
}

func (s *mockSMTPServer) handleConn(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	// Send 220 banner
	_, _ = writer.WriteString("220 mailbaby.mock SMTP Service Ready\r\n")
	_ = writer.Flush()

	var inData bool
	var dataBuffer strings.Builder

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		if inData {
			if line == ".\r\n" || line == ".\n" {
				inData = false
				s.mu.Lock()
				s.receivedMsgs = append(s.receivedMsgs, []byte(dataBuffer.String()))
				s.mu.Unlock()
				dataBuffer.Reset()
				_, _ = writer.WriteString("250 2.0.0 OK message queued\r\n")
				_ = writer.Flush()
			} else {
				dataBuffer.WriteString(line)
			}
			continue
		}

		cmd := strings.ToUpper(strings.TrimSpace(line))
		switch {
		case strings.HasPrefix(cmd, "EHLO") || strings.HasPrefix(cmd, "HELO"):
			_, _ = writer.WriteString("250-mailbaby.mock Hello\r\n250-AUTH LOGIN PLAIN\r\n250 8BITMIME\r\n")
			_ = writer.Flush()

		case strings.HasPrefix(cmd, "AUTH PLAIN"):
			atomic.AddInt32(&s.authCount, 1)
			_, _ = writer.WriteString("235 2.7.0 Authentication successful\r\n")
			_ = writer.Flush()

		case strings.HasPrefix(cmd, "AUTH LOGIN"):
			atomic.AddInt32(&s.authCount, 1)
			// Server sends Username prompt
			_, _ = writer.WriteString("334 " + base64.StdEncoding.EncodeToString([]byte("Username:")) + "\r\n")
			_ = writer.Flush()

			// Read username response
			_, _ = reader.ReadString('\n')

			// Server sends Password prompt
			_, _ = writer.WriteString("334 " + base64.StdEncoding.EncodeToString([]byte("Password:")) + "\r\n")
			_ = writer.Flush()

			// Read password response
			_, _ = reader.ReadString('\n')

			_, _ = writer.WriteString("235 2.7.0 Authentication successful\r\n")
			_ = writer.Flush()

		case strings.HasPrefix(cmd, "MAIL FROM:"):
			s.mu.Lock()
			s.mailFroms = append(s.mailFroms, line)
			s.mu.Unlock()
			_, _ = writer.WriteString("250 2.1.0 Sender OK\r\n")
			_ = writer.Flush()

		case strings.HasPrefix(cmd, "RCPT TO:"):
			s.mu.Lock()
			s.rcptTos = append(s.rcptTos, line)
			s.mu.Unlock()
			_, _ = writer.WriteString("250 2.1.5 Recipient OK\r\n")
			_ = writer.Flush()

		case strings.HasPrefix(cmd, "DATA"):
			inData = true
			_, _ = writer.WriteString("354 Start mail input; end with <CRLF>.<CRLF>\r\n")
			_ = writer.Flush()

		case strings.HasPrefix(cmd, "RSET"):
			_, _ = writer.WriteString("250 2.0.0 Reset OK\r\n")
			_ = writer.Flush()

		case strings.HasPrefix(cmd, "NOOP"):
			_, _ = writer.WriteString("250 2.0.0 OK\r\n")
			_ = writer.Flush()

		case strings.HasPrefix(cmd, "QUIT"):
			_, _ = writer.WriteString("221 2.0.0 Bye\r\n")
			_ = writer.Flush()
			return

		default:
			_, _ = writer.WriteString("500 5.5.1 Command unrecognized\r\n")
			_ = writer.Flush()
		}
	}
}

func (s *mockSMTPServer) close() {
	close(s.quitChan)
	_ = s.listener.Close()
}

func TestSenderIntegrationWithMockServer(t *testing.T) {
	mockServer := newMockSMTPServer(t)
	defer mockServer.close()

	host, portStr, _ := net.SplitHostPort(mockServer.addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	// Configure multiple SMTP accounts pointing to mock server
	smtpCfg := config.SmtpConfig{
		"default": config.SmtpAccountConfig{
			Host:       host,
			Port:       port,
			Username:   "default_user",
			Password:   "default_pass",
			From:       "default@mailbaby.io",
			FromName:   "Default Sender",
			Encryption: config.SmtpEncryptionNone,
			AuthType:   config.SmtpAuthTypePlain,
			RateLimit: config.SmtpRateLimitConfig{
				EmailsPerSecond:       100,
				MaxRecipientsPerEmail: 5,
			},
		},
		"marketing": config.SmtpAccountConfig{
			Host:       host,
			Port:       port,
			Username:   "marketing_user",
			Password:   "marketing_pass",
			From:       "marketing@mailbaby.io",
			FromName:   "Marketing Team",
			Encryption: config.SmtpEncryptionNone,
			AuthType:   config.SmtpAuthTypeLogin,
			RateLimit: config.SmtpRateLimitConfig{
				EmailsPerSecond:       50,
				MaxRecipientsPerEmail: 5,
			},
		},
	}

	mailSender, err := New(smtpCfg)
	if err != nil {
		t.Fatalf("failed to create Sender: %v", err)
	}
	defer mailSender.Close()

	ctx := context.Background()

	// 1. Send via default account (unspecified email.Account)
	emailDefault := NewEmail().
		AddTo("user1@example.com").
		SetSubject("Hello Default").
		SetTextBody("Testing default account sending.")

	if err := mailSender.Send(ctx, emailDefault); err != nil {
		t.Fatalf("Send via default account failed: %v", err)
	}

	// 2. Send via marketing account
	emailMarketing := NewEmail().
		SetAccount("marketing").
		AddTo("customer@example.com").
		SetSubject("Special Offer").
		SetHTMLBody("<h1>Big Sale!</h1>")

	if err := mailSender.Send(ctx, emailMarketing); err != nil {
		t.Fatalf("Send via marketing account failed: %v", err)
	}

	// 3. Send to non-existent account
	emailInvalidAcc := NewEmail().
		SetAccount("non_existent").
		AddTo("user@example.com").
		SetSubject("Test")

	err = mailSender.Send(ctx, emailInvalidAcc)
	if !errors.Is(err, ErrAccountNotFound) {
		t.Errorf("expected ErrAccountNotFound, got: %v", err)
	}

	// 4. Exceed MaxRecipientsPerEmail
	emailTooManyRcpt := NewEmail().
		AddTo("u1@a.com", "u2@a.com", "u3@a.com", "u4@a.com", "u5@a.com", "u6@a.com"). // 6 > limit 5
		SetSubject("Too many")

	err = mailSender.Send(ctx, emailTooManyRcpt)
	if !errors.Is(err, ErrMaxRecipientsExceeded) {
		t.Errorf("expected ErrMaxRecipientsExceeded, got: %v", err)
	}

	// 5. SendBatch
	batch := []*Email{
		NewEmail().AddTo("b1@example.com").SetSubject("Batch 1").SetTextBody("body 1"),
		NewEmail().AddTo("b2@example.com").SetSubject("Batch 2").SetTextBody("body 2"),
		NewEmail().AddTo("b3@example.com").SetSubject("Batch 3").SetTextBody("body 3"),
	}
	errs := mailSender.SendBatch(ctx, batch)
	for i, sendErr := range errs {
		if sendErr != nil {
			t.Errorf("batch item %d failed: %v", i, sendErr)
		}
	}

	// 6. Verify stats
	stats := mailSender.Stats()
	if defaultStats, ok := stats["default"]; ok {
		if defaultStats.TotalSent < 4 { // 1 single + 3 batch = 4
			t.Errorf("expected at least 4 total sent on default, got %d", defaultStats.TotalSent)
		}
	} else {
		t.Error("missing default account in stats")
	}

	// 7. Verify mock server received messages
	mockServer.mu.Lock()
	receivedCount := len(mockServer.receivedMsgs)
	mockServer.mu.Unlock()

	if receivedCount < 5 { // 1 default + 1 marketing + 3 batch = 5
		t.Errorf("expected at least 5 received messages on mock server, got %d", receivedCount)
	}
}
