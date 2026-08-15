package sender

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/smtp"
	"os"
	"strings"
	"time"

	"mailbaby/internal/config"
	"mailbaby/internal/logger"
	"mailbaby/internal/metrics"
	"mailbaby/internal/tracing"
)

// SmtpClient wraps a single connection to an SMTP server.
type SmtpClient struct {
	client   *smtp.Client
	conn     net.Conn
	cfg      config.SmtpAccountConfig
	lastUsed time.Time
}

// Dial creates and authenticates a new SmtpClient connected to the server specified in SmtpAccountConfig.
func Dial(ctx context.Context, cfg config.SmtpAccountConfig) (*SmtpClient, error) {
	ctx, span := tracing.StartSpan(ctx, "smtp.dial")
	defer span.End()

	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	span.SetAttribute("smtp.host", cfg.Host)
	span.SetAttribute("smtp.port", cfg.Port)
	span.SetAttribute("smtp.encryption", string(cfg.Encryption))

	tlsConfig := &tls.Config{
		ServerName:         cfg.Host,
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	}

	heloHost := cfg.HeloHostname
	if heloHost == "" {
		if h, err := os.Hostname(); err == nil && h != "" {
			heloHost = h
		} else {
			heloHost = "localhost"
		}
	}

	connectTimeout := cfg.ConnectTimeout
	if connectTimeout <= 0 {
		connectTimeout = 10 * time.Second
	}

	dialer := &net.Dialer{
		Timeout: connectTimeout,
	}

	encryption := cfg.Encryption
	if encryption == "" || strings.EqualFold(string(encryption), string(config.SmtpEncryptionAuto)) {
		if cfg.Port == 465 {
			encryption = config.SmtpEncryptionSSL
		} else {
			encryption = config.SmtpEncryptionSTARTTLS
		}
	}

	var rawConn net.Conn
	var client *smtp.Client
	var err error

	dialStart := time.Now()

	// Connect based on encryption type
	switch strings.ToUpper(string(encryption)) {
	case "SSL", "TLS":
		tlsConn, dialErr := tls.DialWithDialer(dialer, "tcp", addr, tlsConfig)
		if dialErr != nil {
			span.RecordError(dialErr)
			return nil, fmt.Errorf("sender: failed to dial TLS %s: %w", addr, dialErr)
		}
		rawConn = tlsConn
		client, err = smtp.NewClient(tlsConn, cfg.Host)
		if err != nil {
			_ = tlsConn.Close()
			span.RecordError(err)
			return nil, fmt.Errorf("sender: failed to create smtp client: %w", err)
		}

	case "STARTTLS":
		rawConn, err = dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			span.RecordError(err)
			return nil, fmt.Errorf("sender: failed to dial %s: %w", addr, err)
		}
		client, err = smtp.NewClient(rawConn, cfg.Host)
		if err != nil {
			_ = rawConn.Close()
			span.RecordError(err)
			return nil, fmt.Errorf("sender: failed to create smtp client: %w", err)
		}

		if err = client.Hello(heloHost); err != nil {
			_ = client.Close()
			span.RecordError(err)
			return nil, fmt.Errorf("sender: HELO/EHLO failed: %w", err)
		}

		if ok, _ := client.Extension("STARTTLS"); ok {
			tlsStart := time.Now()
			if err = client.StartTLS(tlsConfig); err != nil {
				_ = client.Close()
				span.RecordError(err)
				return nil, fmt.Errorf("sender: STARTTLS failed: %w", err)
			}
			metrics.Get().ObserveSmtpTLSHandshake(cfg.From, time.Since(tlsStart))
		}

	case "NONE":
		rawConn, err = dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			span.RecordError(err)
			return nil, fmt.Errorf("sender: failed to dial %s: %w", addr, err)
		}
		client, err = smtp.NewClient(rawConn, cfg.Host)
		if err != nil {
			_ = rawConn.Close()
			span.RecordError(err)
			return nil, fmt.Errorf("sender: failed to create smtp client: %w", err)
		}

		if err = client.Hello(heloHost); err != nil {
			_ = client.Close()
			span.RecordError(err)
			return nil, fmt.Errorf("sender: HELO/EHLO failed: %w", err)
		}

	default:
		return nil, fmt.Errorf("sender: unsupported encryption %q", encryption)
	}

	metrics.Get().ObserveSmtpDial(cfg.From, time.Since(dialStart))

	// SASL Authentication
	if cfg.Username != "" || cfg.Password != "" {
		authStart := time.Now()
		auth := BuildAuth(cfg.AuthType, cfg.Username, cfg.Password, cfg.Host)
		if auth != nil {
			if err = client.Auth(auth); err != nil {
				_ = client.Close()
				span.RecordError(err)
				return nil, fmt.Errorf("%w: %v", ErrAuthFailed, err)
			}
			metrics.Get().ObserveSmtpAuth(cfg.From, time.Since(authStart))
		}
	}

	logger.Get().WithContext(ctx).WithFields(logger.Fields{
		"host":       cfg.Host,
		"port":       cfg.Port,
		"encryption": string(encryption),
		"duration":   time.Since(dialStart).String(),
	}).Debug("SMTP connection established")

	return &SmtpClient{
		client:   client,
		conn:     rawConn,
		cfg:      cfg,
		lastUsed: time.Now(),
	}, nil
}

// Send sends an email message via this SMTP connection.
func (c *SmtpClient) Send(ctx context.Context, from string, recipients []string, msg []byte) error {
	if len(recipients) == 0 {
		return ErrNoRecipients
	}

	ctx, span := tracing.StartSpan(ctx, "smtp.send_transaction")
	defer span.End()

	span.SetAttribute("smtp.from", from)
	span.SetAttribute("smtp.recipients_count", len(recipients))
	span.SetAttribute("smtp.payload_bytes", len(msg))

	sendTimeout := c.cfg.SendTimeout
	if sendTimeout <= 0 {
		sendTimeout = 30 * time.Second
	}

	if c.conn != nil {
		deadline := time.Now().Add(sendTimeout)
		if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
			deadline = ctxDeadline
		}
		_ = c.conn.SetDeadline(deadline)
		defer func() {
			_ = c.conn.SetDeadline(time.Time{})
		}()
	}

	if err := ctx.Err(); err != nil {
		span.RecordError(err)
		return err
	}

	if err := c.client.Mail(from); err != nil {
		span.RecordError(err)
		return fmt.Errorf("sender: MAIL FROM <%s> failed: %w", from, err)
	}

	for _, rcpt := range recipients {
		if err := c.client.Rcpt(rcpt); err != nil {
			_ = c.client.Reset()
			span.RecordError(err)
			return fmt.Errorf("sender: RCPT TO <%s> failed: %w", rcpt, err)
		}
	}

	w, err := c.client.Data()
	if err != nil {
		_ = c.client.Reset()
		span.RecordError(err)
		return fmt.Errorf("sender: DATA command failed: %w", err)
	}

	if _, err = w.Write(msg); err != nil {
		_ = w.Close()
		_ = c.client.Reset()
		span.RecordError(err)
		return fmt.Errorf("sender: writing message body failed: %w", err)
	}

	if err = w.Close(); err != nil {
		_ = c.client.Reset()
		span.RecordError(err)
		return fmt.Errorf("sender: closing DATA writer failed: %w", err)
	}

	c.lastUsed = time.Now()
	return nil
}

// Noop checks if the connection is still alive using the SMTP NOOP command.
func (c *SmtpClient) Noop() error {
	if c.client == nil {
		return errors.New("client is nil")
	}
	return c.client.Noop()
}

// Reset resets the current SMTP transaction using RSET.
func (c *SmtpClient) Reset() error {
	if c.client == nil {
		return errors.New("client is nil")
	}
	return c.client.Reset()
}

// Close closes the SMTP connection gracefully.
func (c *SmtpClient) Close() error {
	if c.client == nil {
		return nil
	}
	_ = c.client.Quit()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// LastUsed returns the timestamp of when this client connection was last used.
func (c *SmtpClient) LastUsed() time.Time {
	return c.lastUsed
}
