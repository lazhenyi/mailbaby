package rpc

import (
	"context"
	"errors"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"mailbaby/internal/config"
	"mailbaby/internal/queue"
	"mailbaby/internal/sender"
	pb "mailbaby/proto"
)

type mockSender struct {
	sendFunc func(ctx context.Context, email *sender.Email) error
}

func (m *mockSender) Send(ctx context.Context, email *sender.Email) error {
	if m.sendFunc != nil {
		return m.sendFunc(ctx, email)
	}
	return nil
}

func (m *mockSender) SendBatch(ctx context.Context, emails []*sender.Email) []error {
	errs := make([]error, len(emails))
	for i, e := range emails {
		errs[i] = m.Send(ctx, e)
	}
	return errs
}

func (m *mockSender) Account(name string) (sender.AccountSender, error) {
	return nil, errors.New("not implemented")
}

func (m *mockSender) AccountNames() []string {
	return []string{"default"}
}

func (m *mockSender) Stats() map[string]sender.AccountStats {
	return nil
}

func (m *mockSender) Close() error {
	return nil
}

type mockProducer struct {
	published []*queue.Message
}

func (m *mockProducer) Publish(ctx context.Context, msg *queue.Message, opts ...queue.PublishOption) error {
	m.published = append(m.published, msg)
	return nil
}

func (m *mockProducer) PublishBatch(ctx context.Context, msgs []*queue.Message, opts ...queue.PublishOption) error {
	m.published = append(m.published, msgs...)
	return nil
}

func (m *mockProducer) Close() error {
	return nil
}

func setupTestGRPCServer(t *testing.T, cfg *config.Config, s sender.Sender, p queue.Producer) (pb.MailServiceClient, func()) {
	const bufSize = 1024 * 1024
	lis := bufconn.Listen(bufSize)

	server, err := New(cfg, s, p)
	if err != nil {
		t.Fatalf("failed to create rpc server: %v", err)
	}

	go func() {
		if err := server.GRPCServer().Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Logf("Serve error: %v", err)
		}
	}()

	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufnet: %v", err)
	}

	client := pb.NewMailServiceClient(conn)

	cleanup := func() {
		_ = conn.Close()
		server.GRPCServer().Stop()
		_ = lis.Close()
	}

	return client, cleanup
}

func TestGRPCServer_Send(t *testing.T) {
	cfg := &config.Config{
		App: config.AppConfig{Env: "test"},
		GRPC: config.GrpcConfig{
			Enabled: true,
			Port:    8081,
		},
		Auth: config.AuthConfig{
			Enabled: false,
		},
	}

	s := &mockSender{
		sendFunc: func(ctx context.Context, email *sender.Email) error {
			if email.Subject == "fail" {
				return errors.New("smtp down")
			}
			return nil
		},
	}
	prod := &mockProducer{}

	client, cleanup := setupTestGRPCServer(t, cfg, s, prod)
	defer cleanup()

	ctx := context.Background()

	t.Run("successful sync send", func(t *testing.T) {
		req := &pb.SendMailRequest{
			To:       []string{"test@example.com"},
			Subject:  "Hello gRPC",
			TextBody: "Text body content",
		}
		resp, err := client.Send(ctx, req)
		if err != nil {
			t.Fatalf("Send failed: %v", err)
		}
		if resp.Status != "sent" {
			t.Errorf("expected status 'sent', got %q", resp.Status)
		}
		if resp.Id == "" {
			t.Errorf("expected generated id")
		}
	})

	t.Run("successful async send", func(t *testing.T) {
		req := &pb.SendMailRequest{
			To:       []string{"async@example.com"},
			Subject:  "Hello Async",
			TextBody: "Async text",
			Async:    true,
		}
		resp, err := client.Send(ctx, req)
		if err != nil {
			t.Fatalf("Send async failed: %v", err)
		}
		if resp.Status != "queued" {
			t.Errorf("expected status 'queued', got %q", resp.Status)
		}
		if len(prod.published) != 1 {
			t.Errorf("expected 1 message in producer, got %d", len(prod.published))
		}
	})

	t.Run("delivery failure returns internal error", func(t *testing.T) {
		req := &pb.SendMailRequest{
			To:      []string{"test@example.com"},
			Subject: "fail",
		}
		_, err := client.Send(ctx, req)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.Internal {
			t.Errorf("expected codes.Internal, got %v", err)
		}
	})

	t.Run("validation failure returns invalid argument", func(t *testing.T) {
		req := &pb.SendMailRequest{
			Subject: "No recipients",
		}
		_, err := client.Send(ctx, req)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.InvalidArgument {
			t.Errorf("expected codes.InvalidArgument, got %v", err)
		}
	})
}

func TestGRPCServer_SendBatch(t *testing.T) {
	cfg := &config.Config{
		App:  config.AppConfig{Env: "test"},
		GRPC: config.GrpcConfig{Enabled: true},
		Auth: config.AuthConfig{Enabled: false},
	}
	s := &mockSender{}
	client, cleanup := setupTestGRPCServer(t, cfg, s, nil)
	defer cleanup()

	ctx := context.Background()

	req := &pb.BatchSendMailRequest{
		Emails: []*pb.SendMailRequest{
			{To: []string{"u1@example.com"}, Subject: "Batch 1"},
			{To: []string{"u2@example.com"}, Subject: "Batch 2"},
		},
	}

	resp, err := client.SendBatch(ctx, req)
	if err != nil {
		t.Fatalf("SendBatch failed: %v", err)
	}

	if resp.Total != 2 || resp.Succeeded != 2 || resp.Failed != 0 {
		t.Errorf("unexpected batch result: total=%d, succ=%d, failed=%d", resp.Total, resp.Succeeded, resp.Failed)
	}
}

func TestGRPCServer_PingAndHealth(t *testing.T) {
	cfg := &config.Config{
		App:  config.AppConfig{Env: "test-env"},
		GRPC: config.GrpcConfig{Enabled: true},
		Auth: config.AuthConfig{Enabled: false},
	}
	s := &mockSender{}
	client, cleanup := setupTestGRPCServer(t, cfg, s, nil)
	defer cleanup()

	ctx := context.Background()

	// 1. Test Ping
	pingResp, err := client.Ping(ctx, &pb.PingRequest{Message: "hello"})
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
	if pingResp.Status != "OK" || pingResp.Version != "test-env" {
		t.Errorf("unexpected Ping response: %v", pingResp)
	}

	// 2. Test HealthCheck
	healthResp, err := client.HealthCheck(ctx, &pb.HealthCheckRequest{Service: "mailbaby"})
	if err != nil {
		t.Fatalf("HealthCheck failed: %v", err)
	}
	if healthResp.Status != pb.HealthCheckResponse_SERVING {
		t.Errorf("expected SERVING status, got %v", healthResp.Status)
	}
}

func TestGRPCServer_AuthInterceptor(t *testing.T) {
	cfg := &config.Config{
		App:  config.AppConfig{Env: "test"},
		GRPC: config.GrpcConfig{Enabled: true},
		Auth: config.AuthConfig{
			Enabled:    true,
			SecretKey:  "secret_rpc_token",
			HeaderName: "X-API-Key",
		},
	}
	s := &mockSender{}
	client, cleanup := setupTestGRPCServer(t, cfg, s, nil)
	defer cleanup()

	req := &pb.SendMailRequest{
		To:      []string{"user@example.com"},
		Subject: "Auth Test",
	}

	t.Run("no metadata returns unauthenticated", func(t *testing.T) {
		_, err := client.Send(context.Background(), req)
		if err == nil {
			t.Fatal("expected unauthenticated error, got nil")
		}
		st, _ := status.FromError(err)
		if st.Code() != codes.Unauthenticated {
			t.Errorf("expected codes.Unauthenticated, got %v", st.Code())
		}
	})

	t.Run("invalid token returns unauthenticated", func(t *testing.T) {
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-api-key", "wrong_key"))
		_, err := client.Send(ctx, req)
		if err == nil {
			t.Fatal("expected unauthenticated error, got nil")
		}
		st, _ := status.FromError(err)
		if st.Code() != codes.Unauthenticated {
			t.Errorf("expected codes.Unauthenticated, got %v", st.Code())
		}
	})

	t.Run("valid x-api-key succeeds", func(t *testing.T) {
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-api-key", "secret_rpc_token"))
		resp, err := client.Send(ctx, req)
		if err != nil {
			t.Fatalf("expected success, got %v", err)
		}
		if resp.Status != "sent" {
			t.Errorf("expected status 'sent', got %q", resp.Status)
		}
	})

	t.Run("valid authorization bearer succeeds", func(t *testing.T) {
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer secret_rpc_token"))
		resp, err := client.Send(ctx, req)
		if err != nil {
			t.Fatalf("expected success, got %v", err)
		}
		if resp.Status != "sent" {
			t.Errorf("expected status 'sent', got %q", resp.Status)
		}
	})
}
