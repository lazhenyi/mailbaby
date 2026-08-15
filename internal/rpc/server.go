package rpc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"mailbaby/internal/config"
	"mailbaby/internal/logger"
	"mailbaby/internal/metrics"
	"mailbaby/internal/queue"
	"mailbaby/internal/sender"
	"mailbaby/internal/tracing"
	pb "mailbaby/proto"
)

// Server coordinates the gRPC email service lifecycle.
type Server struct {
	pb.UnimplementedMailServiceServer
	cfg        *config.Config
	grpcServer *grpc.Server
	listener   net.Listener
	sender     sender.Sender
	producer   queue.Producer
	queueName  string
	mu         sync.Mutex
	running    bool
}

// New creates and configures a new gRPC Server instance.
func New(cfg *config.Config, s sender.Sender, p queue.Producer, extraOpts ...grpc.ServerOption) (*Server, error) {
	if cfg == nil {
		return nil, errors.New("rpc: config cannot be nil")
	}

	cfg.GRPC.ApplyDefaults()
	cfg.Auth.ApplyDefaults()

	queueName := ""
	if p != nil {
		queueName = cfg.Queue.TopicName()
	}

	serverOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(cfg.GRPC.MaxRecvMsgSize),
		grpc.MaxSendMsgSize(cfg.GRPC.MaxSendMsgSize),
		grpc.ChainUnaryInterceptor(
			UnaryAuthInterceptor(cfg.Auth),
		),
		grpc.ChainStreamInterceptor(
			StreamAuthInterceptor(cfg.Auth),
		),
	}
	serverOpts = append(serverOpts, extraOpts...)

	grpcSrv := grpc.NewServer(serverOpts...)

	srv := &Server{
		cfg:        cfg,
		grpcServer: grpcSrv,
		sender:     s,
		producer:   p,
		queueName:  queueName,
	}

	pb.RegisterMailServiceServer(grpcSrv, srv)
	return srv, nil
}

// Start binds the network listener and launches the gRPC server.
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return nil
	}

	addr := s.cfg.GRPC.Address()
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("rpc: failed to listen on %s: %w", addr, err)
	}
	s.listener = lis
	s.running = true
	s.mu.Unlock()

	errChan := make(chan error, 1)
	go func() {
		if err := s.grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			errChan <- err
		}
		close(errChan)
	}()

	select {
	case err := <-errChan:
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
		return fmt.Errorf("rpc: server error on %s: %w", addr, err)
	case <-time.After(50 * time.Millisecond):
		logger.Get().WithFields(logger.Fields{
			"addr": addr,
			"auth": s.cfg.Auth.Enabled,
		}).Info("gRPC server started")
		return nil
	}
}

// Stop gracefully stops the gRPC server.
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	s.mu.Unlock()

	logger.Get().Info("stopping gRPC server...")
	stopped := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		s.grpcServer.Stop()
		return ctx.Err()
	}
}

// Address returns the configured gRPC listen address.
func (s *Server) Address() string {
	return s.cfg.GRPC.Address()
}

// GRPCServer returns the underlying grpc.Server instance (useful for custom bindings / testing).
func (s *Server) GRPCServer() *grpc.Server {
	return s.grpcServer
}

// Send delivers a single email message synchronously or asynchronously.
func (s *Server) Send(ctx context.Context, req *pb.SendMailRequest) (*pb.SendMailResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "rpc: send mail request is nil")
	}

	email := protoToEmail(req)
	if err := email.Validate(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "rpc: email validation failed: %v", err)
	}

	if req.Async {
		return s.sendAsync(ctx, email)
	}
	return s.sendSync(ctx, email)
}

// SendBatch delivers multiple email messages.
func (s *Server) SendBatch(ctx context.Context, req *pb.BatchSendMailRequest) (*pb.BatchSendMailResponse, error) {
	if req == nil || len(req.Emails) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "rpc: batch request is empty")
	}

	resp := &pb.BatchSendMailResponse{
		Total:   int32(len(req.Emails)),
		Results: make([]*pb.SendMailResponse, len(req.Emails)),
	}

	var wg sync.WaitGroup
	var mu sync.Mutex

	for i, mailReq := range req.Emails {
		wg.Add(1)
		go func(idx int, item *pb.SendMailRequest) {
			defer wg.Done()
			if item == nil {
				mu.Lock()
				resp.Failed++
				resp.Results[idx] = &pb.SendMailResponse{
					Status:  "failed",
					Message: "nil item in batch",
					SentAt:  time.Now().UnixNano(),
				}
				mu.Unlock()
				return
			}

			email := protoToEmail(item)
			if err := email.Validate(); err != nil {
				mu.Lock()
				resp.Failed++
				resp.Results[idx] = &pb.SendMailResponse{
					Id:      email.ID,
					Status:  "failed",
					Message: fmt.Sprintf("validation failed: %v", err),
					SentAt:  time.Now().UnixNano(),
				}
				mu.Unlock()
				return
			}

			if req.Async || item.Async {
				r, err := s.sendAsync(ctx, email)
				mu.Lock()
				if err != nil {
					resp.Failed++
					resp.Results[idx] = &pb.SendMailResponse{
						Id:      email.ID,
						Status:  "failed",
						Message: fmt.Sprintf("enqueue failed: %v", err),
						SentAt:  time.Now().UnixNano(),
					}
				} else {
					resp.Succeeded++
					resp.Results[idx] = r
				}
				mu.Unlock()
			} else {
				r, err := s.sendSync(ctx, email)
				mu.Lock()
				if err != nil {
					resp.Failed++
					resp.Results[idx] = &pb.SendMailResponse{
						Id:      email.ID,
						Status:  "failed",
						Message: fmt.Sprintf("delivery failed: %v", err),
						SentAt:  time.Now().UnixNano(),
					}
				} else {
					resp.Succeeded++
					resp.Results[idx] = r
				}
				mu.Unlock()
			}
		}(i, mailReq)
	}

	wg.Wait()
	return resp, nil
}

// Ping checks service liveness.
func (s *Server) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{
		Status:    "OK",
		Version:   s.cfg.App.Env,
		Timestamp: time.Now().UnixNano(),
	}, nil
}

// HealthCheck checks service readiness and dependencies.
func (s *Server) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	details := make(map[string]string)
	statusVal := pb.HealthCheckResponse_SERVING

	if s.sender != nil {
		details["sender"] = "READY"
	} else {
		details["sender"] = "NOT_CONFIGURED"
	}

	if s.producer != nil {
		details["queue"] = "READY"
	} else {
		details["queue"] = "DIRECT_MODE"
	}

	return &pb.HealthCheckResponse{
		Status:  statusVal,
		Details: details,
	}, nil
}

func (s *Server) sendSync(ctx context.Context, email *sender.Email) (*pb.SendMailResponse, error) {
	if s.sender == nil {
		return nil, status.Errorf(codes.Unavailable, "rpc: smtp sender is not initialized")
	}

	start := time.Now()
	ctx, span := tracing.StartSpan(ctx, "grpc.send_email_sync")
	defer span.End()

	span.SetAttribute("email.id", email.ID)
	span.SetAttribute("email.subject", email.Subject)

	err := s.sender.Send(ctx, email)
	account := email.Account
	if account == "" {
		account = "default"
	}

	if err != nil {
		span.RecordError(err)
		metrics.Get().IncEmailsSent(account, "failed")
		return nil, status.Errorf(codes.Internal, "rpc: delivery failed: %v", err)
	}

	metrics.Get().IncEmailsSent(account, "success")
	metrics.Get().ObserveEmailDuration(account, time.Since(start))

	return &pb.SendMailResponse{
		Id:      email.ID,
		Status:  "sent",
		Message: "email sent successfully",
		SentAt:  time.Now().UnixNano(),
	}, nil
}

func (s *Server) sendAsync(ctx context.Context, email *sender.Email) (*pb.SendMailResponse, error) {
	data, err := email.ToJSON()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "rpc: failed to serialize email: %v", err)
	}

	msg := &queue.Message{
		ID:        email.ID,
		Payload:   data,
		Topic:     s.queueName,
		Timestamp: time.Now(),
		Attempts:  1,
	}

	if s.producer != nil {
		if err := s.producer.Publish(ctx, msg); err != nil {
			return nil, status.Errorf(codes.Internal, "rpc: failed to publish email to queue: %v", err)
		}
	} else if s.sender != nil {
		go func() {
			bgCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			if sendErr := s.sender.Send(bgCtx, email); sendErr != nil {
				logger.Get().WithError(sendErr).WithField("email_id", email.ID).Error("async background fallback delivery failed")
			}
		}()
	} else {
		return nil, status.Errorf(codes.Unavailable, "rpc: no queue producer or sender available")
	}

	return &pb.SendMailResponse{
		Id:      email.ID,
		Status:  "queued",
		Message: "email enqueued successfully",
		SentAt:  time.Now().UnixNano(),
	}, nil
}

func protoToEmail(req *pb.SendMailRequest) *sender.Email {
	id := req.Id
	if strings.TrimSpace(id) == "" {
		id = generateID()
	}

	attachments := make([]*sender.Attachment, 0, len(req.Attachments))
	for _, att := range req.Attachments {
		if att != nil {
			attachments = append(attachments, &sender.Attachment{
				Filename:    att.Filename,
				ContentType: att.ContentType,
				Data:        att.Data,
				Inline:      att.Inline,
				ContentID:   att.ContentId,
			})
		}
	}

	return &sender.Email{
		ID:          id,
		Account:     req.Account,
		From:        req.From,
		FromName:    req.FromName,
		ReplyTo:     req.ReplyTo,
		To:          req.To,
		Cc:          req.Cc,
		Bcc:         req.Bcc,
		Subject:     req.Subject,
		TextBody:    req.TextBody,
		HTMLBody:    req.HtmlBody,
		Headers:     req.Headers,
		Attachments: attachments,
		Tags:        req.Tags,
		Metadata:    req.Metadata,
	}
}

func generateID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
