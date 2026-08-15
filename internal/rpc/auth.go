package rpc

import (
	"context"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"mailbaby/internal/config"
)

// UnaryAuthInterceptor returns a gRPC unary server interceptor enforcing secret key authentication.
func UnaryAuthInterceptor(cfg config.AuthConfig) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !cfg.Enabled {
			return handler(ctx, req)
		}

		if err := validateMetadataAuth(ctx, cfg); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

// StreamAuthInterceptor returns a gRPC stream server interceptor enforcing secret key authentication.
func StreamAuthInterceptor(cfg config.AuthConfig) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !cfg.Enabled {
			return handler(srv, ss)
		}

		if err := validateMetadataAuth(ss.Context(), cfg); err != nil {
			return err
		}

		return handler(srv, ss)
	}
}

func validateMetadataAuth(ctx context.Context, cfg config.AuthConfig) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Errorf(codes.Unauthenticated, "rpc: metadata is missing")
	}

	providedKey := extractMetadataToken(md, cfg.HeaderName)
	if providedKey == "" || subtle.ConstantTimeCompare([]byte(providedKey), []byte(cfg.SecretKey)) != 1 {
		return status.Errorf(codes.Unauthenticated, "rpc: invalid or missing authentication token / secret key")
	}

	return nil
}

func extractMetadataToken(md metadata.MD, headerName string) string {
	// 1. Authorization metadata
	if vals := md.Get("authorization"); len(vals) > 0 && vals[0] != "" {
		parts := strings.SplitN(vals[0], " ", 2)
		if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
			return strings.TrimSpace(parts[1])
		}
		return strings.TrimSpace(vals[0])
	}

	// 2. Custom header name
	if headerName != "" {
		lowerName := strings.ToLower(headerName)
		if vals := md.Get(lowerName); len(vals) > 0 && vals[0] != "" {
			return strings.TrimSpace(vals[0])
		}
	}

	// 3. Fallback x-api-key
	if vals := md.Get("x-api-key"); len(vals) > 0 && vals[0] != "" {
		return strings.TrimSpace(vals[0])
	}

	return ""
}
