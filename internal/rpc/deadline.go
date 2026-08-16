package rpc

import (
	"context"
	"time"

	"google.golang.org/grpc"
)

// DefaultRPCTimeout caps the lifetime of any unary RPC that the caller did not
// already constrain with its own deadline. Callers that need longer-running
// operations should pass a ctx with their own deadline; this interceptor only
// protects against clients that forget to set one.
const DefaultRPCTimeout = 30 * time.Second

// UnaryDeadlineInterceptor returns an interceptor that bounds handler
// execution time by enforcing a default deadline when the inbound context has
// none. It should run AFTER the auth interceptor so unauthenticated calls are
// rejected promptly without consuming a deadline.
func UnaryDeadlineInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, hasDeadline := ctx.Deadline(); !hasDeadline {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, DefaultRPCTimeout)
			defer cancel()
		}
		return handler(ctx, req)
	}
}