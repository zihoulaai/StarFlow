package admin

import (
	"context"
	"strconv"
)

// principal is the resolved caller on a request context.
type principal struct {
	TenantID string
	Subject  string
	Role     Role
}

type contextKey struct{}

// withPrincipal returns a context carrying the resolved caller.
func withPrincipal(ctx context.Context, p principal) context.Context {
	return context.WithValue(ctx, contextKey{}, p)
}

// principalFrom returns the resolved caller.
func principalFrom(ctx context.Context) (principal, bool) {
	p, ok := ctx.Value(contextKey{}).(principal)
	return p, ok
}

func principalTenant(ctx context.Context) string {
	if p, ok := principalFrom(ctx); ok {
		return p.TenantID
	}
	return ""
}

func itoa(v int) string       { return strconv.Itoa(v) }
func itoaU64(v uint64) string { return strconv.FormatUint(v, 10) }

func parseUint(s string) uint64 {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return v
}
