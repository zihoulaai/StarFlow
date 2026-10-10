package adminadapters

import (
	"errors"
	"testing"

	"github.com/starflow/push/internal/admin"
	"github.com/starflow/push/internal/controlplane"
)

// One adapter satisfies all four aggregate interfaces. These fail to compile if
// a method moves, so the split cannot silently leave the adapter implementing
// only part of the surface.
var (
	_ admin.TenantStore      = (*Store)(nil)
	_ admin.ApplicationStore = (*Store)(nil)
	_ admin.QuotaStore       = (*Store)(nil)
	_ admin.ChannelStore     = (*Store)(nil)
)

// The admin handler decides 404 against 500 by branching on this sentinel, so a
// translated miss has to be matchable from outside this package. Translating
// onto a private value would force the handler to compare error text instead.
func TestTranslateExposesTheAdminNotFoundSentinel(t *testing.T) {
	err := translate(controlplane.ErrNotFound)
	if !errors.Is(err, admin.ErrNotFound) {
		t.Errorf("translated miss = %v, want it to match admin.ErrNotFound", err)
	}
}

// Anything that is not a miss must pass through untouched, or a store fault
// would be reported to the operator as a tenant that does not exist.
func TestTranslatePassesThroughEverythingElse(t *testing.T) {
	fault := errors.New("dial tcp 10.0.0.4:5432: connection refused")
	if got := translate(fault); !errors.Is(got, fault) {
		t.Errorf("translate returned %v, want the original error", got)
	}
	if errors.Is(translate(fault), admin.ErrNotFound) {
		t.Error("a store fault was translated into a not-found")
	}
}

// The admin surface reports a terminate as successful when this returns true,
// so the tenant has to reach the lookup rather than being dropped on the way
// through. A signature that accepted only an id could not scope anything, which
// is how an administrator granted one tenant ends up closing another's
// connection by naming its id.
func TestConnectionsTerminateScopesToItsTenant(t *testing.T) {
	var gotTenant string
	var gotID uint64
	calls := 0
	c := &Connections{
		Close: func(tenantID string, id uint64) bool {
			calls++
			gotTenant, gotID = tenantID, id
			return true
		},
	}

	if !c.Terminate("t1", 42) {
		t.Fatal("Terminate = false, want the wired Close's result to be reported")
	}
	if calls != 1 {
		t.Fatalf("Close called %d times, want 1", calls)
	}
	if gotTenant != "t1" {
		t.Errorf("Close received tenant %q, want %q", gotTenant, "t1")
	}
	if gotID != 42 {
		t.Errorf("Close received id %d, want 42", gotID)
	}
}

// A Connections value with nothing wired can list but cannot terminate. That
// case must report failure rather than appearing to have closed something.
func TestConnectionsTerminateWithoutWiringReportsFailure(t *testing.T) {
	c := &Connections{}
	if c.Terminate("t1", 1) {
		t.Error("Terminate = true with no Close wired, want false")
	}
}
