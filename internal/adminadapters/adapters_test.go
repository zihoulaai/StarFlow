package adminadapters

import "testing"

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
