package controlplane

import (
	"context"
	"testing"
	"time"
)

// Every moderation decision must leave a trace. An unlogged block is
// indistinguishable from a message that was never published.
func TestAppendAuditRecordsEachDecision(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	decisions := []struct {
		channel   string
		messageID string
		ruleID    string
		decision  string
	}{
		{channel: "room:1", messageID: "m-100", ruleID: "rule-a", decision: "blocked"},
		{channel: "room:1", messageID: "m-101", ruleID: "rule-a", decision: "blocked"},
		{channel: "room:2", messageID: "m-102", ruleID: "rule-b", decision: "tagged"},
		{channel: "room:2", messageID: "m-103", ruleID: "", decision: "muted"},
	}

	for i, d := range decisions {
		err := f.store.AppendAudit(ctx, AuditEntry{
			TenantID: f.t1.ID, Kind: AuditModeration,
			Subject: "u1", Channel: d.channel, MessageID: d.messageID,
			RuleID: d.ruleID, Decision: d.decision,
			Body: RedactBody(f.t1, "original text"),
		})
		if err != nil {
			t.Errorf("AppendAudit %d: %v", i, err)
		}
	}

	entries, err := f.store.QueryAudit(ctx, AuditQuery{TenantID: f.t1.ID, Kind: AuditModeration})
	if err != nil {
		t.Fatalf("QueryAudit: %v", err)
	}
	if len(entries) != len(decisions) {
		t.Fatalf("got %d entries, want %d", len(entries), len(decisions))
	}
	for _, e := range entries {
		if e.TenantID != f.t1.ID {
			t.Errorf("entry tenant = %q, want %q", e.TenantID, f.t1.ID)
		}
		if e.Decision == "" || e.Channel == "" {
			t.Errorf("entry is missing required context: %+v", e)
		}
	}
}

// Retention and redaction are tenant preferences, so an audit trail must honour
// whichever the tenant has set rather than one global default.
func TestAuditRedactionFollowsTenantSetting(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	// f.t1 defaults to redacting. Give t2 the opposite setting.
	unredacted, err := f.store.UpdateTenant(ctx, f.t2.ID, func(tn *Tenant) {
		tn.RedactAuditBodies = false
	})
	if err != nil {
		t.Fatalf("UpdateTenant: %v", err)
	}

	for _, tenant := range []Tenant{f.t1, unredacted} {
		if err := f.store.AppendAudit(ctx, AuditEntry{
			TenantID: tenant.ID, Kind: AuditModeration, Decision: "blocked",
			Body: RedactBody(tenant, "sensitive text"),
		}); err != nil {
			t.Fatalf("AppendAudit for %s: %v", tenant.ID, err)
		}
	}

	t1Entries, err := f.store.QueryAudit(ctx, AuditQuery{TenantID: f.t1.ID})
	if err != nil {
		t.Fatalf("QueryAudit t1: %v", err)
	}
	if len(t1Entries) != 1 || t1Entries[0].Body != "[redacted]" {
		t.Errorf("t1 entry = %+v, want a redacted body", t1Entries)
	}

	t2Entries, err := f.store.QueryAudit(ctx, AuditQuery{TenantID: f.t2.ID})
	if err != nil {
		t.Fatalf("QueryAudit t2: %v", err)
	}
	if len(t2Entries) != 1 || t2Entries[0].Body != "sensitive text" {
		t.Errorf("t2 entry = %+v, want the unredacted body", t2Entries)
	}
}

// An audit query without a tenant scope must be refused outright rather than
// defaulting to "everything".
func TestQueryAuditRequiresTenant(t *testing.T) {
	f := newFixture(t)
	if _, err := f.store.QueryAudit(context.Background(), AuditQuery{}); err == nil {
		t.Fatal("unscoped audit query was accepted")
	}
}

func TestQueryAuditFiltersAndOrdering(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	for i, e := range []AuditEntry{
		{TenantID: f.t1.ID, Kind: AuditModeration, Channel: "room:1", Subject: "u1", Decision: "blocked"},
		{TenantID: f.t1.ID, Kind: AuditModeration, Channel: "room:2", Subject: "u2", Decision: "blocked"},
		{TenantID: f.t1.ID, Kind: AuditAuth, Channel: "room:1", Subject: "u3", Decision: "rejected"},
	} {
		e.At = f.now.Add(time.Duration(i) * time.Second)
		if err := f.store.AppendAudit(ctx, e); err != nil {
			t.Fatalf("AppendAudit: %v", err)
		}
	}

	byKind, err := f.store.QueryAudit(ctx, AuditQuery{TenantID: f.t1.ID, Kind: AuditModeration})
	if err != nil {
		t.Fatalf("QueryAudit by kind: %v", err)
	}
	if len(byKind) != 2 {
		t.Errorf("kind filter returned %d, want 2", len(byKind))
	}

	byChannel, err := f.store.QueryAudit(ctx, AuditQuery{TenantID: f.t1.ID, Channel: "room:1"})
	if err != nil {
		t.Fatalf("QueryAudit by channel: %v", err)
	}
	if len(byChannel) != 2 {
		t.Errorf("channel filter returned %d, want 2", len(byChannel))
	}

	bySubject, err := f.store.QueryAudit(ctx, AuditQuery{TenantID: f.t1.ID, Subject: "u2"})
	if err != nil {
		t.Fatalf("QueryAudit by subject: %v", err)
	}
	if len(bySubject) != 1 || bySubject[0].Channel != "room:2" {
		t.Errorf("subject filter returned %+v", bySubject)
	}

	// Newest first, so an operator sees the most recent decision without paging.
	if len(byChannel) < 2 || !byChannel[0].At.After(byChannel[1].At) {
		t.Errorf("entries are not ordered newest first: %v", byChannel)
	}
}

func TestQueryAuditClampsUnboundedLimit(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := f.store.AppendAudit(ctx, AuditEntry{
			TenantID: f.t1.ID, Kind: AuditAdmin, Decision: "granted",
		}); err != nil {
			t.Fatalf("AppendAudit: %v", err)
		}
	}
	got, err := f.store.QueryAudit(ctx, AuditQuery{TenantID: f.t1.ID, Limit: -1})
	if err != nil {
		t.Fatalf("QueryAudit: %v", err)
	}
	if len(got) != 5 {
		t.Errorf("got %d, want all 5 within the default clamp", len(got))
	}
}

func TestPurgeAuditHonoursRetention(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	old, err := f.store.UpdateTenant(ctx, f.t1.ID, func(tn *Tenant) {
		tn.AuditRetentionDays = 7
	})
	if err != nil {
		t.Fatalf("UpdateTenant: %v", err)
	}

	if err := f.store.AppendAudit(ctx, AuditEntry{
		TenantID: f.t1.ID, Kind: AuditModeration, Decision: "blocked", At: f.now,
	}); err != nil {
		t.Fatalf("AppendAudit old: %v", err)
	}
	f.advance(8 * 24 * time.Hour)
	if err := f.store.AppendAudit(ctx, AuditEntry{
		TenantID: f.t1.ID, Kind: AuditModeration, Decision: "blocked", At: f.now,
	}); err != nil {
		t.Fatalf("AppendAudit recent: %v", err)
	}
	// Another tenant's entry inside the same window must be untouched.
	if err := f.store.AppendAudit(ctx, AuditEntry{
		TenantID: f.t2.ID, Kind: AuditModeration, Decision: "blocked", At: f.now,
	}); err != nil {
		t.Fatalf("AppendAudit other tenant: %v", err)
	}

	cutoff := RetentionCutoff(old, f.now)
	n, err := f.store.PurgeAuditBefore(ctx, f.t1.ID, cutoff)
	if err != nil {
		t.Fatalf("PurgeAuditBefore: %v", err)
	}
	if n != 1 {
		t.Errorf("purged %d entries, want 1", n)
	}

	remaining, err := f.store.QueryAudit(ctx, AuditQuery{TenantID: f.t1.ID})
	if err != nil {
		t.Fatalf("QueryAudit: %v", err)
	}
	if len(remaining) != 1 {
		t.Errorf("t1 retained %d entries, want 1", len(remaining))
	}
	other, err := f.store.QueryAudit(ctx, AuditQuery{TenantID: f.t2.ID})
	if err != nil {
		t.Fatalf("QueryAudit other: %v", err)
	}
	if len(other) != 1 {
		t.Errorf("purge affected another tenant: %d entries left, want 1", len(other))
	}
}

func TestAppendAuditRejectsIncompleteEntry(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	if err := f.store.AppendAudit(ctx, AuditEntry{Kind: AuditAdmin, Decision: "x"}); err == nil {
		t.Error("entry without a tenant was accepted")
	}
	if err := f.store.AppendAudit(ctx, AuditEntry{TenantID: f.t1.ID, Decision: "x"}); err == nil {
		t.Error("entry without a kind was accepted")
	}
	if err := f.store.AppendAudit(ctx, AuditEntry{TenantID: f.t1.ID, Kind: AuditAdmin}); err == nil {
		t.Error("entry without a decision was accepted")
	}
}
