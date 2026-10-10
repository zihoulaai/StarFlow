package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The distinction between a mute and a ban is entirely in the expiry window: a
// mute lapses, a ban does not. If the effective-window filter confuses the two,
// either a mute keeps silencing a user forever or a ban silently expires.
func TestRestrictionEffectiveWindow(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	mutedUntil := f.now.Add(10 * time.Minute)
	if _, err := f.store.CreateRestriction(ctx, Restriction{
		TenantID: f.t1.ID, SubjectKind: SubjectUser, SubjectID: "u-muted",
		Action: RestrictionMute, ExpiresAt: mutedUntil,
	}); err != nil {
		t.Fatalf("CreateRestriction (mute): %v", err)
	}

	if _, err := f.store.CreateRestriction(ctx, Restriction{
		TenantID: f.t1.ID, SubjectKind: SubjectUser, SubjectID: "u-banned",
		Action: RestrictionBan,
	}); err != nil {
		t.Fatalf("CreateRestriction (ban): %v", err)
	}

	activeFor := func(subject string) []Restriction {
		rs, err := f.store.ActiveRestrictionsFor(ctx, f.t1.ID, SubjectUser, subject, f.now)
		if err != nil {
			t.Fatalf("ActiveRestrictionsFor(%s): %v", subject, err)
		}
		return rs
	}

	if got := activeFor("u-muted"); len(got) != 1 {
		t.Errorf("mute active before expiry: %d restrictions, want 1", len(got))
	}
	if got := activeFor("u-banned"); len(got) != 1 {
		t.Errorf("ban active: %d restrictions, want 1", len(got))
	}

	// Just past the mute's expiry, and far past it, the mute must not block.
	f.advance(11 * time.Minute)
	if got := activeFor("u-muted"); len(got) != 0 {
		t.Errorf("mute still blocking after expiry: %d restrictions, want 0", len(got))
	}
	f.advance(365 * 24 * time.Hour)
	if got := activeFor("u-muted"); len(got) != 0 {
		t.Errorf("mute still blocking a year later: %d restrictions, want 0", len(got))
	}

	// The ban has no expiry at all and must survive the same passage of time.
	if got := activeFor("u-banned"); len(got) != 1 {
		t.Errorf("ban expired: %d restrictions, want 1", len(got))
	}
}

// The filter must be evaluated in the query, not in Go after a full scan, so a
// lapsed restriction is never returned to a caller that then has to remember to
// re-check it.
func TestActiveRestrictionsExcludesLapsedInQuery(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	if _, err := f.store.CreateRestriction(ctx, Restriction{
		TenantID: f.t1.ID, SubjectKind: SubjectUser, SubjectID: "u1",
		Action: RestrictionMute, ExpiresAt: f.now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("CreateRestriction: %v", err)
	}

	// History must remain retrievable even after the restriction lapses, so an
	// operator can see a mute was applied.
	all, err := f.store.ListRestrictions(ctx, f.t1.ID)
	if err != nil {
		t.Fatalf("ListRestrictions: %v", err)
	}
	if len(all) != 1 {
		t.Errorf("ListRestrictions = %d, want 1 retained for history", len(all))
	}

	f.advance(2 * time.Minute)
	active, err := f.store.ActiveRestrictionsFor(ctx, f.t1.ID, SubjectUser, "u1", f.now)
	if err != nil {
		t.Fatalf("ActiveRestrictionsFor: %v", err)
	}
	if len(active) != 0 {
		t.Errorf("lapsed restriction returned as active: %d", len(active))
	}
}

func TestRestrictionScopedToSubjectKind(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	if _, err := f.store.CreateRestriction(ctx, Restriction{
		TenantID: f.t1.ID, SubjectKind: SubjectIP, SubjectID: "10.0.0.5",
		Action: RestrictionBan,
	}); err != nil {
		t.Fatalf("CreateRestriction: %v", err)
	}

	// An IP block must not silence a user whose id happens to be the same
	// string, and a user restriction must not be consulted for an address.
	byUser, err := f.store.ActiveRestrictionsFor(ctx, f.t1.ID, SubjectUser, "10.0.0.5", f.now)
	if err != nil {
		t.Fatalf("ActiveRestrictionsFor(user): %v", err)
	}
	if len(byUser) != 0 {
		t.Errorf("IP restriction matched a user subject: %d", len(byUser))
	}
	byIP, err := f.store.ActiveRestrictionsFor(ctx, f.t1.ID, SubjectIP, "10.0.0.5", f.now)
	if err != nil {
		t.Fatalf("ActiveRestrictionsFor(ip): %v", err)
	}
	if len(byIP) != 1 {
		t.Errorf("IP restriction not found: %d", len(byIP))
	}
}

func TestDeleteRestrictionLiftsIt(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	r, err := f.store.CreateRestriction(ctx, Restriction{
		TenantID: f.t1.ID, SubjectKind: SubjectUser, SubjectID: "u1", Action: RestrictionBan,
	})
	if err != nil {
		t.Fatalf("CreateRestriction: %v", err)
	}
	if err := f.store.DeleteRestriction(ctx, f.t1.ID, r.ID); err != nil {
		t.Fatalf("DeleteRestriction: %v", err)
	}
	active, err := f.store.ActiveRestrictionsFor(ctx, f.t1.ID, SubjectUser, "u1", f.now)
	if err != nil {
		t.Fatalf("ActiveRestrictionsFor: %v", err)
	}
	if len(active) != 0 {
		t.Errorf("restriction still active after delete: %d", len(active))
	}
	if err := f.store.DeleteRestriction(ctx, f.t1.ID, r.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("second delete err = %v, want ErrNotFound", err)
	}
}

func TestNetworkBlockLifecycle(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	b, err := f.store.CreateNetworkBlock(ctx, NetworkBlock{
		TenantID: f.t1.ID, CIDR: "203.0.113.0/24", Reason: "scraping",
	})
	if err != nil {
		t.Fatalf("CreateNetworkBlock: %v", err)
	}

	blocks, err := f.store.ListNetworkBlocks(ctx, f.t1.ID)
	if err != nil {
		t.Fatalf("ListNetworkBlocks: %v", err)
	}
	if len(blocks) != 1 || blocks[0].CIDR != "203.0.113.0/24" {
		t.Fatalf("blocks = %+v, want the created block", blocks)
	}
	if !blocks[0].Enabled {
		t.Error("a block created through this path should be enabled")
	}

	if err := f.store.DeleteNetworkBlock(ctx, f.t1.ID, b.ID); err != nil {
		t.Fatalf("DeleteNetworkBlock: %v", err)
	}
	blocks, err = f.store.ListNetworkBlocks(ctx, f.t1.ID)
	if err != nil {
		t.Fatalf("ListNetworkBlocks after delete: %v", err)
	}
	if len(blocks) != 0 {
		t.Errorf("block survived deletion: %d", len(blocks))
	}
}

func TestCreateNetworkBlockRejectsIncompleteInput(t *testing.T) {
	f := newFixture(t)
	if _, err := f.store.CreateNetworkBlock(context.Background(), NetworkBlock{TenantID: f.t1.ID}); err == nil {
		t.Error("block without a cidr was accepted")
	}
	if _, err := f.store.CreateNetworkBlock(context.Background(), NetworkBlock{CIDR: "10.0.0.0/8"}); err == nil {
		t.Error("block without a tenant was accepted")
	}
}

// Replacement rules must be evaluated before block rules, otherwise a block
// rule naming a short string would shadow a replacement rule naming a longer
// one, and the longer content would never reach the replacement path.
func TestListRulesOrdersReplacementBeforeBlock(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	for _, r := range []ModerationRule{
		{Pattern: "zzz-block", Action: ActionBlock},
		{Pattern: "aaa-block", Action: ActionBlock},
		{Pattern: "mmm-tag", Action: ActionTag},
		{Pattern: "bbb-replace", Action: ActionReplace},
	} {
		if _, err := f.store.UpsertRule(ctx, ModerationRule{
			TenantID: f.t1.ID, Kind: RuleKindSensitiveWord,
			Pattern: r.Pattern, Action: r.Action, Replacement: "*", Tag: "t",
		}); err != nil {
			t.Fatalf("UpsertRule(%s): %v", r.Pattern, err)
		}
	}

	rules, err := f.store.ListRules(ctx, f.t1.ID)
	if err != nil {
		t.Fatalf("ListRules: %v", err)
	}
	if len(rules) != 4 {
		t.Fatalf("got %d rules, want 4", len(rules))
	}
	wantOrder := []string{ActionReplace, ActionTag, ActionBlock, ActionBlock}
	for i, want := range wantOrder {
		if rules[i].Action != want {
			t.Errorf("position %d action = %q, want %q", i, rules[i].Action, want)
		}
	}
	// Within the same action, ordering must be stable so a rebuild produces the
	// same automaton.
	if rules[2].Pattern != "aaa-block" || rules[3].Pattern != "zzz-block" {
		t.Errorf("same-action ordering is not stable: %q then %q", rules[2].Pattern, rules[3].Pattern)
	}
}

func TestRuleDefaultsAndUpdates(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	r, err := f.store.UpsertRule(ctx, ModerationRule{TenantID: f.t1.ID, Pattern: "spam"})
	if err != nil {
		t.Fatalf("UpsertRule: %v", err)
	}
	if r.Action != ActionBlock {
		t.Errorf("default action = %q, want %q", r.Action, ActionBlock)
	}
	if r.Kind != RuleKindSensitiveWord {
		t.Errorf("default kind = %q, want %q", r.Kind, RuleKindSensitiveWord)
	}

	if _, err := f.store.UpsertRule(ctx, ModerationRule{
		ID: r.ID, TenantID: f.t1.ID, Pattern: "spam", Action: ActionTag, Tag: "promo",
	}); err != nil {
		t.Fatalf("update rule: %v", err)
	}
	rules, err := f.store.ListRules(ctx, f.t1.ID)
	if err != nil {
		t.Fatalf("ListRules: %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("upsert created a duplicate: %d rules", len(rules))
	}
	if rules[0].Action != ActionTag || rules[0].Tag != "promo" {
		t.Errorf("update did not apply: %+v", rules[0])
	}

	if err := f.store.DeleteRule(ctx, f.t1.ID, r.ID); err != nil {
		t.Fatalf("DeleteRule: %v", err)
	}
	rules, _ = f.store.ListRules(ctx, f.t1.ID)
	if len(rules) != 0 {
		t.Errorf("rule survived deletion: %d", len(rules))
	}
}
