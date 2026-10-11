package controlplane

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// UpsertQuota writes a tenant's ceilings.
//
// The content filters default to enabled, matching what a tenant with no
// recorded quota is treated as having in Quota. Without this, a quota
// written from a struct literal would silently switch both filters off.
func (s *Store) UpsertQuota(ctx context.Context, q Quota) (Quota, error) {
	if q.TenantID == "" {
		return Quota{}, errors.New("controlplane: quota requires a tenant id")
	}
	if q.PublishFailurePolicy == "" {
		q.PublishFailurePolicy = PolicyFailFast
	}
	if q.PerUserPublishQPS == 0 {
		q.PerUserPublishQPS = 10
	}
	if q.DuplicateWindowMS == 0 {
		q.DuplicateWindowMS = 3000
	}
	q.DuplicateContentEnabled = true
	q.PromoPatternEnabled = true
	q.UpdatedAt = s.now()

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO tenant_quotas
		   (tenant_id, max_connections, max_publish_qps, max_channels,
		    publish_failure_policy, per_user_publish_qps, duplicate_window_ms,
		    duplicate_content_enabled, promo_pattern_enabled, updated_at_ms)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(tenant_id) DO UPDATE SET
		   max_connections = excluded.max_connections,
		   max_publish_qps = excluded.max_publish_qps,
		   max_channels = excluded.max_channels,
		   publish_failure_policy = excluded.publish_failure_policy,
		   per_user_publish_qps = excluded.per_user_publish_qps,
		   duplicate_window_ms = excluded.duplicate_window_ms,
		   duplicate_content_enabled = excluded.duplicate_content_enabled,
		   promo_pattern_enabled = excluded.promo_pattern_enabled,
		   updated_at_ms = excluded.updated_at_ms`,
		q.TenantID, q.MaxConnections, q.MaxPublishQPS, q.MaxChannels,
		q.PublishFailurePolicy, q.PerUserPublishQPS, q.DuplicateWindowMS,
		boolToInt(q.DuplicateContentEnabled), boolToInt(q.PromoPatternEnabled),
		q.UpdatedAt.UnixMilli())
	if err != nil {
		return Quota{}, fmt.Errorf("controlplane: upsert quota: %w", err)
	}
	return q, nil
}

// Quota returns a tenant's ceilings. A tenant with no row gets defaults
// rather than an error, so a tenant is never left without a limit.
func (s *Store) Quota(ctx context.Context, tenantID string) (Quota, error) {
	var q Quota
	var dupEnabled, promoEnabled int
	var updatedMs int64
	err := s.db.QueryRowContext(ctx,
		`SELECT tenant_id, max_connections, max_publish_qps, max_channels,
		        publish_failure_policy, per_user_publish_qps, duplicate_window_ms,
		        duplicate_content_enabled, promo_pattern_enabled, updated_at_ms
		 FROM tenant_quotas WHERE tenant_id = ?`, tenantID).
		Scan(&q.TenantID, &q.MaxConnections, &q.MaxPublishQPS, &q.MaxChannels,
			&q.PublishFailurePolicy, &q.PerUserPublishQPS, &q.DuplicateWindowMS,
			&dupEnabled, &promoEnabled, &updatedMs)
	if errors.Is(err, sql.ErrNoRows) {
		return Quota{
			TenantID:                tenantID,
			MaxConnections:          0,
			MaxPublishQPS:           0,
			MaxChannels:             0,
			PublishFailurePolicy:    PolicyFailFast,
			PerUserPublishQPS:       10,
			DuplicateWindowMS:       3000,
			DuplicateContentEnabled: true,
			PromoPatternEnabled:     true,
			UpdatedAt:               s.now(),
		}, nil
	}
	if err != nil {
		return Quota{}, fmt.Errorf("controlplane: get quota: %w", err)
	}
	q.DuplicateContentEnabled = dupEnabled != 0
	q.PromoPatternEnabled = promoEnabled != 0
	q.UpdatedAt = time.UnixMilli(updatedMs)
	return q, nil
}

// UpsertChannelConfig writes a channel's overrides. Only the columns present
// on the supplied value are treated as intentional; a nil pointer means
// "derive it" and is stored as NULL so a later read reproduces the absence.
func (s *Store) UpsertChannelConfig(ctx context.Context, c ChannelConfig) (ChannelConfig, error) {
	if c.TenantID == "" || c.Channel == "" {
		return ChannelConfig{}, errors.New("controlplane: channel config requires a tenant id and a channel")
	}
	c.UpdatedAt = s.now()

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO channel_config
		   (tenant_id, channel, publish_ceiling_override_qps, receive_rate_target,
		    merge_ratio_override, coalescing_enabled, max_subscribers, updated_at_ms)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(tenant_id, channel) DO UPDATE SET
		   publish_ceiling_override_qps = excluded.publish_ceiling_override_qps,
		   receive_rate_target = excluded.receive_rate_target,
		   merge_ratio_override = excluded.merge_ratio_override,
		   coalescing_enabled = excluded.coalescing_enabled,
		   max_subscribers = excluded.max_subscribers,
		   updated_at_ms = excluded.updated_at_ms`,
		c.TenantID, c.Channel,
		nullableFloat(c.PublishCeilingOverrideQPS),
		nullableFloat(c.ReceiveRateTarget),
		nullableInt(c.MergeRatioOverride),
		boolToInt(c.CoalescingEnabled), c.MaxSubscribers, c.UpdatedAt.UnixMilli())
	if err != nil {
		return ChannelConfig{}, fmt.Errorf("controlplane: upsert channel config: %w", err)
	}
	return c, nil
}

// ChannelConfig returns a channel's overrides. A channel with no row is not
// an error: channels are created implicitly on first use, so the absence of a
// row is the normal state and yields all-derived configuration.
func (s *Store) ChannelConfig(ctx context.Context, tenantID, channel string) (ChannelConfig, error) {
	var c ChannelConfig
	var ceiling, rate sql.NullFloat64
	var merge sql.NullInt64
	var coalescing int
	var updatedMs int64
	err := s.db.QueryRowContext(ctx,
		`SELECT tenant_id, channel, publish_ceiling_override_qps, receive_rate_target,
		        merge_ratio_override, coalescing_enabled, max_subscribers, updated_at_ms
		 FROM channel_config WHERE tenant_id = ? AND channel = ?`, tenantID, channel).
		Scan(&c.TenantID, &c.Channel, &ceiling, &rate, &merge, &coalescing,
			&c.MaxSubscribers, &updatedMs)
	if errors.Is(err, sql.ErrNoRows) {
		return ChannelConfig{TenantID: tenantID, Channel: channel, CoalescingEnabled: true}, nil
	}
	if err != nil {
		return ChannelConfig{}, fmt.Errorf("controlplane: get channel config: %w", err)
	}
	if ceiling.Valid {
		v := ceiling.Float64
		c.PublishCeilingOverrideQPS = &v
	}
	if rate.Valid {
		v := rate.Float64
		c.ReceiveRateTarget = &v
	}
	if merge.Valid {
		v := int(merge.Int64)
		c.MergeRatioOverride = &v
	}
	c.CoalescingEnabled = coalescing != 0
	c.UpdatedAt = time.UnixMilli(updatedMs)
	return c, nil
}

// ListChannelConfigs returns every configured channel of one tenant.
func (s *Store) ListChannelConfigs(ctx context.Context, tenantID string) ([]ChannelConfig, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT tenant_id, channel, publish_ceiling_override_qps, receive_rate_target,
		        merge_ratio_override, coalescing_enabled, max_subscribers, updated_at_ms
		 FROM channel_config WHERE tenant_id = ? ORDER BY channel`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("controlplane: list channel configs: %w", err)
	}
	defer rows.Close()

	var out []ChannelConfig
	for rows.Next() {
		var c ChannelConfig
		var ceiling, rate sql.NullFloat64
		var merge sql.NullInt64
		var coalescing int
		var updatedMs int64
		if err := rows.Scan(&c.TenantID, &c.Channel, &ceiling, &rate, &merge,
			&coalescing, &c.MaxSubscribers, &updatedMs); err != nil {
			return nil, fmt.Errorf("controlplane: scan channel config: %w", err)
		}
		if ceiling.Valid {
			v := ceiling.Float64
			c.PublishCeilingOverrideQPS = &v
		}
		if rate.Valid {
			v := rate.Float64
			c.ReceiveRateTarget = &v
		}
		if merge.Valid {
			v := int(merge.Int64)
			c.MergeRatioOverride = &v
		}
		c.CoalescingEnabled = coalescing != 0
		c.UpdatedAt = time.UnixMilli(updatedMs)
		out = append(out, c)
	}
	return out, rows.Err()
}

// DeleteChannelConfig removes a channel's overrides, returning it to all-derived
// configuration.
func (s *Store) DeleteChannelConfig(ctx context.Context, tenantID, channel string) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM channel_config WHERE tenant_id = ? AND channel = ?`, tenantID, channel)
	if err != nil {
		return fmt.Errorf("controlplane: delete channel config: %w", err)
	}
	return nil
}

func nullableFloat(v *float64) any {
	if v == nil {
		return nil
	}
	return *v
}

func nullableInt(v *int) any {
	if v == nil {
		return nil
	}
	return *v
}
