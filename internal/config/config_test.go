package config

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// The whole capacity model rests on a known per-viewer egress budget, and the
// memory limit is what keeps a mis-sized instance from being OOM-killed with no
// diagnosis. A missing or malformed limit must stop the process, not quietly
// fall back to a default.
func TestLoadRequiresExplicitMemoryLimit(t *testing.T) {
	_, err := Load(func(string) string { return "" })
	if !errors.Is(err, ErrMissingMemLimit) {
		t.Fatalf("err = %v, want %v", err, ErrMissingMemLimit)
	}
}

func TestLoadRejectsUnparseableMemoryLimit(t *testing.T) {
	env := map[string]string{"GOMEMLIMIT_BYTES": "12GB"}
	_, err := Load(mapGetenv(env))
	if !errors.Is(err, ErrMissingMemLimit) {
		t.Fatalf("err = %v, want %v", err, ErrMissingMemLimit)
	}
}

func TestLoadRejectsNonPositiveMemoryLimit(t *testing.T) {
	for _, raw := range []string{"0", "-1"} {
		env := map[string]string{"GOMEMLIMIT_BYTES": raw}
		_, err := Load(mapGetenv(env))
		if !errors.Is(err, ErrMissingMemLimit) {
			t.Fatalf("GOMEMLIMIT_BYTES=%s: err = %v, want %v", raw, err, ErrMissingMemLimit)
		}
	}
}

func TestLoadAppliesExplicitMemoryLimit(t *testing.T) {
	env := map[string]string{"GOMEMLIMIT_BYTES": "12884901888"} // 12 GiB
	cfg, err := Load(mapGetenv(env))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Runtime.MemLimitBytes != 12884901888 {
		t.Errorf("MemLimitBytes = %d, want 12884901888", cfg.Runtime.MemLimitBytes)
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(func(key string) string {
		if key == "GOMEMLIMIT_BYTES" {
			return "8589934592"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"FlushInterval", cfg.Delivery.FlushInterval, 10 * time.Millisecond},
		{"WriteBufferBytes", cfg.Delivery.WriteBufferBytes, 16 * 1024},
		{"ReadBufferBytes", cfg.Delivery.ReadBufferBytes, 4 * 1024},
		{"MergeRatio", cfg.Delivery.MergeRatio, 20},
		{"PerViewerBytesPerSecond", cfg.Delivery.PerViewerBytesPerSecond, DefaultPerViewerBytesPerSecond},
		{"Shards", cfg.Delivery.Shards, 1024},
		{"GOGC", cfg.Runtime.GOGC, 400},
		{"SecretSource", cfg.ControlPlane.SecretSource, "env"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestLoadOverrides(t *testing.T) {
	env := map[string]string{
		"GOMEMLIMIT_BYTES":        "8589934592",
		"INSTANCE_ID":             "12",
		"DELIVERY_FLUSH_INTERVAL": "25ms",
		"DELIVERY_MERGE_RATIO":    "8",
		"DELIVERY_PER_VIEWER_BPS": "4096",
		"DELIVERY_SHARDS":         "256",
		"AUTH_TOKEN_TTL":          "10m",
		"BUS_KIND":                "redis",
		"BUS_ADDR":                "bus:6379",
		"SECRET_SOURCE":           "file",
		"SECRET_REF":              "/run/secrets/token",
		"CONTROL_POLL_INTERVAL":   "30s",
		"CONTROL_MAX_OPEN_CONNS":  "8",
	}
	cfg, err := Load(mapGetenv(env))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Instance != 12 {
		t.Errorf("Instance = %d, want 12", cfg.Instance)
	}
	if cfg.Delivery.FlushInterval != 25*time.Millisecond {
		t.Errorf("FlushInterval = %v, want 25ms", cfg.Delivery.FlushInterval)
	}
	if cfg.Delivery.MergeRatio != 8 {
		t.Errorf("MergeRatio = %d, want 8", cfg.Delivery.MergeRatio)
	}
	if cfg.Delivery.PerViewerBytesPerSecond != 4096 {
		t.Errorf("PerViewerBytesPerSecond = %d, want 4096", cfg.Delivery.PerViewerBytesPerSecond)
	}
	if cfg.Bus.Kind != "redis" || cfg.Bus.Addr != "bus:6379" {
		t.Errorf("Bus = %+v, want redis at bus:6379", cfg.Bus)
	}
	if cfg.ControlPlane.SecretSource != "file" || cfg.ControlPlane.SecretRef != "/run/secrets/token" {
		t.Errorf("secret source = %q/%q", cfg.ControlPlane.SecretSource, cfg.ControlPlane.SecretRef)
	}
}

func TestValidateRejectsBadValues(t *testing.T) {
	valid := func() *Config {
		cfg, err := Load(func(key string) string {
			if key == "GOMEMLIMIT_BYTES" {
				return "8589934592"
			}
			return ""
		})
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		return cfg
	}

	cases := map[string]func(*Config){
		"unknown bus kind":    func(c *Config) { c.Bus.Kind = "kafka" },
		"redis without addr":  func(c *Config) { c.Bus.Kind = "redis"; c.Bus.Addr = "" },
		"empty bus channel":   func(c *Config) { c.Bus.Channel = "" },
		"bad secret source":   func(c *Config) { c.ControlPlane.SecretSource = "arg" },
		"empty secret ref":    func(c *Config) { c.ControlPlane.SecretRef = "" },
		"zero flush interval": func(c *Config) { c.Delivery.FlushInterval = 0 },
		"zero write buffer":   func(c *Config) { c.Delivery.WriteBufferBytes = 0 },
		"zero merge ratio":    func(c *Config) { c.Delivery.MergeRatio = 0 },
		"zero per viewer bps": func(c *Config) { c.Delivery.PerViewerBytesPerSecond = 0 },
		"zero shards":         func(c *Config) { c.Delivery.Shards = 0 },
		"zero heartbeat":      func(c *Config) { c.Delivery.HeartbeatInterval = 0 },
		"negative instance":   func(c *Config) { c.Instance = -1 },
		"negative token ttl":  func(c *Config) { c.Delivery.TokenTTL = 0 },
	}

	for name, mutate := range cases {
		cfg := valid()
		mutate(cfg)
		if err := cfg.Validate(); err == nil {
			t.Errorf("%s: Validate returned nil, want an error", name)
		}
	}
}

func TestApplyRuntimeInstallsMemoryLimitAndGCPercent(t *testing.T) {
	original := setGCPercent
	defer func() { setGCPercent = original }()

	var gotPercent int
	setGCPercent = func(p int) int { gotPercent = p; return 100 }

	cfg, err := Load(func(key string) string {
		switch key {
		case "GOMEMLIMIT_BYTES":
			return "8589934592"
		case "GOGC":
			return "350"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	cfg.ApplyRuntime()
	if gotPercent != 350 {
		t.Errorf("GC percent = %d, want 350", gotPercent)
	}
}

func mapGetenv(env map[string]string) func(string) string {
	return func(key string) string { return env[key] }
}

// baseEnv is a configuration that loads cleanly, for tests that vary one key.
func baseEnv(extra map[string]string) map[string]string {
	env := map[string]string{"GOMEMLIMIT_BYTES": "8589934592"}
	for k, v := range extra {
		env[k] = v
	}
	return env
}

// Compression is on by default because coalescing is on by default, and the
// default pairing is the one that was measured to save 88% of egress bytes.
func TestCompressDefaultsOnWithCoalescing(t *testing.T) {
	cfg, err := Load(mapGetenv(baseEnv(nil)))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.Delivery.Compress {
		t.Error("Compress = false by default; coalescing is on by default and an 88% saving should be taken")
	}
	if cfg.Delivery.MergeRatio != 20 {
		t.Errorf("MergeRatio = %d, want 20", cfg.Delivery.MergeRatio)
	}
}

// The coupling: a threshold below two would compress one-message batches, which
// expand rather than shrink. It must be refused at startup, where the operator is
// present, rather than surfacing later as bandwidth that quietly went up.
func TestCompressRejectsThresholdBelowTwo(t *testing.T) {
	for _, threshold := range []string{"0", "1"} {
		_, err := Load(mapGetenv(baseEnv(map[string]string{
			"DELIVERY_COMPRESS_MIN_BATCH": threshold,
		})))
		if err == nil {
			t.Errorf("Load accepted DELIVERY_COMPRESS_MIN_BATCH=%s; it compresses expanding batches", threshold)
			continue
		}
		if !strings.Contains(err.Error(), "DELIVERY_COMPRESS_MIN_BATCH") {
			t.Errorf("error does not name the setting that caused it: %v", err)
		}
	}
}

// The threshold is not checked against the merge target. A merge ratio of 1
// still loads, because the window-expiry path can emit a one-message batch even
// at a high ratio, and the per-batch size check is what actually excludes it.
func TestUnmergedTargetIsAllowedAndLeftToThePerBatchCheck(t *testing.T) {
	cfg, err := Load(mapGetenv(baseEnv(map[string]string{
		"DELIVERY_MERGE_RATIO": "1",
	})))
	if err != nil {
		t.Fatalf("Load rejected a merge ratio of 1: %v", err)
	}
	if cfg.Delivery.MergeRatio != 1 || !cfg.Delivery.Compress {
		t.Errorf("MergeRatio = %d, Compress = %v; want 1 and true",
			cfg.Delivery.MergeRatio, cfg.Delivery.Compress)
	}
}

// Disabling compression must stay valid at any threshold.
func TestCompressionDisabledIsAllowedAtAnyThreshold(t *testing.T) {
	cfg, err := Load(mapGetenv(baseEnv(map[string]string{
		"DELIVERY_COMPRESS":           "false",
		"DELIVERY_MERGE_RATIO":        "1",
		"DELIVERY_COMPRESS_MIN_BATCH": "1",
	})))
	if err != nil {
		t.Fatalf("Load rejected disabled compression: %v", err)
	}
	if cfg.Delivery.Compress {
		t.Error("Compress = true, want false")
	}
}

// Coalescing with compression is the supported combination, and must load.
func TestCoalescedDeliveryWithCompressionIsAccepted(t *testing.T) {
	cfg, err := Load(mapGetenv(baseEnv(map[string]string{
		"DELIVERY_MERGE_RATIO": "40",
		"DELIVERY_COMPRESS":    "true",
	})))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Delivery.MergeRatio != 40 || !cfg.Delivery.Compress {
		t.Errorf("MergeRatio = %d, Compress = %v; want 40 and true",
			cfg.Delivery.MergeRatio, cfg.Delivery.Compress)
	}
}
