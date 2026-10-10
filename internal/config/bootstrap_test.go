package config

import (
	"strings"
	"testing"
)

func envWith(extra map[string]string) map[string]string {
	env := map[string]string{"GOMEMLIMIT_BYTES": "8589934592"}
	for k, v := range extra {
		env[k] = v
	}
	return env
}

// The bootstrap settings are all-or-nothing: a tenant without a subject names an
// administrator with no identity, and a subject without a tenant has no scope.
func TestBootstrapSettingsMustBePaired(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"tenant only":  {"BOOTSTRAP_TENANT": "t1"},
		"subject only": {"BOOTSTRAP_SUBJECT": "root"},
	} {
		if _, err := Load(mapGetenv(envWith(env))); err == nil {
			t.Errorf("%s was accepted", name)
		} else if !strings.Contains(err.Error(), "BOOTSTRAP") {
			t.Errorf("%s: error does not name the setting: %v", name, err)
		}
	}

	cfg, err := Load(mapGetenv(envWith(map[string]string{
		"BOOTSTRAP_TENANT": "t1", "BOOTSTRAP_SUBJECT": "root",
	})))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ControlPlane.BootstrapTenant != "t1" || cfg.ControlPlane.BootstrapSubject != "root" {
		t.Errorf("bootstrap = %q/%q, want t1/root",
			cfg.ControlPlane.BootstrapTenant, cfg.ControlPlane.BootstrapSubject)
	}
}

// A typo in the role would otherwise leave an instance that authorizes nobody
// while looking correctly configured.
func TestBootstrapRoleIsCheckedAtLoad(t *testing.T) {
	_, err := Load(mapGetenv(envWith(map[string]string{
		"BOOTSTRAP_TENANT": "t1", "BOOTSTRAP_SUBJECT": "root", "BOOTSTRAP_ROLE": "superuser",
	})))
	if err == nil {
		t.Fatal("an unknown bootstrap role was accepted")
	}
	if !strings.Contains(err.Error(), "BOOTSTRAP_ROLE") {
		t.Errorf("error does not name the role setting: %v", err)
	}
}

// Bootstrap must be off unless asked for, so an ordinary deployment does not
// silently create an administrator.
func TestBootstrapIsOffByDefault(t *testing.T) {
	cfg, err := Load(mapGetenv(envWith(nil)))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ControlPlane.BootstrapSubject != "" || cfg.ControlPlane.BootstrapTenant != "" {
		t.Error("a principal would be created without being configured")
	}
}
