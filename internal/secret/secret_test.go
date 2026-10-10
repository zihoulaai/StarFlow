package secret

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnvSource(t *testing.T) {
	t.Setenv("PUSH_SIGNING_KEY", "material")
	s := NewEnv("PUSH_SIGNING_KEY")
	got, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if string(got) != "material" {
		t.Errorf("material = %q", got)
	}
}

func TestEnvSourceRejectsEmpty(t *testing.T) {
	t.Setenv("PUSH_SIGNING_KEY", "   ")
	s := NewEnv("PUSH_SIGNING_KEY")
	if _, err := s.Load(); !errors.Is(err, ErrEmpty) {
		t.Errorf("err = %v, want ErrEmpty", err)
	}
}

func TestFileSource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("file-material\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := NewFile(path).Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if string(got) != "file-material" {
		t.Errorf("material = %q, want it trimmed", got)
	}
}

// A mounted secret changing on disk must be picked up without replacing the
// process. That is what makes rotation a non-event.
func TestFileSourcePicksUpRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("first"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	s := NewFile(path)

	first, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := os.WriteFile(path, []byte("second"), 0o600); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	second, err := s.Load()
	if err != nil {
		t.Fatalf("Load after rotation: %v", err)
	}

	if string(first) == string(second) {
		t.Fatal("rotation was not observed")
	}
	if string(second) != "second" {
		t.Errorf("material after rotation = %q", second)
	}
}

func TestFileSourceRejectsEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := NewFile(path).Load(); !errors.Is(err, ErrEmpty) {
		t.Errorf("err = %v, want ErrEmpty", err)
	}
}

func TestFileSourceReportsMissingFile(t *testing.T) {
	if _, err := NewFile(filepath.Join(t.TempDir(), "absent")).Load(); err == nil {
		t.Error("a missing secret file loaded without error")
	}
}

// The source kind comes from configuration, so an unknown value must be
// refused rather than silently falling back to something.
func TestUnknownSourceKindIsRefused(t *testing.T) {
	for _, kind := range []string{"arg", "literal", "", "kubernetes"} {
		if _, err := New(kind, "ref"); err == nil {
			t.Errorf("source kind %q was accepted", kind)
		}
	}
}

// A name that ends up in a log or a probe response must never carry the
// material itself.
func TestSourceNamesDoNotLeakMaterial(t *testing.T) {
	for _, s := range []Source{
		NewEnv("PUSH_SIGNING_KEY"),
		NewFile("/run/secrets/token"),
		Static("material"),
	} {
		if strings.Contains(s.Name(), "material") {
			t.Errorf("source name %q contains the material", s.Name())
		}
	}
}

func TestStaticSource(t *testing.T) {
	got, err := Static("m").Load()
	if err != nil || string(got) != "m" {
		t.Errorf("Load = %q, %v", got, err)
	}
	if _, err := Static("").Load(); !errors.Is(err, ErrEmpty) {
		t.Errorf("err = %v, want ErrEmpty", err)
	}
}

// Every source resolves material the same way, which is what lets the caller
// treat them interchangeably.
func TestSourcesShareOneContract(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("m"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	t.Setenv("PUSH_SIGNING_KEY", "m")

	for _, s := range []Source{NewEnv("PUSH_SIGNING_KEY"), NewFile(path), Static("m")} {
		got, err := s.Load()
		if err != nil || string(got) != "m" {
			t.Errorf("%s: Load = %q, %v", s.Name(), got, err)
		}
	}
}
