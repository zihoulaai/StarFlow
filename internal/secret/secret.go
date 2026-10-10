// Package secret reads signing material from a secret source.
//
// The material never comes from image contents, command-line arguments, or a
// file committed to the repository. An argument in particular is a bad habit
// worth forbidding explicitly: it is visible in the process table to every other
// process on the host and is recorded verbatim in the image history.
package secret

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
)

// ErrEmpty is returned when a source resolves to no material.
var ErrEmpty = errors.New("secret: signing material is empty")

// Source resolves signing material.
type Source interface {
	// Load returns the material, and may be called again after a rotation.
	Load() ([]byte, error)
	// Name identifies the source for diagnostics.
	Name() string
}

// EnvSource reads a named environment variable.
type EnvSource struct {
	Variable string
	once     sync.Once
	cached   []byte
	err      error
}

// NewEnv returns an environment-backed source.
func NewEnv(variable string) *EnvSource { return &EnvSource{Variable: variable} }

// Load reads the variable.
func (s *EnvSource) Load() ([]byte, error) {
	s.once.Do(func() {
		v := strings.TrimSpace(os.Getenv(s.Variable))
		if v == "" {
			s.err = fmt.Errorf("%w: %s", ErrEmpty, s.Variable)
			return
		}
		s.cached = []byte(v)
	})
	return s.cached, s.err
}

// Name identifies the source.
func (s *EnvSource) Name() string { return "env:" + s.Variable }

// FileSource reads a mounted secret file.
//
// This is the shape most orchestrators provide, and it is why rotation is a
// non-event: the material changes on disk and the next reload picks it up,
// without the process needing to be replaced and without the value ever being in
// an argument or an image layer.
type FileSource struct {
	Path string

	mu     sync.Mutex
	cached []byte
}

// NewFile returns a file-backed source.
func NewFile(path string) *FileSource { return &FileSource{Path: path} }

// Load reads the file.
func (s *FileSource) Load() ([]byte, error) {
	b, err := os.ReadFile(s.Path)
	if err != nil {
		return nil, fmt.Errorf("secret: read %s: %w", s.Path, err)
	}
	b = []byte(strings.TrimSpace(string(b)))
	if len(b) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrEmpty, s.Path)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.cached = b
	return b, nil
}

// Name identifies the source.
func (s *FileSource) Name() string { return "file:" + s.Path }

// New returns the source named by kind.
func New(kind, ref string) (Source, error) {
	switch kind {
	case "env":
		return NewEnv(ref), nil
	case "file":
		return NewFile(ref), nil
	default:
		return nil, fmt.Errorf("secret: unknown source kind %q", kind)
	}
}

// Static returns a fixed source, for tests and single-node development.
//
// It is deliberately not reachable from configuration in a deployed gateway: a
// configured static secret is a committed secret.
func Static(material string) Source { return staticSource{material: []byte(material)} }

type staticSource struct{ material []byte }

func (s staticSource) Load() ([]byte, error) {
	if len(s.material) == 0 {
		return nil, ErrEmpty
	}
	return s.material, nil
}

func (s staticSource) Name() string { return "static" }
