package sdktest

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Every option the SDK document lists must be one the client actually accepts.
// A documented option that does not exist is a tuning attempt that silently
// does nothing.
func TestDocumentedSDKOptionsExist(t *testing.T) {
	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "sdk.md"))
	if err != nil {
		t.Fatalf("read doc: %v", err)
	}
	dts, err := os.ReadFile(filepath.Join("..", "..", "sdk", "src", "client.ts"))
	if err != nil {
		t.Fatalf("read client: %v", err)
	}
	decl, err := os.ReadFile(filepath.Join("..", "..", "sdk", "src", "protocol.ts"))
	if err != nil {
		t.Fatalf("read protocol: %v", err)
	}
	source := string(dts) + string(decl)

	// The option table's first column.
	row := regexp.MustCompile("(?m)^\\| `([a-zA-Z]+(?:\\.[a-zA-Z]+)?)` \\|")
	found := 0
	for _, m := range row.FindAllStringSubmatch(string(doc), -1) {
		opt := m[1]
		found++
		// A dotted option names a field of a nested object, so only the leaf
		// is checked against the declaration.
		leaf := opt
		if i := strings.LastIndex(opt, "."); i >= 0 {
			leaf = opt[i+1:]
		}
		if !strings.Contains(source, leaf+":") && !strings.Contains(source, leaf+"?:") {
			t.Errorf("SDK option %q is documented but not declared", opt)
		}
	}
	if found == 0 {
		t.Fatal("no options parsed from the document")
	}
	t.Logf("%d documented options, all declared", found)
}

// Every documented callback must be wired in the client.
func TestDocumentedCallbacksAreWired(t *testing.T) {
	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "sdk.md"))
	if err != nil {
		t.Fatalf("read doc: %v", err)
	}
	dts, err := os.ReadFile(filepath.Join("..", "..", "sdk", "src", "client.ts"))
	if err != nil {
		t.Fatalf("read client: %v", err)
	}
	source := string(dts)

	row := regexp.MustCompile("(?m)^\\| `(on[A-Za-z]+)` \\|")
	found := 0
	for _, m := range row.FindAllStringSubmatch(string(doc), -1) {
		cb := m[1]
		found++
		if !strings.Contains(source, cb+"?") && !strings.Contains(source, cb+":") {
			t.Errorf("callback %q is documented but not declared", cb)
		}
	}
	if found == 0 {
		t.Fatal("no callbacks parsed from the document")
	}
	t.Logf("%d documented callbacks, all declared", found)
}
