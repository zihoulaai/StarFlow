package controlplane

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
)

// These wrappers exist so the packages that need randomness and constant-time
// comparison import them from one place, and so a future switch to a
// deterministic source in tests has a single seam to change.

func randRead(b []byte) (int, error) { return rand.Read(b) }

func hexEncode(b []byte) string { return hex.EncodeToString(b) }

// subtleEqual compares two digests in constant time.
//
// Secret digests are compared here, so a byte-by-byte early-exit comparison
// would leak how much of a guessed digest was correct. Values of differing
// length cannot be a correct comparison anyway, and ConstantTimeCompare already
// handles that case without an early exit.
func subtleEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
