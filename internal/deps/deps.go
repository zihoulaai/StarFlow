// Package deps pins the external modules the architecture depends on.
//
// The design in design.md names five external capabilities: a WebSocket
// transport, an Aho-Corasick matcher, a message bus client, a relational
// driver, and a metrics exporter. This file imports each of them so the module
// graph is fixed from the first commit: `go mod tidy` will not drop a
// dependency that an upcoming implementation task still needs, and the exact
// versions the service was designed against are reviewable in isolation.
//
// Each import below is blank on purpose. None of these symbols is called from
// this package; they are pinned here and used from the packages that own the
// corresponding concern:
//
//	websocket    internal/gateway  (subscriber and publish transports)
//	ahocorasick  internal/moderation (in-memory sensitive word automaton)
//	go-redis     internal/bus      (message bus client)
//	sqlite       internal/controlplane (driver, used directly by tests and by
//	                                    single-node development deployments)
//	prometheus   internal/observability (metrics export)
package deps

import (
	_ "github.com/cloudflare/ahocorasick"
	_ "github.com/gorilla/websocket"
	_ "github.com/prometheus/client_golang/prometheus"
	_ "github.com/redis/go-redis/v9"
	_ "modernc.org/sqlite"
)
