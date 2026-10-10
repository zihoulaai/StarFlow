# CLAUDE.md

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

---

# Go Best Practices

Condensed from [Google Go Style Guide: Best practices](https://google.github.io/styleguide/go/best-practices). Read the source when a rule needs justification.

## Naming

**Avoid repetition** - at the call site most names already have context:
- Omit input/output types, receiver type, and pointer-ness from names.
- Don't repeat the package name: `yamlconfig.Parse(input)`, not `ParseYAMLConfig`.
- Don't repeat the receiver name: `func (c *Config) WriteTo(...)`, not `WriteConfigTo`.
- Don't repeat parameter names or return types: `Override(dest, source *Config)`, not `OverrideFirstWithSecond`.
- Exception: add the extra info when disambiguating similar functions - `WriteTextTo` vs `WriteBinaryTo`.

**Conventions:**
- Functions that return something get noun-like names; **avoid the `Get` prefix** - `JobName(key)`, not `GetJobName`.
- Functions that do something get verb-like names - `WriteDetail(w)`.
- Same function differing only by type suffixes the type: `ParseInt` / `ParseInt64`.
- One clear "primary" version omits the type: `Marshal()` / `MarshalText()`.

**Test doubles** (stub / fake / mock / spy):
- Test helper package = production package name + `test` suffix - `package creditcardtest`.
- One type, one behavior → `creditcardtest.Stub`.
- One type, many behaviors → name after the behavior - `AlwaysCharges`, `AlwaysDeclines`.
- Many types → `StubService`, `StubStoredValue`.
- Test-local variables for doubles get a prefix to disambiguate from production types - `spyCC`, not `cc`.

**Shadowing vs stomping:**
- Stomping (`:=` reusing an existing var in the same scope) is fine when the old value is gone.
- Shadowing (`:=` in a *new* scope) silently discards the outer variable - the classic `ctx` inside an `if`. Use `var cancel func()` + plain `=` instead.
- Intentional shadowing is allowed when it's clearer.
- Don't name variables after std packages outside tiny scopes (`url` shadows `net/url`). Pick package names that don't force import renaming.

**No `util` / `helper` / `common` packages.** Judge a package name by the callsite: `spannertest.NewDatabaseFromFile(...)`, not `test.NewDatabaseFromFile(...)`. Names like `SeekStart` belong in a specific package (`io.SeekStart`), not `common.SeekStart`.

## Package size

- Same package when client code needs the types to interact, or when implementations are tightly coupled. Test: if a user must import both packages to use either meaningfully, combine them.
- Conceptually distinct code gets its own small package - `bytes.Buffer`, `ring.New`.
- Don't put a whole project in one package. File size is flexible - no "one type, one file" rule; avoid both thousands-of-lines files and dozens of tiny files. `doc.go` may hold package docs alone.
- Proto imports: `pb` suffix for `go_proto_library`, `grpc` for `go_grpc_library`. Prefer descriptive whole words - `pushqueueservicepb`, not `xpb`.

## Error handling

**Give errors structure** so callers can branch programmatically:
- Simplest form: unparameterized sentinel globals compared with `==`, or `errors.Is` when wrapped.
- **Never** make callers match on error strings.
- Extra data callers need programmatically should be a struct field (like `os.PathError.Path`).
- At RPC/IPC/storage boundaries, translate to canonical status codes (`status.Errorf(codes.Internal, ...)`) rather than leaking raw internal errors.

**Adding context to errors:**
- Never repeat information the underlying error already carries (don't re-append the path from `os.Open`).
- Don't annotate when the only purpose is to signal failure - `fmt.Errorf("failed: %v", err)` should just be `return err`.
- `%v` for: non-redundant context, log/display output, and fresh independent errors at system boundaries.
- `%w` for: adding context while preserving the chain for `errors.Is`/`errors.As`, and for wrapping that's an explicitly documented part of the package contract.

**`%w` placement:**
- Prefer `%w` at the **end** (`"...: %w"`) so printed text mirrors the newest-to-oldest chain.
- **Exception:** wrap a *sentinel/category* error with `%w` **first** - `fmt.Errorf("%w: invalid header", ErrParse)` - so the category is visible immediately.

**Logging:**
- Say what went wrong plus what's needed to diagnose - like a good test failure message.
- If you return the error, let the caller log it. Don't log and return.
- Watch for PII; not every sink is a safe destination.
- Use error level sparingly - it forces a flush. Error level means "actionable", not "worse than warning".
- Guard expensive calls in verbose logs: `if log.V(2) { log.Infof(...) }`, never `log.V(2).Infof("%v", sql.Explain())`.

## Panics

- Libraries return errors. Don't abort the process, especially for transient failures.
- Unrecoverable invariant violations → `log.Fatal`, not `panic` - deferred functions may deadlock or corrupt state.
- Don't `recover` to dodge a crash. You can't know the state (locks, resources) at that distance; fix the bug and alert instead.
- Panics are acceptable as an internal package implementation detail **only** with a matching `recover` at the public API boundary. Such panics must never escape the package. Re-panic foreign panics you didn't raise.
- Add `panic("unreachable")` after a non-returning call like `log.Fatalf` so the compiler is satisfied.
- Don't call `log` before flags are parsed; inside `init`/`must` functions panic instead.

## Documentation

- Runnable `Example*` functions are the best usage docs - they render in godoc.
- Don't enumerate every parameter. Document only error-prone / non-obvious things, and say *why* they matter.
- Context: cancellation returning `ctx.Err()` is implied - don't restate it. Document explicitly when you return something else on cancel, when another mechanism (`Stop()`) interrupts the function, or when the function has unusual expectations about the context's deadline, lineage, or values.
- Concurrency: read-only methods are assumed safe and mutating ones assumed unsafe - don't add redundant notes. Document when it's genuinely unclear whether an operation mutates, when the API itself provides synchronization (document on the type, not each method), or when you consume user-implemented interfaces.
- Always document cleanup requirements. Point at the method when obvious ("Call Stop to release resources"); show a snippet when not (`defer resp.Body.Close()`).
- Document sentinel errors and error types callers can handle, and whether the returned error is a pointer receiver.
- Godoc: blank line between paragraphs; two-space indent renders verbatim; a lone capitalized line with no punctuation becomes a heading.

## Variables

- Prefer `:=` over `var` for non-zero values.
- Use zero-value declaration when you want an empty value ready for later use - `var coords Point` before `json.Unmarshal(data, &coords)`.
- Use composite literals only when you know the initial members. Don't write `Point{X:0, Y:0}`.
- Size hints: preallocate when the size is known and measured, and comment why - `make([]byte, 131072)`. Don't guess; over-allocation wastes memory.
- Maps must be explicitly initialized before modification. Reading a zero-value map is fine.
- **Always specify channel direction** where possible: `func sum(values <-chan int) int`.

## Function arguments

Signatures shouldn't sprawl - roles get unclear and same-typed params get swapped.

**Option struct** - use when most callers pass several options, options are shared across functions, or you want self-documenting keyed literals:
- Never put `context.Context` in an option struct.

**Variadic options** - use when most callers pass nothing, there are many options, options take arguments, can fail, or need heavy docs:
- Options should take parameters rather than signal by presence - `rpc.FailFast(enable bool)`, not `rpc.EnableFailFast()`.
- Process in order; last argument wins on conflict.
- Keep the option func's parameter unexported to limit definitions to the package.

## Interfaces

- **Don't create an interface before a real need exists.** Most common mistake. A "service" or "repository" doesn't require a named `type Service interface`.
- Reuse existing interfaces, especially generated ones. Don't hand-wrap generated RPC clients for testability - use real transports instead.
- Don't define test-only back doors. Export a test double only when there's real substitution need.
- The **consumer** defines the interface, and defines only the methods it uses. Don't export interfaces used only internally.
- The producer may export one when the interface *is* the product (`io.Writer`, generated protobuf interfaces), to prevent duplicated `Authorizer` interfaces, or to break a dependency cycle.
- Keep interfaces small. Every interface is a user manual - document contracts, edge cases, and errors, not just signatures.
- **Accept interfaces, return concrete types.**
- Returning an interface is idiomatic for: encapsulation (limiting the default API surface), patterns with inherent runtime selection (factory/command/strategy), and breaking dependency cycles.

## Global state

- Don't expose package-level state that controls behavior for all clients. Let clients construct and pass instances explicitly.
- Global state breaks test hermeticity, parallel tests, sharding, and filtering. Approach it with extreme scrutiny; legacy stdlib counters are not precedent.
- Watch for: exported/unexported top-level variables, service locators, callback registries, and thick-client singletons.
- Safe only when: the state is logically constant, the observable behavior is stateless (private cache), or the state doesn't bleed outside the process.
- A default instance (`http.DefaultServeMux`) is acceptable **only** if the package also exposes instance APIs, the global is a thin proxy, it's used by binaries rather than libraries, and it documents its invariants and offers a reset.

## Tests

- **Leave failing to the `Test` function.** Assertion helpers that call `t.Error` are not idiomatic Go. Inline the check, use a table-driven test with the logic in the loop, or have shared validation return an `error` and let the test decide how to fail.
- Test *helpers* (setup/cleanup) do call `t.Fatal` - the test can't continue - and must call `t.Helper()` and include enough context. If nothing in a helper can fail the test, drop `t` from its signature.
- Use real transports: connect the production client to a test double of the server rather than hand-rolling a client.
- Prefer `t.Error` and keep going. Use `t.Fatal` only when the test genuinely cannot proceed (setup failures, before a table-driven loop). Inside `t.Run`, `t.Fatal` ends just that subtest.
- **Never call `t.Fatal` from a goroutine other than the test's** - it calls `FailNow`, which only works on the test's goroutine. Use `t.Errorf` + `return`. `t.Parallel` does not make it safe.
- Prefer keyed struct literals for table-driven cases, especially when cases span many lines or adjacent fields share a type.
- Scope setup narrowly - call `mustLoadDataset(t)` from only the tests that need it. Never do expensive setup in `init()` or package vars.
- Use `sync.Once` for expensive setup that needs no teardown (store the data *and* the error so later callers can still fail). Don't use it for setup that depends on a context.
- Use `TestMain` only when *all* tests need common setup *and* it requires teardown. Put the logic in `runMain(ctx, m)` so defers work, then `os.Exit` in `TestMain`.
- Tests should be hermetic - reset any global state a test modifies.

## String building

- `+` for a few simple concatenations - simplest, no import.
- `fmt.Sprintf` for complex formatting; write `fmt.Fprintf(w, ...)` directly to an `io.Writer` instead of building a temporary string.
- `strings.Builder` when assembling a string piecemeal - it's amortized linear, whereas repeated `+`/`Sprintf` is quadratic.
- Use backtick literals for constant multi-line strings, not `"" + "..."` chains.
- For anything more complex, reach for `text/template` (or `safehtml/template` for HTML).
