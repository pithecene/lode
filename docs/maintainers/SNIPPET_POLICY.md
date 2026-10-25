# SNIPPET_POLICY.md — Markdown Code Fence Conventions

This document defines conventions for code snippets in Lode documentation.

---

## Snippet Categories

### Runnable Snippets

Snippets that can be executed directly or extracted and compiled.

**Markers:**
- Fenced with language identifier: ` ```go `, ` ```bash `
- Contains complete, self-contained code OR references a runnable example
- Annotated with `<!-- runnable -->` comment if verification is required

**Requirements:**
- Must compile (Go) or execute (bash) without modification
- Import paths must be valid
- No placeholder values (e.g., `...`, `<your-value>`)

**Verification:**
- CI job extracts and validates runnable snippets
- Failures block merge

**Verification Strictness (Known Trade-offs):**

The snippet verifier (`scripts/verify-snippets.sh`) has intentionally permissive Go validation:

- Go snippets are wrapped with injected imports (`context`, `fmt`, `lode`) before compilation
- This may produce **false positives**: snippets that pass CI but aren't truly self-contained
- Bash snippets use `bash -n` syntax checking only (not execution)

**Why permissive?**
- Current runnable snippet set is mostly bash (see inventory table below)
- Low complexity keeps the verifier maintainable
- Full Go execution would require careful sandboxing

**Authoritative runtime verification:**
- `examples/*/main.go` files are the authoritative source for complete, runnable code
- `task examples` verifies these at runtime
- When in doubt, add runnable code to an example, not to docs

**When to revisit:**
- If runnable Go snippets become common in docs
- If false positives cause user confusion
- Consider: extracting snippets to temp files with full `go run` verification

### Illustrative Snippets

Snippets that demonstrate API patterns but are not independently runnable.

**Markers:**
- Fenced with language identifier: ` ```go `, ` ```bash `
- Annotated with `<!-- illustrative -->` comment
- May contain:
  - Partial code (no `package`/`func main()`)
  - Elided imports or context
  - Placeholder values (`...`, `// ...`)

**Requirements:**
- Must be syntactically plausible
- Should align with actual API signatures
- Not verified by CI

---

## Annotation Format

Place HTML comments immediately before the code fence:

```markdown
<!-- illustrative -->
```go
ds, _ := lode.Open(lode.FS("/data")).Blobs(ctx, "mydata")
```

<!-- runnable -->
```bash
go get github.com/pithecene/lode
```
```

If no annotation is present, the snippet is assumed **illustrative** by default.

---

## File-Specific Conventions

| File | Default | Notes |
|------|---------|-------|
| `README.md` | Illustrative | Quick Start snippets are illustrative patterns |
| `PUBLIC_API.md` | Illustrative | API examples are patterns, not runnable programs |
| `examples/*/README.md` | Illustrative | Prose guidance; runnable code is in `main.go` |
| `examples/*/main.go` | Runnable | Full programs; verified by `task examples` |

---

## Inventory

### README.md

| Lines | Language | Category | Description |
|-------|----------|----------|-------------|
| 56-58 | bash | Runnable | `go get` install command |
| 63-86 | go | Illustrative | Write and read blob pattern |
| 91-103 | go | Illustrative | Write structured records pattern |
| 108-115 | go | Illustrative | StreamWrite pattern |
| 230-233 | bash | Illustrative | mkdir for filesystem setup |
| 236-239 | go | Illustrative | Filesystem dataset construction |
| 243-246 | bash | Illustrative | S3 bucket creation |
| 249-253 | go | Illustrative | S3 dataset construction |
| 260-268 | go | Illustrative | Bootstrap helper pattern |

### PUBLIC_API.md

| Lines | Language | Category | Description |
|-------|----------|----------|-------------|
| 34-44 | go | Illustrative | Root, Records, and view construction |
| 54-63 | go | Illustrative | Path name options |
| 141-145 | go | Illustrative | Timestamped interface definition |
| 152-163 | go | Illustrative | Timestamped implementation example |
| 254-262 | go | Illustrative | Bootstrap helper pattern |
| 341-346 | go | Illustrative | errors.Is usage pattern |
| 392-407 | go | Illustrative | AWS S3 client setup (Storage Prerequisites) |
| 412-434 | go | Illustrative | LocalStack client setup (Storage Prerequisites) |
| 438-451 | go | Illustrative | MinIO client setup (Storage Prerequisites) |
| 455-467 | go | Illustrative | Cloudflare R2 client setup (Storage Prerequisites) |
| 576-581 | go | Illustrative | errors.Is sentinel check pattern |
| 634-649 | go | Illustrative | AWS S3 client setup (S3 Adapter) |
| 654-676 | go | Illustrative | LocalStack client setup (S3 Adapter) |
| 681-694 | go | Illustrative | MinIO client setup (S3 Adapter) |
| 699-711 | go | Illustrative | Cloudflare R2 client setup (S3 Adapter) |

### examples/blob_upload/README.md

No code fences.

---

## Maintenance

When adding new snippets:
1. Determine if the snippet is runnable or illustrative
2. Add the appropriate annotation comment
3. Update the inventory table in this document
4. If runnable, ensure CI verification passes

---

## References

- Runnable examples: `examples/*/main.go` (verified by `task examples`)
- Contract specifications: `docs/contracts/`
