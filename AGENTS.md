# AGENTS.md — Lode Guardrails

This file defines **non-negotiable guardrails** for working on Lode.
It encodes *discipline and constraints*, not architecture.

---

## Core Principles

- Prefer **clarity over cleverness**
- Favor **explicit behavior** over implicit magic
- Keep abstractions **shallow and inspectable**
- Optimize for **correctness and debuggability**, not elegance

---

## Core constraint

Lode abstracts **persistence structure**, not execution.

The system defines:
- datasets
- snapshots
- manifests
- metadata
- safe write semantics

The system does NOT define:
- query planning
- execution strategies
- scheduling
- optimization heuristics
- distributed coordination

Do not introduce features that cross this boundary.

---

## Scope Discipline

Agents must not:
- Invent new features unless explicitly requested
- Redesign core abstractions unprompted
- Introduce DSLs, frameworks, or configuration layers
- Optimize for scale or performance without evidence

If scope feels ambiguous or expanding, **pause and ask**.

---

## Change Discipline

- API changes are expensive; internal refactors are cheap
- Behavior changes must be observable
- Avoid silent fallbacks, hidden retries, or implicit recovery

---

## Public API boundaries

- Public APIs live in `/lode`
- The public surface should remain intentionally scoped and coherent
- Adding a public method or type is allowed when it clearly improves usability or expressiveness
- Prefer internal implementations over public extensibility

If functionality can live in `/internal`, it must live there.

---

## Abstraction discipline

Agents must not expand abstractions to accommodate:
- backend-specific quirks
- performance optimizations
- convenience behaviors
- conditional logic for edge cases

Differences between backends must be handled by composition, not flags.

---

## Structural Rules

- Small, single-purpose modules
- No premature generalization
- No "utility" dumping grounds
- Separate concerns explicitly:
  - storage vs codec
  - layout vs partitioning
  - read path vs write path

---

## Contract rules

- A contract (`docs/contracts/CONTRACT_*.md`) states invariants only
- A contract holds no status, progress, plans, resolved questions, or deferred work
- Status belongs in `docs/plans/V1_READINESS.md` or `CHANGELOG.md`, and history in git, pull requests, and issues

---

## Metadata rules

- Metadata must be explicit
- Metadata must be persisted
- Metadata must never be inferred implicitly
- Snapshots and manifests must be self-describing

---

## Immutability rules

- Data files are immutable once written
- Snapshots are immutable
- Commits create new state; they do not mutate existing state

Any design that violates immutability is invalid.

---

## Allowed additions

Agents may add:
- storage adapters
- codecs
- compression implementations
- partitioning strategies
- examples

Provided they respect existing invariants.

---

## Disallowed additions

Agents must not add:
- background workers
- automatic compaction
- query execution logic
- hidden global state
- convenience APIs that bypass snapshots

---

## When uncertain

If a change increases:
- API surface area
- configuration options
- conditional behavior

Assume it is invalid and stop.

---

## Testing conventions

- Use `t.Context()` / `b.Context()` instead of `context.Background()` in tests and benchmarks
- Use `errors.Is()` for error comparisons, not `==`

---

## Go Rules

- Prefer `any` over `interface{}`
- Exceptions: when implementing interfaces from external packages that use `interface{}`
- Prefer `errors.New` over `fmt.Errorf` when no formatting verbs are needed
- Prefer iterators (`range`/`yield`-based or `iter.Seq`) over building intermediate slices, where appropriate
- Use `//nolint:` comments with explanation when suppressing linters

---

## Code Style & Composition

Code in this repository should prefer declarative patterns over imperative control flow.

### Preferred patterns

- **Data-first shapes**: Define behavior via data structures (tables, maps, slices) rather than branching logic.
- **Table-driven logic**: Use lookup tables for type dispatch, validation rules, and error mapping.
- **Iterators and helpers**: Extract iteration patterns into reusable helpers; avoid inline loops with complex bodies.
- **Named helpers**: Extract small, single-purpose functions with clear names instead of inline blocks.
- **Explicit validation passes**: Validate inputs in a dedicated pass before processing, not scattered inline.
- **Extract when a pattern repeats ≥ 3 times**: error formatting, validation, resource cleanup, etc.
- **Group code at similar abstraction levels**: high-level orchestration should not inline low-level mechanics.
- **Wrap mutable or chainable behavior**: use return structs or factory functions so callers stay declarative.
- **One level of nesting per function**: if a function has nested if/else, consider extracting the inner block.
- **Helpers are file-local unless shared**: do not export helpers that serve only one call site.

### Anti-patterns to avoid

- Long switch/if chains that could be a map lookup
- Deep nesting (prefer ≤2 levels unless it improves clarity)
- Inline anonymous functions with complex logic
- Mixing validation, transformation, and I/O in a single function
- Repeated patterns that should be a helper

### Refactoring rules

Style refactors are allowed when:
- Behavior is unchanged (tests remain green without modification)
- Changes are localized (one file or small group of related files)
- The PR is clearly marked "refactor only"

Style refactors must NOT:
- Change API surface
- Modify error messages (may break error matching)
- Introduce new abstractions or types
- Combine with feature or bugfix changes

If a style change starts to feel like a design change, **stop and ask**.

---

## Control Flow

- Early returns over nesting
- No implicit mutation unless justified
- Pure functions where practical (not a hard requirement)
- Errors must be handled or propagated explicitly

---

## Errors

- Prefer sentinel error values (type-based) for classification
- Use `errors.Is()` / `errors.As()` for assertions, not string matching
- Wrap errors with `fmt.Errorf("context: %w", err)` to preserve the chain
- Never discard errors silently

---

## Formatting & Comments

- Formatting is automated (`gofumpt` + `goimports`); do not hand-format
- Comments explain **why**, not **what**
- No commented-out code
- All exported types and functions must have doc comments

---

## API and example conventions

- `PUBLIC_API.md` is canonical for callsite conventions
- Example code must not drift from `PUBLIC_API.md`
- Write examples prefer `lode.R(...)` for inline record construction
- `[]lode.D` is allowed only when backing an iterator (add comment explaining why)
- Pre-v1 ergonomics changes require contract+docs+tests+examples updated together

If examples diverge from these conventions, stop and ask before editing.

See `docs/contracts/CONTRACT_EXAMPLES.md` for full normative specification.

---

## Priority order

1. Correctness
2. Clarity
3. Explicitness
4. Convenience
5. Extensibility

---

## Litmus Test

Before adding code, ask:

> Does this make the system easier to reason about for a future reader?

If not, reconsider.

---

## Agent Implementation Procedure

When given a task:

1. Read this file (`AGENTS.md`) in full.
2. Read only the files explicitly referenced by the task.
3. Do not infer architecture beyond what is visible in code.
4. Modify only files within the stated scope.
5. Do not introduce new dependencies unless explicitly requested.
6. Preserve existing public APIs unless the task explicitly permits changes.
7. Make all behavior changes observable.
8. Follow Go Rules strictly.
9. If an instruction is ambiguous, stop and ask before writing code.
10. If a change feels like scope expansion, stop and surface the concern.
11. Do not refactor unrelated code "for cleanliness."
12. Output only the requested artifacts (code, diffs, or explanations).
