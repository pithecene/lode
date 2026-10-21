# Contributing to Lode

## Prerequisites

- [mise](https://mise.jdx.dev/), which manages Go, the task runner, and the
  linters

```bash
mise install
```

This command installs:
- Go 1.25.6
- [Task](https://taskfile.dev/) 3.47.0
- [golangci-lint](https://golangci-lint.run/) 2.8.0
- [benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat)

## Development Tasks

| Command | Description |
|---------|-------------|
| `task test` | Run the tests |
| `task test:race` | Run the tests with the race detector |
| `task lint` | Run the linters |
| `task fmt` | Format the code and apply automatic fixes |
| `task build` | Build all packages |
| `task examples` | Run all examples |
| `task snippets` | Verify the runnable Markdown snippets |
| `task bench` | Run all benchmarks (run `s3:up` first for S3) |
| `task bench:stat` | Run the benchmarks with `-count=10` and summarize them with benchstat |
| `task bench:compare` | Compare two files of benchmark results with benchstat |
| `task integration` | Run the integration tests (requires `s3:up`) |

### Integration Tests

```bash
task s3:up          # start LocalStack and MinIO
task integration    # run all integration tests
task s3:down        # stop the services
```

## Code Style

- Formatting is automated (`gofumpt` and `goimports`) through `task fmt`.
- Use `t.Context()` or `b.Context()` instead of `context.Background()` in
  tests.
- Compare errors with `errors.Is()`, not `==`.
- Prefer `any` to `interface{}`.
- Prefer early returns to nesting.
- See `AGENTS.md` for the guardrails that apply to AI agents.

## Documentation Style

Prose in documentation, contracts, comments, commit bodies, and pull
requests follows *The Chicago Manual of Style*:

- Write complete sentences in a formal, neutral register, in the active
  voice by default.
- Prefer precise verbs to vague phrasal ones ("persist" rather than "write
  out", "retain" rather than "keep") and avoid metaphor ("pending" rather
  than "in flight").
- Define a term before relying on it, and keep table cells and list items
  parallel and self-contained.
- Use the serial comma and headline-style capitalization for headings.
- Keep normative statements (MUST, SHOULD, MAY) in the contracts; guides
  explain them without adding requirements.

## Commits

Use [Conventional Commits](https://www.conventionalcommits.org/), with a
[gitmoji](https://gitmoji.dev/) emoji after the scope:

```
type(scope): <emoji> short imperative description
```

Examples:

```
feat(dataset): ✨ add snapshot metadata filtering
fix(storage): 🐛 handle empty manifest on first read
docs(api): 📝 clarify write API codec requirements
refactor(layout): ♻️ extract hive partition logic
```

## Pull Requests

- Branch from `main`; never commit directly to `main`.
- PR titles follow the same format as commit messages.
- Keep each PR focused on a single concern.

## Architecture

- `AGENTS.md`: guardrails for AI agents
- `docs/ARCH_INDEX.md`: navigation of subsystems
- `docs/contracts/CONTRACT_*.md`: normative behavior contracts
- `PUBLIC_API.md`: the public API specification

Contracts are authoritative over code. When in doubt, consult the
contracts first.

## License

By contributing, you agree that your contributions will be licensed under
the Apache License 2.0.
