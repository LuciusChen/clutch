# Clutch working guide

Build a maintainable Emacs database client. Functional correctness and passing tests are the baseline; implementation quality also requires clear ownership and low cognitive cost.

## Implementation quality

- Prefer simpler state, data flow and control flow. A refactor should make a concrete improvement; moving code or reducing line count alone is not enough.
- A helper should own a meaningful operation, express a useful domain concept, or centralize a shared rule. Inline pure forwarding and one-use accessor chains when direct code is clearer. Do not split functions by line count or introduce layers to flatten indentation. Named commands, callbacks and protocol adapters can have valid roles even when small.
- Reuse an existing owner or rule before adding another. Share code only when semantics match; keep transaction completion, savepoint recovery, local filtering and server queries distinct.
- Fix failures at the responsible layer. Catch expected errors where input is interpreted, recovery is owned, or resources are cleaned up. Preserve the original failure and diagnostics; do not turn internal errors into empty results, guessed defaults or misleading user errors. Cleanup failures must not conceal the primary failure.
- Add recovery, retries or compatibility handling only for a supported contract or demonstrated failure, with explicit success/failure semantics. Preserve required Emacs and backend compatibility; remove unused internal APIs and speculative shims.
- Keep state with its workflow owner. Avoid wrapper ladders, generic helper modules and file splits that add declarations or cross-file navigation without simplifying ownership.
- Keep tests within the same complexity budget as production code: assert public behavior and meaningful invariants, not helper structure or cosmetic details. Deterministic expected values are normal; choose representative inputs and boundaries rather than adding random cases by default.

## Scope and completion

- For an implementation request, continue through the requested change, relevant verification, review of the diff and necessary documentation. For a review or diagnosis request, report evidence and recommendations without changing behavior.
- Read the affected path and its callers first. Broaden inspection when dependencies, failures or the requested scope justify it. A project-wide audit must cover the project and relevant sibling boundaries; a local fix does not require an unrelated repository tour.
- Ask only when an unresolved choice materially affects behavior, compatibility, scope or external effects. Make reasonable local implementation decisions within the authorized task. Preserve unrelated changes; commit, push, release and changes to a running user environment require task authorization.
- If a fix fails, revise the hypothesis and gather evidence before changing behavior again. Stop stacking speculative patches; resume implementation when evidence supports it. Stop the task only for a concrete blocker, missing authority or an explicitly requested review point.
- Complete authorized local implementation and isolated test iterations without asking at every step, within the active tool permissions. Do not treat arbitrary live credentials or existing databases as disposable fixtures.
- Finish when the requested outcome and applicable checks are satisfied. State what changed, what was verified and any unresolved limitation; passing mocks or skipped live tests are not evidence of real database coverage. Record unrelated debt briefly without expanding the task.

## Project contracts

- Baseline: Emacs 29.1+ and Java 17+ for JDBC. A baseline change requires explicit scope, release metadata, user documentation and a rationale.
- Keep `clutch.el` as the package entry point and assembler. Workflow code uses `clutch-db-*`; native protocol work belongs to the optional external `mysql.el`, `pgsql.el`, `mongodb.el` and `redis.el` packages. `ob-clutch` remains a separate optional package.
- Do not call another package's private API. Missing or too-old optional dependencies must produce a clear connection-time error. Required package metadata belongs only in `clutch.el`; load optional backends when needed.
- MongoDB has one public backend, `:backend mongodb`; SQL Interface uses `:surface sql-interface`. Clutch owns the document console, basic documented helper forms and the adapter, not a JavaScript runtime or mongosh implementation. Keep BSON, wire protocol, URI/SRV interpretation, authentication, sessions, pooling and retries in `mongodb.el`. Pass opaque connection params and use public accessors; user configuration must not expose the internal `:driver mongodb` key.
- Keep connection identity, lifecycle and metadata freshness explicit. Preserve primary/metadata JDBC isolation and do not replay SQL whose execution or transaction outcome is unknown.
- SQL transformations must respect top-level clauses and query semantics. Unsupported rewrites may leave SQL unchanged with appropriate capability limits; do not add guessed WHERE/LIMIT insertion or a full parser for an unrelated fix.
- Preserve NULL, empty, DEFAULT, row identity and transaction-state distinctions. Mutation preview must match execution; validation must retain the user's editing context. Copy/export scope, encoding, atomic file replacement and incomplete-value handling are data contracts.
- Public names use `clutch-`; private names use `clutch--` or existing backend-local private prefixes. Keep existing public faces. Follow local Elisp conventions, Emacs indentation with spaces, lexical binding and hygienic macros; add required Edebug/indent declarations to changed macros.
- Keep loading free of editing side effects; package registrations are allowed. Use stock Emacs facilities, buffer-local mode state and hooks, and cached data for rendering. Add cross-module declarations only where compilation genuinely needs them, not to conceal an architectural dependency.

## Read when relevant

- Changing ownership, dependencies or backend surfaces: [architecture](docs/architecture.md).
- Changing JDBC transport, lifecycle or release integration: [protocol contract](docs/jdbc-agent-protocol.md) and [backend setup](docs/jdbc-backend.org).
- Changing commands, defaults or result workflows: [interactive guide](docs/interactive-client.org).
- Editing package headers, autoloads, Elisp tooling or preparing a code commit/release: [development checks](CONTRIBUTING.md).
- Working on a non-obvious design or previously failed approach: search [postmortem/](postmortem/) for the relevant decision, not the entire history.

## Pre-Commit Checklist

Choose checks by the actual change. During iteration, start with affected tests; before committing code, complete the full non-live gate. Do not repeat a passing gate on the same code and environment unless a new failure or unresolved concern justifies it.

| Change | Required verification |
| --- | --- |
| Documentation or instructions only | Review changed guidance for consistency, resolve referenced paths/anchors and check the diff; no database suite or new product tests. |
| Production code or tests | Focused tests during iteration; `./test/run-ci.sh all` before a code commit. This covers main/backend ERT, byte compilation, package-lint, checkdoc and architecture checks. |
| Query execution, row identity, result workflows, object metadata or native adapters | Also run `./test/run-ci.sh native-live` through the changed workflow. |
| JDBC runtime or adapter behavior | Include the affected JDBC live coverage using an explicitly selected jar and isolated runtime; see the development guide. |

- For a bug fix, reproduce the failure with a focused regression before fixing it, reusing existing coverage where possible. Dispatch bugs need the installed/public path. Behavior-preserving cleanup needs relevant existing tests, not new tests that merely mirror the refactor.
- Pure presentation needs new tests only when it carries a product contract, such as scope, transaction visibility, destructive-action warnings or accessibility. Export data-path changes require content and encoding coverage.
- Review the complete intended diff before committing. Run the applicable dependency/surface checks in the development guide. Do not modify tests or fixtures merely to hide a failure; identify unrelated failures separately.

## Documentation and release

- Update README and the relevant guide for changed commands, defaults or user-visible behavior. Keep current documentation consistent with implementation; do not change product code to justify a documentation claim.
- Add release-relevant changes to the existing version-based `Unreleased` section of CHANGELOG. Internal cleanup, tests and instructions with no product contract change do not need a release note. Commit or merge does not imply release or version bump.
- Preserve the published agent version/checksum pair. Changed published jar bytes require a matching Clutch checksum; prefer a new agent version, and verify the published artifact rather than substituting a local build.
- Record a short design rationale for material workflow changes, non-obvious architecture or compatibility decisions, abandoned approaches and deliberately deferred limitations. Reuse a relevant record for one change; routine refactors, wording and instruction maintenance do not require a new postmortem. Historical records remain historical.
- Keep Markdown/Org paragraphs on one source line. Reference detailed contracts from their canonical document rather than copying inventories into AGENTS.md.
