# Clutch working guide

Build a maintainable Emacs database client. Functional correctness and passing tests are the baseline; implementation quality also requires clear ownership and low cognitive cost.

## Implementation quality

- Prefer simpler state, data flow and control flow. A refactor should make a concrete improvement; moving code or reducing line count alone is not enough.
- A helper should own a meaningful operation, express a useful domain concept, or centralize a shared rule. Inline pure forwarding and one-use accessor chains when direct code is clearer. Do not split functions by line count or introduce layers to flatten indentation. Named commands, callbacks and protocol adapters can have valid roles even when small.
- Reuse an existing owner or rule before adding another. Share code only when semantics match; keep transaction completion, savepoint recovery, local filtering and server queries distinct.
- Keep copies of one mechanism in step: a fix to one copy reaches the others, or its commit says why not. Extract the shared part when a third copy would appear, not before.
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
- A reply to SQL that runs without blocking goes through `clutch--query-activity-reply`, which lets it count only while its activity's buffer is live and holds the connection it came from. That includes a reply a synchronous backend delivers while the SQL is being dispatched, because its network wait runs timers that can kill or move the buffer. A new flow that runs SQL this way does the same, and its tests cover a killed buffer, a buffer that moved to another connection, a cancel that met the reply and a handler that exits nonlocally.
- The parameters a namespace switch records lead a new connection back there, over a `:url` too and without changing where it authenticates, as `clutch-test-live-console-params-lead-back-to-a-switched-namespace` checks for each SQL backend with a switch, and `clutch-db-test-mongodb-live-switched-params-lead-back-there` for MongoDB. Where no new connection can return, `clutch-db-unreachable-namespace` says so, and the automatic reconnect refuses.
- A new connection takes over a session through `clutch--replace-connection`, which keeps the session's commit mode, moves every buffer attached to the old connection and marks the results of work lost with it; the automatic reconnect, reopening a console, a schema switch that reconnects and `C-c C-e` in a console all go through it, `C-c C-e` only while the parameters it reads again, `:password` and `:pass-entry` aside, are those the session connected with (`clutch--console-target`), so a session's results never move to another target. An open console is named by the parameters it was opened with, which do not follow its session.
- A result command that builds SQL or loads metadata through the result's connection calls `clutch-result--require-connection` first, since a result whose session ended has none.
- A command that stages or submits a change through a result, or loads more of its query's rows, such as a page, a count or an export, calls `clutch-result--refuse-if-moved` first; running the query again, as `g` and a server-side filter do, records where it ran instead. A backend that resolves an unqualified name through more than its database and current schema overrides `clutch-db-resolution-context`.
- SQL transformations must respect top-level clauses and query semantics. Unsupported rewrites may leave SQL unchanged with appropriate capability limits; do not add guessed WHERE/LIMIT insertion or a full parser for an unrelated fix.
- Preserve NULL, empty, DEFAULT, row identity and transaction-state distinctions. Mutation preview must match execution; validation must retain the user's editing context. Copy/export scope, encoding, atomic file replacement and incomplete-value handling are data contracts.
- Where a backend reports whether a transaction is open (`clutch-db-transaction-open-p`), SQL that writes may mark the work uncommitted, and only that report, SQL that ends the transaction and chains another, or a commit or rollback that Clutch runs, clears it. `uncertain` is cleared only by an explicit rollback or reconnect.
- Public names use `clutch-`; private names use `clutch--` or existing backend-local private prefixes. Keep existing public faces. Follow local Elisp conventions, Emacs indentation with spaces, lexical binding and hygienic macros; add required Edebug/indent declarations to changed macros.
- Keep loading free of editing side effects; package registrations are allowed. Use stock Emacs facilities, buffer-local mode state and hooks, and cached data for rendering. Add cross-module declarations only where compilation genuinely needs them, not to conceal an architectural dependency.
- Raise the cross-module declaration baseline in `test/check-architecture.el` only for a declaration that compilation needs on an existing boundary, giving the reason in the commit message; lower it when declarations go away.

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
| Material workflow, architecture or compatibility decision | A design record in `postmortem/NNN-slug.md`, using the next unused number, in the same commit; see Documentation and release for what it covers. |

- For a bug fix, reproduce the failure with a focused regression before fixing it, reusing existing coverage where possible. Dispatch bugs need the installed/public path. Behavior-preserving cleanup needs relevant existing tests, not new tests that merely mirror the refactor.
- Pure presentation needs new tests only when it carries a product contract, such as scope, transaction visibility, destructive-action warnings or accessibility. Export data-path changes require content and encoding coverage.
- Review the complete intended diff before committing. Run the applicable dependency/surface checks in the development guide. Do not modify tests or fixtures merely to hide a failure; identify unrelated failures separately.
- Both runners load the protocol packages from sibling checkouts (`../mysql.el`, `../pgsql.el`, `../mongodb.el` and `../redis.el`). Emacs searches `-L` directories in the order given, so a sibling beats a straight copy, and `CLUTCH_EXTRA_LOAD_PATH`, which comes last, cannot override one; `PGSQL_EL_DIR` chooses pgsql.el. Both runners print each dependency with its commit; check each sibling's branch and uncommitted changes before trusting a result.
- CI installs `mysql` and `pgsql` from MELPA and clones `main` of mongodb.el and redis.el. Clutch code that needs a protocol API those do not provide yet must detect it with `fboundp`, or land after the API reaches them.
- `native-live` stops at the first failing suite. Report that suite and name the suites that did not run.

## Documentation and release

- Update README and the relevant guide for changed commands, defaults or user-visible behavior. Keep current documentation consistent with implementation; do not change product code to justify a documentation claim.
- Add release-relevant changes to the existing version-based `Unreleased` section of CHANGELOG. Internal cleanup, tests and instructions with no product contract change do not need a release note. Commit or merge does not imply release or version bump.
- Preserve the published agent version/checksum pair. Changed published jar bytes require a matching Clutch checksum; prefer a new agent version, and verify the published artifact rather than substituting a local build.
- Before a release, review the range since the previous release tag for guards that no longer protect anything, copies that have drifted apart and code without callers, and converge them in commits of their own. Fix the bugs such a review finds first, each with a failing test.
- Record a short design rationale for material workflow changes, non-obvious architecture or compatibility decisions, abandoned approaches and deliberately deferred limitations. Reuse a relevant record for one change; routine refactors, wording and instruction maintenance do not require a new postmortem. Historical records remain historical.
- Keep Markdown/Org paragraphs on one source line. Reference detailed contracts from their canonical document rather than copying inventories into AGENTS.md.
