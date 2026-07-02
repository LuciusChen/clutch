# clutch Development Guide

Elisp best practices distilled from llm.el, magit, consult, eglot, vertico/marginalia.

## Core Principles

- **Question every abstraction**: Add layers, files, or indirection only when they solve a current problem. Prefer simpler code and clear ownership over speculative structure.
- **Refactor for net value**: A refactor must produce a concrete improvement in architecture, implementation simplicity, code size, robustness, extensibility, or test value. Moving code, renaming layers, or adding wrappers without making the system easier to understand or maintain is not enough.
- **Make abstractions pay for themselves**: A refactor should remove duplication, centralize a rule, or make callers simpler. If it mostly adds wrappers, accessors, or renamed intermediate state around one use site, keep the direct code.
- **Root out helper stacking**: Treat piles of tiny helpers, one-use wrappers, accessor layers, and pass-through functions as structural debt. When you find them, fix the missing ownership boundary or duplicated rule instead of only renaming helpers. Inline trivial one-use helpers, collapse wrapper ladders into one direct path, or move the whole workflow into a module that owns its state, commands, and rendering together.
- **Reduce code by improving the model**: Prefer slimming through simpler state, data flow, control flow, and ownership. Do not treat deduplication or file extraction as the primary route to code-size reduction.
- **Prefer lightweight Elisp shapes**: Use `let*`, `pcase-let`, alists/plists, small helpers, or table-driven mappings for short-lived context. Reserve `cl-defstruct` or object-style layers for stable data that crosses module or lifecycle boundaries, such as connection, result, or protocol state.
- **Delete, don't deprecate**: Remove unused code entirely. No backward-compatibility shims, re-exports, or "removed" comments.
- **Converge UX**: Prefer one clear entry point and one consistent behavior model over overlapping commands or branchy mode-specific behavior. Wrapper commands are fine, but they must share one resolution path, one action registry, and one default-action model.

## Diagnosis and Change Discipline

- **Find the root cause before changing behavior**: Do not patch UI timing, cache invalidation, or command flow until you can name the failing layer and explain why it is responsible.
- **One failed fix narrows the hypothesis**: If the first attempted fix does not hold, reduce the hypothesis space and gather evidence. Do not stack another speculative patch on top.
- **Two failed fixes stop the patching loop**: After two failed fixes on the same issue, stop changing behavior and switch to diagnosis only.
- **Fix the right layer**: If the real problem belongs in the JDBC agent, protocol code, cache model, or connection lifecycle, move the fix there instead of compensating in the UI layer.
- **Stabilize workflow changes before coding**: For any change that alters a primary entry point, default action, or action menu, write a short design note first.
- **Keep experiments narrow**: Start new directions with the smallest slice that proves the workflow is worth having. Do not expand scope before the first slice shows real user value.
- **Audit the whole project for broad refactors**: For project-wide cleanup, review all `*.el` modules, tests, documentation, and relevant sibling repositories before choosing changes. Do not optimize one visible subsystem and call the architecture done.
- **Clarify broad refactors before coding**: When architecture, scope,
  compatibility, naming, ownership, user-visible behavior, or stopping criteria
  are unclear, ask focused questions before implementation.  Inspect code,
  tests, docs, and existing conventions first; do not ask the user questions
  that local evidence can answer.  Ask at most 10 questions per round, include
  the recommended answer and tradeoff for each question, and order questions so
  upstream architectural decisions come before downstream implementation
  details.  Multiple rounds are allowed, but stop asking once the remaining
  uncertainty no longer changes the implementation plan.
- **Flag compensating code as design debt**: When touching a subsystem, look for code that compensates in the wrong layer — `condition-case nil` swallowing internal errors, re-querying data already available from a caller, timing hacks, or silent fallbacks. These are not blockers; record them in a postmortem as design debt rather than fixing inline. Do not let debt discovery delay the current change.

## Error Handling and Testing Discipline

- **Errors must surface, not hide**: Do not add fallback/default returns that silently swallow failures. Let errors propagate immediately.
- **Catch at the boundary, nowhere else**: Only the outermost API layer (process loop, top-level command handler) should catch and convert exceptions to error responses. Business logic must not `condition-case` around internal calls.
- **Robustness is not defensive programming**: Prefer clear ownership, fewer states and branches, explicit error boundaries, and verifiable invariants over broad fallback paths or compatibility scaffolding.
- **Error messages should describe the current problem**: Prefer "Not connected" or "Schema cache is stale" over command-style wording such as "Must be connected" or "Refresh schema first".
- **Tests must fail when the code is wrong**: If deleting or breaking the function under test does not turn the test red, the test is worthless. Assert specific, distinguishable output values.
- **Test the real dispatch path for dispatch bugs**: When the bug is in completion, hooks, command routing, async callbacks, or another dispatcher, include a test that drives the installed/public entry path. Helper-level tests are fine, but they must use the same filtering/input shape as the real caller; do not assert only against an unfiltered candidate collection unless the test is explicitly about candidate construction.
- **Match test weight to change size**: Use the smallest test that proves the intended behavior. Do not turn comment edits, documentation changes, mechanical refactors, or message-only wording changes into heavy red/green exercises.
- **Treat tests as part of the architecture budget**: Keep tests that prove public workflows, real invariants, and meaningful edge cases. Remove or simplify tests that only lock in implementation details, duplicate another assertion, or cannot fail when product behavior is wrong.
- **No hard-coded expectations**: Use diverse inputs — multiple data sets, random values, boundary cases — so that a hard-coded return cannot satisfy all assertions.
- **Red before green for real bug fixes**: When fixing a user-visible bug, correctness issue, regression, or timing-sensitive behavior, first write a failing test that reproduces it. Confirm it fails. Then fix the code. If an existing test already proves the path and the change is only a small wording or expectation update, updating that test is sufficient.

## Architecture and Implementation

- **Interface / implementation separation**: `mysql`, upstream `pg`, `mongodb`, and `redis` are external protocol libraries with no UI. `clutch.el` depends on `clutch-backend.el`, not protocol layers directly.
- **External dependency boundaries stay explicit**: Protocol packages are backend-specific optional dependencies. `mysql.el`, `pg.el`, `mongodb.el`, and `redis.el` are loaded only when users connect to the corresponding backend; missing protocol packages must produce clear connection-time errors. `ob-clutch` is a separate optional package and must not drift back into the `clutch` repo.
- **No external private APIs**: Do not call another package's double-dash symbols such as `mysql--*`, `mongodb--*`, `nerd-icons--*`, or `tramp-rpc--*`. If clutch needs behavior that only exists behind an external private helper, add or request a public API in that package and depend on the version that provides it. Optional integrations must warn clearly when the installed package is too old for the public interface.
- **MongoDB is one backend**: User configuration must use `:backend mongodb` for
  MongoDB.  The ordinary document surface uses native `mongodb.el`; MongoDB SQL
  Interface is only `:surface sql-interface` on the same backend.  Do not add a
  second public SQL-specific MongoDB backend, driver, feature, or manual chooser
  entry.
- **MongoDB protocol belongs in `mongodb.el`**: Clutch owns query-console syntax,
  completion, result rendering, saved connections, and auth-source/pass
  resolution.  BSON, wire messages, auth, sessions, server selection, cursors,
  and pooling belong in `mongodb.el` behind public `mongodb-` APIs.
- **MongoDB helper syntax stays basic**: Clutch's native MongoDB query adapter
  may translate a documented, small set of `db.*` helper forms into public
  `mongodb-` calls, but it must not grow toward full `mongosh` compatibility.
  Do not add arbitrary JavaScript evaluation, broad BSON constructor emulation,
  regex literal parsing, change-stream helpers, or long-tail cursor options
  without first moving the responsibility into a separate package/API and
  updating the support-level contract.
- **MongoDB residue must stay classified**: Clutch may keep only caller-facing
  MongoDB code: `clutch-mongodb.el` as the adapter from public `mongodb-` APIs
  to Clutch's generic database contract; `clutch-document.el` as the document
  query-console layer currently providing `clutch-mongodb-mode` syntax,
  highlighting, and completion; and MongoDB SQL Interface JDBC routing inside
  `clutch-db-jdbc.el` for `:backend mongodb :surface sql-interface`.
  Clutch must not contain BSON codecs, wire framing, URI/SRV parsing, auth,
  sessions, transactions, server selection, cursors, retry logic, compression,
  pooling, `mongosh` dependencies, or direct `mongodb--*` calls.  The internal
  JDBC driver key `mongodb` may exist only inside the JDBC surface and tests;
  user-facing configuration examples must not use `:driver mongodb`.
- **MongoDB client handles stay opaque**: The native adapter may store the
  public `mongodb-conn` object returned by `mongodb-connect`, but it should name it
  as `client` or connection state.  Do not expose protocol-layer words such as
  `wire`, `socket`, `pool`, `topology`, `OP_MSG`, or `serviceId` as Clutch
  adapter state or user-documentation concepts.
- **MongoDB connection params are opaque to Clutch**: Clutch may collect saved
  connection params, resolve `:password` from `:pass-entry` / auth-source, and
  pass the resulting plist to public `mongodb-` APIs.  It must not parse
  `mongodb://` / `mongodb+srv://` URLs, synthesize MongoDB URIs, infer SRV/TLS
  semantics, or duplicate `mongodb.el`'s effective database logic.  When Clutch
  needs the effective database or endpoint, read it from public `mongodb-`
  connection accessors or add a public API in `mongodb.el`.
- **Single responsibility per file**: Do not mix protocol code with rendering code.
- **Keep `clutch.el` as the entry point**: External consumers should continue to load `(require 'clutch)`. When implementation moves out, `clutch.el` becomes the assembler, not a grab bag.
- **Split by stable workflow boundaries**: Prefer complete responsibilities such as result UI, object workflow, staged mutation flow, or schema/cache lifecycle. Do not split by vague internal labels like `common`, `utils`, or `helpers`, and do not split only to make `clutch.el` shorter.
- **Stop splitting before glue takes over**: If an extraction mostly adds `defvar`, `declare-function`, cross-file hopping, or leftovers in the original file, stop and keep the ownership direct.
- **Use declarations to keep modules honest**: When a module depends on shared globals or functions defined elsewhere, add explicit `defvar` / `declare-function` forms so byte-compilation stays clean.
- **Do not use declarations as boundary patches**: A new `declare-function` to a higher-level clutch module, or to any external package private symbol, is a design smell. Move the interface to the owner module or expose a real public API instead.
- **Favor incremental modularization**: Move the smallest coherent slice first, then reload, byte-compile, and rerun focused tests before attempting the next extraction.
- **No behavioral side effects on load**: Loading a file must not alter Emacs editing behavior (no modes enabled, no hooks fired). Package-level registration side effects are allowed: fringe bitmaps, `auto-mode-alist` entries, backend registrations, Embark action registrations, and `kill-emacs-hook` cleanup.
- **Reuse Emacs infrastructure**: Use `completing-read`, `special-mode`, `text-property-search-forward`, standard hooks, and other stock primitives.
- **Public naming**: `clutch-` for the clutch package. External protocol packages keep their own public namespaces (`mysql-` / upstream `pg-`). No double dash for public API.
- **Private naming**: `clutch--` inside the clutch repo. Never call private symbols across subsystem boundaries. Files split from the same subsystem (e.g., `clutch-query.el`, `clutch-object.el`, `clutch-edit.el`, `clutch-schema.el` all belong to the `clutch` subsystem) may call each other's `clutch--` symbols, but must add `declare-function` / `defvar` declarations for byte-compilation.
- **Predicates**: Multi-word predicate names end in `-p`.
- **Unused args**: Prefix with `_`.
- **Prefer flat control flow**: Avoid deep `let` → `if` → `let` nesting. Use `if-let*`, `when-let*`, `pcase`, and `pcase-let`.
- **Prefer destructuring over repeated accessors**: Use `pcase-let` to destructure lists and plists instead of multiple `nth` or `plist-get` calls on the same object. For example, prefer `(pcase-let ((\`(,a ,b ,c) row)) ...)` over `(let ((a (nth 0 row)) (b (nth 1 row)) (c (nth 2 row))) ...)`.
- **Prefer `cl-loop` for non-trivial accumulation**: Use it instead of `dolist` + manual accumulators or over-clever folds.
- **Use the right error type**: `user-error` for user-caused problems; `error` for programmer bugs; `condition-case` for recoverable failures.
- **Do not wrap stdlib errors without semantics**: Use `user-error` directly unless the wrapper adds behavior that the builtin does not provide and the docstring names that behavior.
- **Prefer idiomatic primitives**: Use `vconcat` to build vectors from lists, not `apply #'vector`. Predicates returning non-nil need no `(not (null ...))` wrapper — the return value itself suffices.
- **State placement**: `defvar-local` for buffer state, plain `defvar` for shared state, `defcustom` for user options. Major modes must make their state buffer-local.
- **Mode definitions**: Read-only UI buffers derive from `special-mode`; editing buffers derive from the right parent (`sql-mode`, `comint-mode`, etc.). Register buffer-local hooks in the mode body with LOCAL=`t`.
- **Rendering discipline**: Use text properties for data-bearing annotations and overlays only for ephemeral visuals. Build render buffers from cached data, not by reparsing displayed text.
- **Function design**: Keep functions short, separate pure computation from display mutation, and keep interactive commands thin.

## Completion and Object Workflow

- Always use standard `completing-read`.
- Completion-at-point functions must return quickly and use `:exclusive 'no`.
- Add CAPFs buffer-locally via `add-hook` with LOCAL=`t`.
- Keep CAPF implementations close to the Emacs protocol: compute bounds and candidates directly, return the standard completion list, and avoid inventing a separate completion context model unless multiple real call paths share it.
- Keep object resolution, action definition, and action presentation separate. Embark and Transient are presentation layers, not independent business logic systems.

## Mutation Workflow Convergence

- **One staged-mutation vocabulary everywhere**: Footer, transient labels, help text, and `README.org` must use the same staged-edit / staged-delete / staged-insert terminology.
- **Identity must converge fully**: If pending state becomes row-identity-based, every lookup and render path must use the same row identity model.
- **Preview must show real execution payload**: A command named `Preview execution` must preview what would actually run.
- **Nearby workflows should share helpers**: Insert and edit flows should reuse completion, temporal helpers, and validation rules when semantics match.
- **UI symmetry must follow SQL semantics**: Do not copy insert-buffer metadata or controls into edit buffers unless update semantics truly match.
- **Validation must happen before context is destroyed**: Keep the user in the current insert/edit buffer when local validation fails.

## Version Baseline

- `clutch` targets **Emacs 29.1+**.
- The SQLite backend depends on built-in `sqlite-*` functions and is covered by the package baseline.
- The JDBC path depends on `clutch-jdbc-agent`, whose published baseline is **Java 17+**.
- Do not silently raise any baseline. If a change requires a higher Emacs or Java version, update:
  - `README.org`
  - relevant release/version metadata
  - a postmortem explaining why the higher baseline is justified

## SQL Rewrite Guardrails

- Do not rewrite SQL by brittle raw string insertion of `WHERE`, `ORDER BY`, or `LIMIT`.
- Prefer top-level clause-aware transformations with safe fallback behavior.
- For CTE / UNION / DISTINCT / window-function queries, prioritize semantic correctness over aggressive rewriting.
- Keep AST-level rewriting on the roadmap; do not force full AST complexity into small fixes.

## Documentation and Release Records

- Any change to key bindings, defaults, export behavior, or user-visible workflow must update `README.org` in the same change.
- Every release-relevant change must update `CHANGELOG.md` in the same change. This includes user-visible bug fixes, backend/protocol support, configuration or dependency changes, public API changes, and behavior that affects documented usage. Pure test refactors, comment-only edits, mechanical formatting, and internal cleanup with no release-note value do not need a changelog entry; when skipping the changelog for a non-trivial commit, state why in the final summary or commit rationale.
- Changelog release sections are version-based. Use `## VERSION - Unreleased` while a release is still in development, and replace `Unreleased` with the release date only when cutting the release or tag. Do not date unreleased feature branches.
- Merging or committing to `main` does not by itself create a release. Accumulate related fixes and small changes under the existing next-version `Unreleased` section; do not create or bump a version for every commit. Cut, date, and tag that version only when intentionally publishing a release.
- While `clutch` is still pre-1.0, bump patch for bug-fix-only releases and bump minor for new backends, substantial user-visible features, configuration/API breaks, or backend contract breaks. Breaking changes before 1.0 are recorded under the next minor version.
- Feature-branch changelog sections that summarize changes relative to `origin/main` must include a `Breaking Changes` section before `Added`. List real upgrade/configuration/API breaks there, and write `None` explicitly when the target version has no breaking changes.
- If code and docs diverge, treat code as source of truth and fix docs immediately.
- Optimize documentation for the rendered reader, not source-width aesthetics. Do not rewrap unchanged Markdown/Org prose or lists just to fit a column; rendered documents already wrap naturally.
- When documentation feels hard to read, improve the information structure: use a table, shorter bullets, a clearer heading, or a focused rewrite. Avoid changes whose only effect is different source line breaks.
- `clutch-jdbc-agent-version` and `clutch-jdbc-agent-sha256` are a pair. If one changes, review whether the other must change in the same commit.
- Do not assume a release asset is immutable just because the version string is unchanged. If the jar bytes change, update `clutch-jdbc-agent-sha256` immediately.
- Prefer bumping the agent version for released jar content changes. Replacing a GitHub release asset in place is an exceptional repair path, not normal workflow.
- Any release-asset change affecting JDBC startup or installation must update `README.org` and, when the tradeoff is non-obvious, add or update a postmortem.
- The `postmortem/` directory records design decisions and lessons learned. Read relevant records before significant changes.
- Postmortems are historical decision records, not current product documentation. Do not rewrite old postmortems just to match current behavior; when a later design supersedes an older record, write a new postmortem and optionally add a short "Superseded by NNN" note at the top of the older file.
- Write a postmortem when:
  - adding or changing a user-visible workflow
  - choosing between non-obvious architectural approaches
  - integrating an optional dependency
  - reverting or abandoning an approach
  - deliberately deferring a known limitation
- Postmortems must explain **why**, not restate the code.

## Quality and Release Checks

- Byte-compile distributable `clutch*.el` files, run `checkdoc` on them, and run `package-lint` on `clutch.el` with zero warnings.
- Export features that write files must provide explicit encoding behavior and sensible defaults.
- Document Excel compatibility guidance clearly.
- Any export-path change must include regression tests for content correctness and at least one encoding-related path.

## MELPA Compatibility Checklist

These rules keep the package compatible with MELPA submission requirements
(`package-lint`, `checkdoc`, and MELPA review conventions).

### Emacs 29.1 baseline

- Do not use Emacs 30+ APIs without a version guard or compatibility shim.
- When in doubt, check `M-x find-function` to verify when a symbol was introduced.

### File headers

- First line: `;;; file.el --- Short description -*- lexical-binding: t; -*-`
  - Description must NOT contain "for Emacs" or the package name — both are redundant.
  - Keep the description under 60 characters.
- `clutch.el` is the package entry file.  It is the only file that should carry
  package metadata such as `;; Package-Requires:`, `;; URL:`, `;; Version:`,
  and `;; Author:`.
- `;; Package-Requires:` in `clutch.el` must list all direct required dependencies with
  minimum versions, including the declared Emacs baseline. Lazy optional backend protocol packages such as `mysql.el`, `pg.el`, `mongodb.el`, and `redis.el` are documented but not listed.
- Split implementation files must not carry `;; Package-Requires:` headers, but
  they must carry formal license metadata, preferably `;; SPDX-License-Identifier:`.
- Keep the MELPA checklist attribution in the main package file when AI tools
  materially assist the package:
  `;; Assisted-by: OpenAI Codex:gpt-5.5`
- Last line: `;;; file.el ends here`

### Naming

- Follow the public/private naming rules above. MELPA additionally expects internal mode names, maps, and hooks that are not user-facing to use double-dash `clutch--`.
- Every `define-derived-mode` and `define-minor-mode` that is **not** user-facing should be private (`clutch--foo-mode`), or have `;;;###autoload` if it is user-facing.
- `defcustom` `:type` must be specified.

### Autoloads

- Add `;;;###autoload` to user-facing commands (entry points users call via `M-x`) and user-facing minor modes.
- Do NOT autoload internal helpers, variables, or private modes.

### checkdoc

- Every public `defun`, `defmacro`, `defcustom`, and `defvar` must have a docstring.
- Docstring first line must be a complete sentence ending with a period.
- Argument names in docstrings should be UPPERCASED.
- Run checkdoc across every distributable `clutch*.el` file, not only the main
  entry file.

### Common pitfalls

- `cl-lib` functions require `(require 'cl-lib)` — do not rely on transitive loading.
- Avoid `eval-when-compile` for runtime-needed dependencies.
- Compatibility shims must stay in the `clutch--` namespace.  When an upstream
  function exists, prefer a prefixed `defalias` over defining an unprefixed
  replacement.
- Avoid `with-eval-after-load` in package code unless the form registers an
  optional integration at a clear package boundary.

## Pre-Commit Checklist (Mandatory)

Every commit must pass all of these steps.

### 1. Read the full diff

```bash
git diff HEAD
```

Read every changed line before committing.

Also check that clutch does not depend on external private APIs:

```bash
rg -n -P "(?<![A-Za-z0-9-])(mysql|mongodb|nerd-icons|tramp-rpc)--[A-Za-z0-9-]+" clutch*.el test/*.el
```

This command should return no matches. Internal `clutch--*` and backend-local
`clutch-foo--*` symbols are allowed inside this repo.

Also check that MongoDB protocol implementation residue has not drifted back
into Clutch:

```bash
rg -n -P "require 'mongodb-(wire|bson|params|auth)|(?<![A-Za-z0-9-])mongodb--[A-Za-z0-9-]+|mongosh" clutch*.el test/*.el
```

This command should return no matches. Clutch may `(require 'mongodb)` and call
public `mongodb-` APIs, but BSON, wire protocol, auth, URI parsing, pooling, and
shell executable dependencies belong outside this repo.

Also check that Clutch has not reintroduced MongoDB URI parsing or synthesis:

```bash
rg -n "clutch-mongodb--.*(uri|url)|url-hexify-string|url-unhex-string" clutch-mongodb.el test/*.el
```

This command should return no matches.  `:url` may be passed through as an
opaque saved connection parameter, but MongoDB URL interpretation belongs in
`mongodb.el`.

Also check that Clutch's native MongoDB adapter has not reintroduced
protocol-layer naming:

```bash
rg -n "conn-wire|:wire|OP_MSG|wire protocol|MongoDB wire" clutch-mongodb.el
```

This command should return no matches.  Clutch may hold a public `mongodb-conn`
as an opaque client handle; wire/protocol terminology belongs in `mongodb.el`.

Also check that Clutch user docs do not duplicate detailed MongoDB protocol
capability prose:

```bash
rg -n "OP_MSG|wire compression|BSON wrappers|SASLprep|server selection|load-balanced|serviceId|lsid|endSessions|speculative SCRAM" README.org docs PRD.md
```

This command should return no matches.  Clutch docs may say that ordinary
MongoDB uses the external `mongodb.el` native client, then link to `mongodb.el` for
protocol details.

Also check that MongoDB SQL Interface has not reappeared as a second backend or
driver:

```bash
rg -n "mongodb[-_]sql(|[-_]interface)" clutch*.el test/*.el README.org docs
```

This command should return no matches.  Prose may say "MongoDB SQL Interface"
as the product name, but symbols and configuration examples must use
`:backend mongodb :surface sql-interface`.

Also check that user-facing documentation does not recommend the internal JDBC
driver key as configuration:

```bash
rg -n ":driver +'?mongodb|:driver +mongodb" README.org docs PRD.md
```

This command should return no matches.  `:driver 'mongodb` may appear only as
internal JDBC connection state or in tests that reject old public config.

### 2. Run all test files

```bash
./test/run-ci.sh main db
```

Default ERT runs skip live tests when credentials are unset. For changes touching
query execution, row identity, result-buffer workflows, object metadata, or native
backend adapters, also run the real MySQL/PostgreSQL/MongoDB live suite:

```bash
./test/run-ci.sh native-live
```

The native live runner starts or reuses local containers, preferring Podman on
Linux and OrbStack-backed Docker on macOS. It runs both UI-level `:clutch-live`
tests and backend-level `:pg-live` / `:mysql-live` / `:mongodb-live` /
`:redis-live` tests. JDBC
live tests remain separate because they require external credentials.

### 3. Byte-compile with zero warnings

```bash
./test/run-ci.sh byte-compile
```

### 4. Run package-lint on the package entry file

`clutch` is one package split across multiple implementation files, so
`package-lint` should run on the package entry file rather than on extracted
modules as if they were standalone packages.  For local straight checkouts,
make sure package metadata for required external deps such as `transient` is
available to `package.el` in the batch session before running the command.
Do not move `Package-Requires` into split files to satisfy per-file lint; set
`package-lint-main-file` to `clutch.el` when linting implementation files
directly.

```bash
./test/run-ci.sh package-lint checkdoc
```

### 5. Update tests when behavior changes

When behavior changes intentionally, update existing relevant tests first. Add a new failing test only when the current suite does not already prove the regression or changed behavior.
