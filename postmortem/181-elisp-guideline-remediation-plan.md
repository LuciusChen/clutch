# 181 — Elisp guideline remediation plan

> **Status:** Phases 0 and 1 are complete on `plan/elisp-live-remediation`.
> Later phases remain planned work. No dates or effort estimates are part of the
> plan.

## Execution evidence

- The pre-fix Podman baseline passed with 55 expected results, seven expected
  skips, and no unexpected results. Targeted live probes confirmed synchronous
  CAPF metadata calls for native PostgreSQL, MySQL, and MongoDB.
- The Phase 1 fix makes native SQL completion advertise deferred column loading
  through a backend capability and gives MongoDB a minimal idle metadata method.
  Cache-hit completion behavior remains covered by ERT.
- The post-fix Podman baseline again passed with 55 expected results, seven
  expected skips, and no unexpected results. Targeted real-connection probes
  recorded zero synchronous CAPF metadata calls and one successful deferred
  cache hydration for each of PostgreSQL, MySQL, and MongoDB.
- The complete non-live gate passed: 541 main ERT tests, 220 backend ERT tests,
  byte compilation, package lint, checkdoc, and 13 architecture tests.
- Oracle/JDBC completion remains unchanged and issue-specific Oracle live
  reproduction remains unavailable, so it is still outside Phase 1.

## Purpose and evidence boundary

This plan turns the current Emacs Lisp guideline findings into ordered, reviewable work while preserving the existing AgentKnot/Clutch boundaries. It fixes the narrow behavioral defect first, then addresses error-capture hygiene, documentation ownership, deterministic style, and only finally the structural findings that may or may not justify a refactor.

The repository's product requirements live in the root `PRD.md`. Together with `docs/architecture.md`, `docs/backend-support.org`, `docs/interactive-client.org`, `docs/mongodb-backend.org`, `docs/native-backends.md`, `README.md`, the executable tests, and the related postmortems below, it supplies the evidence used by this plan.

The relevant historical records are `postmortem/028-context-bounded-schema-hints-and-result-status.md`, `postmortem/037-oracle-completion-vs-schema-refresh.md`, `postmortem/047-oracle-completion-cache-first.md`, `postmortem/084-native-metadata-latency-model.md`, `postmortem/086-pg-idle-timer-response-contamination.md`, `postmortem/104-backend-query-mode-facet.md`, `postmortem/109-document-surface-boundaries.md`, `postmortem/110-support-level-and-transport-boundaries.md`, `postmortem/131-mongodb-helper-surface-convergence.md`, and `postmortem/133-architecture-boundaries-and-state-ownership.md`. They are constraints and rationale, not proof that the current code still has every behavior described in them.

## Repository-evidence matrix

| Finding or contract | Current evidence | Classification and owner | Planned treatment |
|---|---|---|---|
| Product and architecture contract | `PRD.md` requires bounded metadata, honest capability gates, staged SQL mutations, explicit external protocol boundaries, and public-path verification; `docs/architecture.md` assigns SQL completion to `clutch-sql.el`, MongoDB query-buffer behavior to `clutch-document.el`, metadata lifecycle to `clutch-schema.el`, and backend protocol translation to adapters. | Source-of-truth contract; workflow owner, not a reason to add a new framework. | Every phase stays within the named owner and records any out-of-scope boundary instead of compensating in another layer. |
| Native-live baseline | `test/run-ci.sh:native-live` invokes `test/run-native-live-tests.sh`; the runner selects Podman on available Linux hosts, and runs UI MySQL/PostgreSQL, backend PostgreSQL/MySQL/cross-SQL, native MongoDB, and native Redis coverage. `test/clutch-test-live.el` and tagged sections of `test/clutch-db-test.el` exercise real paths. | Shared regression gate owned by the test runner and live suites. | Phase 0 preserves the already established full Podman baseline; every later phase runs it as regression protection and never weakens tags or substitutes a partial suite. |
| SQL CAPF can load uncached columns synchronously | `clutch-sql.el:1109-1119` makes `clutch--completion-column-values` call `clutch--ensure-columns` whenever `clutch-db-completion-sync-columns-p` is non-nil; `clutch-sql.el:1296-1382` reaches that path from `clutch-completion-at-point`. `clutch-schema.el:532-549` calls the backend's synchronous `clutch-db-list-columns`. | Behavioral latency/re-entrancy defect in the SQL completion workflow. The generic contract is in `clutch-backend.el:1128-1132`; the hot path is owned by `clutch-sql.el` and the metadata lifecycle by `clutch-schema.el`. | Phase 0 reproduces it, and Phase 1 changes only the PostgreSQL/MySQL/MongoDB CAPF column-miss behavior. The existing cache-hit and table-name behavior remains covered. |
| Native PostgreSQL and MySQL backend calls are real remote metadata work | `clutch-db-pg.el:965-974` queries `information_schema.columns`; `clutch-db-mysql.el:529-536` runs `SHOW COLUMNS`; both adapters install idle metadata methods at `clutch-db-pg.el:922` and `clutch-db-mysql.el:481`. `docs/native-backends.md` already says native MySQL/PostgreSQL CAPF paths are cache-first, which is the contract the reproducer must compare with the implementation. | Same Phase 1 behavioral defect; adapter query implementations are not themselves the initial owner. | Keep query text, protocol adapters, and unrelated Eldoc/schema-refresh behavior out of the first slice unless the red reproducer proves they own the failure. |
| Native MongoDB field CAPF can sample metadata synchronously | `clutch-document.el:325-337` defines `clutch-mongodb--collection-columns`; on a cache miss it calls `clutch--ensure-columns` through `clutch--safe-completion-call`, and `clutch-document.el:428-444` invokes it through `clutch-mongodb-completion-at-point`. `clutch-mongodb.el:1796-1800` samples documents in `clutch-db-list-columns`. | Behavioral document-CAPF latency defect owned by `clutch-document.el` plus the minimal MongoDB adapter metadata boundary needed for deferred work. | Phase 1 tests the public completion dispatch and prevents an uncached field completion from synchronously sampling documents; it does not expand the MongoDB parser or protocol surface. |
| Oracle table search is a different path | Oracle/JDBC deliberately returns nil from `clutch-db-completion-sync-columns-p` at `clutch-db-jdbc.el:1389-1392`, while `clutch-db-complete-tables` uses `search-tables` at `clutch-db-jdbc.el:2129-2136` and `clutch-db-complete-columns` uses `search-columns` at `clutch-db-jdbc.el:2183-2194`. Unit coverage exists at `test/clutch-db-test.el:3355-3375`; an Oracle live low-privilege completion test exists near `test/clutch-db-test.el:6538`. | Separate Oracle/JDBC behavior with a separate evidence requirement, not part of the native CAPF defect. | Phase 1 explicitly does not change Oracle synchronous `search-tables`/`search-columns`; Oracle work is blocked until an Oracle live reproduction of that specific issue is available. Existing generic or low-privilege tests are not silently treated as that reproduction. |
| Completion/re-entrancy history already identifies the risk | `postmortem/028` records synchronous metadata on high-frequency paths; `postmortem/084` establishes cache-first native CAPF/Eldoc policy; `postmortem/086` documents `accept-process-output`/idle-timer response contamination and the earlier PG-specific change. | Historical rationale and a warning against timing patches or broad async abstractions. | Use a fail-first test and the existing metadata lifecycle; do not fix the issue by hiding errors, adding worker threads, or changing external protocol code without evidence. |
| Object error-capture macro has incomplete hygiene metadata and confirmed capture | `clutch-object.el:1358-1367` defines `clutch--with-object-error-capture` with `(declare (indent 4))` but no Edebug debug specification and binds the literal handler variable `err`; callers are `clutch-object.el:1381`, `1421`, `1445`, and `1495`. A batch evaluator probe confirmed that passing a caller connection expression named `err` causes the handler to observe the `clutch-db-error` object instead of the outer connection. Existing public behavior tests are in `test/clutch-test-object.el` around the definition/describe failure and stale-problem-clearing tests, but this capture case has no permanent ERT. | Confirmed error-boundary macro defect owned by `clutch-object.el`; this is a focused hygiene/behavior issue, not a reason to catch more conditions. | Phase 2 adds a fail-first macro/public-workflow test for lexical capture, exact re-signaling, success clearing, and non-`clutch-db-error` propagation, then supplies only the needed hygienic expansion and debug metadata. |
| MongoDB documentation ownership is split but currently overlaps | `docs/backend-support.org` owns support levels and the one-backend/SQL-Interface boundary; `docs/mongodb-backend.org` owns Clutch's native query and object workflow but also contains driver-level claims about URLs, SRV, sessions, pooling, and TLS; `README.md` is the landing page and links to the backend guide; `postmortem/104`, `109`, `110`, and `131` define the intended boundary. | Documentation ownership and capability-claim risk; no MongoDB protocol implementation belongs in Clutch. | Phase 3 keeps Clutch-specific workflow facts in Clutch docs, points protocol semantics to `mongodb.el`, and audits public examples for `:backend mongodb :surface sql-interface` rather than an invented second backend or internal JDBC configuration. |
| Staged and pending vocabulary diverges | `clutch-edit.el` and `clutch-result.el` mostly use “stage”/“staged”, while `docs/interactive-client.org` still says “Submit all pending changes” and “Discard pending change”; `clutch-result.el:367-403` and `clutch-ui.el:1707-1717` use pending-oriented internal names/status, and `clutch-result.el:3784-3794` uses staged transient labels. `postmortem/001`, `004`, `028`, `035`, `115`, `159`, and `177` establish the staged two-step and transaction semantics. | User-visible terminology consistency; internal state names are not automatically defects. | Phase 3 defines one user vocabulary—stage/staged edit, staged deletion, staged insert, submit staged changes, discard staged change—and retains “pending” only where it describes non-mutation runtime state or a literal data value unless a reviewed UI contract requires otherwise. |
| Deterministic style findings exist, but the scan must distinguish code from display text | Actual hard-tab indentation occurs in production source at `clutch-sql.el:530-532`, `clutch-backend.el:1738-1762`, and `clutch-mongodb.el:1845-1846`; other tabs occur in keybinding docstrings such as `clutch-result.el:991-1022`, where they may be display separators. The current `(when (not ...))` idiom appears at `clutch-sql.el:50`, `clutch-sql.el:1271`, `clutch-result.el:1879`, `clutch-edit.el:859-861`, and `clutch-ui.el:2063`; literal `when-not` is absent. Redundant `(not (null ...))` forms appear at `clutch-db-pg.el:636`, `clutch-db-pg.el:888`, and `clutch-ui.el:2309`. | Mechanical style candidates; semantics and docstring display contracts still require review. | Phase 4 makes only small deterministic corrections, with static evidence as the primary acceptance and live runs as regression protection. |
| Structural guideline findings are candidates, not automatic defects | A current arity inventory includes `clutch-result--init-state` (11 args including its optional tail), `clutch-result--display-select` (8), `clutch-record--render-field` (11), `clutch--start-table-metadata-request` (8 including its optional tail), `clutch--completion-column-values` (5), and `clutch--eldoc-metadata-plan` (5). Repeated accessors appear in context/render paths, manual accumulation appears in `clutch--completion-column-candidates` and MongoDB field collection, and several untouched macros lack debug specs. A syntax-aware source-span inventory identifies longer functions such as `clutch-result-edit--open-cell`, `clutch-mongodb--execute-collection-method`, `clutch--object-entry-reader`, `clutch--agent-context-text`, and `clutch-completion-at-point`. | Possible maintainability work; arity may be a generic/callback contract, a stable lifecycle context, or a local implementation detail. | Phase 5 triages each candidate for ownership, duplication, measurable complexity, and test value. No refactor follows from a count alone, and untouched test macros do not receive touch-only metadata edits. |

## Verification policy shared by all phases

**Every behavioral fix must be preceded by a reproducer and followed by `./test/run-ci.sh native-live`.** “Preceded” means the reproducer fails for the intended root cause before the production change; “followed” means the full native Podman suite is run after the change, not only a mocked unit test. Focused tests must exercise the public or dispatch path when the defect is in CAPF dispatch, object commands, callbacks, or lifecycle behavior.

**Static documentation/style findings use static evidence plus live as regression protection.** A clean `rg`, byte-compile, or documentation review proves the static condition; `native-live` and any available JDBC live run only protect unrelated runtime behavior from collateral changes and do not turn an unverified documentation claim or an absent Oracle reproduction into evidence.

The full non-live gate for a code-bearing phase is `./test/run-ci.sh all`, with the mandatory focused baseline also available as `./test/run-ci.sh main db`. Quality checks remain part of `all`: byte compilation, package lint, checkdoc, and architecture checks. Live gates must never print or commit credentials, URLs containing credentials, logs, or runtime state.

## Ordered remediation phases

### Phase 0 — Preserve the full Podman native-live baseline and add issue-specific reproduction evidence

**Exact scope**

- Preserve `test/run-native-live-tests.sh` and its complete container matrix: UI and backend PostgreSQL/MySQL, cross-SQL, native MongoDB, and native Redis. Do not remove a tag, weaken a live assertion, replace the complete runner with a unit-only command, or silently switch the Linux baseline away from Podman.
- Use `CLUTCH_TEST_CONTAINER_RUNTIME=podman ./test/run-ci.sh native-live` on the established Linux baseline, or the runner's documented Podman selection when that variable is not needed. Record the command/result without recording credentials.
- Turn the already observed issue-specific live probes into repeatable test evidence through the real CAPF paths: an uncached PostgreSQL/MySQL SQL identifier or column context must show whether `clutch-completion-at-point`/`completion-at-point` performs synchronous column metadata work, and an uncached MongoDB field/key context must show whether `clutch-mongodb-completion-at-point` samples collection fields synchronously.
- Keep the reproduction bounded to one statement/collection and one cache miss. Capture call counts, busy state, and the returned CAPF shape rather than relying on timing alone; a timeout or sleep is not a root-cause proof.
- Keep Oracle synchronous `search-tables` out of this reproduction. Its evidence must be collected through an Oracle live environment separately, not inferred from native containers or a JDBC mock.

**Dependencies**

- Current `clutch-sql.el`, `clutch-document.el`, `clutch-schema.el`, native adapters, and the existing live runner are the only implementation dependencies for the first evidence slice.
- The local external packages loaded by `test/run-ci.sh`—`pg-el`, `mysql.el`, `mongodb.el`, and `redis.el`—must be available for the full Podman run; no protocol-private API or credential fixture is added.
- The root `PRD.md`, `docs/architecture.md`, `postmortem/028`, `084`, and `086` supply the latency and ownership contract.

**Acceptance criteria**

- The complete Podman native-live baseline passes without reducing its backend or UI coverage, and the result identifies the runtime as Podman.
- The issue-specific evidence distinguishes a cache hit from an uncached column/field miss and identifies the synchronous call chain, if present, rather than reporting only that completion feels slow.
- The reproduction can be rerun without secrets and does not modify the caller repository, external protocol package, container contents beyond test data, or Git state.
- No Phase 1 production fix or Oracle behavior change is smuggled into baseline preservation.

**Focused checks**

- Run the proposed focused ERT reproduction through the normal module loader once named, for example `CLUTCH_TEST_SELECTOR='"^clutch-test-.*capf-uncached-"' ./test/run-ci.sh main`; the pre-fix result must fail for the synchronous metadata call, not for a missing package or malformed fixture.
- Run the backend contract slices that establish metadata behavior with `./test/run-ci.sh db-pg db-mysql db-mongodb` and inspect the ERT output for the issue-specific assertions.
- Use static call-graph checks such as `rg -n "clutch--completion-column-values|clutch--ensure-columns|clutch-mongodb--collection-columns|clutch-mongodb-completion-at-point" clutch-sql.el clutch-document.el clutch-schema.el test` to keep the reproducer attached to current symbols.

**Full non-live checks**

- Run `./test/run-ci.sh main db`, then `./test/run-ci.sh all`; the first confirms the ordinary ERT gates and the second includes byte-compile, package-lint, checkdoc, and architecture checks.

**Podman/JDBC live gate**

- The required native gate is `CLUTCH_TEST_CONTAINER_RUNTIME=podman ./test/run-ci.sh native-live` on Linux, and it must remain the full runner rather than a selected tag. On macOS, use the repository's OrbStack-backed Docker rule instead of claiming a Podman result.
- The JDBC gate is `./test/run-ci.sh db-live` only when the configured JDBC live credentials/endpoints are available; absent credentials must be reported as blocked/skipped, never as a pass. Do not use a skipped Oracle suite as evidence for the Oracle hold.

### Phase 1 — Fix only the PostgreSQL/MySQL/MongoDB CAPF synchronous column-metadata defect

**Exact scope**

- Change the cache-miss behavior reached by `clutch-sql.el:1109-1119` and `clutch-sql.el:1296-1382` for native PostgreSQL and MySQL so the public SQL CAPF does not synchronously issue `clutch-db-list-columns` during typing. Preserve cached candidates, statement scoping, qualification/annotation, `:exclusive 'no`, keyword priority, and the existing public completion dispatch.
- Change the native MongoDB field/field-path path at `clutch-document.el:325-337`, `clutch-document.el:350-377`, and `clutch-document.el:428-444` so an uncached field completion does not synchronously invoke sampled `clutch-db-list-columns`. Reuse the existing metadata lifecycle where it is sufficient; if MongoDB lacks the needed async method, add only the minimal adapter-bound deferred metadata operation required by this defect, not a general MongoDB async framework.
- Keep `clutch-schema.el:532-549` as the explicit/synchronous metadata owner for workflows that intentionally request metadata. Do not change Eldoc, explicit schema refresh, result detail commands, SQLite's in-process synchronous path, table-name completion, or the MongoDB helper parser in this phase unless the reproducer proves one of those is the direct owner of the CAPF defect.
- Keep Oracle/JDBC synchronous table search separate: do not change `clutch-db-jdbc.el:1389-1392`, `2129-2136`, or `2183-2194`, and do not reinterpret Oracle `search-tables` unit coverage as a native CAPF fix. Oracle work remains blocked until a live Oracle reproduction of synchronous `search-tables` behavior is available.

**Dependencies**

- Phase 0 must identify the failing synchronous call chain and preserve a red reproduction before production code is edited.
- The implementation must follow `postmortem/084` and `086`: cache-first native completion, main-thread idle metadata, and no worker-thread or external protocol patch. `postmortem/037` and `047` keep Oracle's prefix/table-search behavior as a separate performance contract.
- Existing focused tests in `test/clutch-test-sql.el:1292-1365`, the SQL CAPF tests around `1048-1124`, and MongoDB CAPF tests in `test/clutch-test-connection.el:2303-2464` are the starting fixtures; add only the missing red assertions.

**Acceptance criteria**

- A focused ERT test fails before the fix and passes afterward for each native backend family in scope: PostgreSQL, MySQL, and MongoDB. The tests invoke `completion-at-point` or the installed buffer-local CAPF, not only a private candidate helper.
- On an uncached column/field miss, CAPF returns without a synchronous metadata round trip and leaves a bounded, observable path for later cache hydration or honestly returns no uncached candidates; it does not manufacture candidates or swallow an internal failure as success.
- A cache hit still returns the same candidates and annotations, and a busy connection still avoids re-entrant work. Existing table-context and empty-prefix SQL completion contracts remain intact.
- The Oracle `search-tables`/`search-columns` path is byte-for-byte out of scope for this phase's intent and remains covered only by its existing tests until an Oracle live reproduction is available.
- The post-fix native-live run passes for real PostgreSQL, MySQL, and native MongoDB connections, demonstrating that the change is not merely a mock-specific result.

**Focused checks**

- Run the new fail-first selector before editing and the same selector after editing: `CLUTCH_TEST_SELECTOR='"^clutch-test-.*capf-uncached-"' ./test/run-ci.sh main`.
- Run the existing source-specific completion set with `CLUTCH_TEST_SELECTOR='"^clutch-test-completion-"' ./test/run-ci.sh main`, then run `./test/run-ci.sh db-pg db-mysql db-mongodb`.
- Verify the Oracle separation with the existing `clutch-db-test-jdbc-complete-tables-searches-rpc-without-schema-cache-dependency` test and a static diff review showing no Oracle completion method was changed.

**Full non-live checks**

- Run `./test/run-ci.sh main db` and `./test/run-ci.sh all` after the focused ERT is green; no live skip is an acceptable substitute for the non-live suite.

**Podman/JDBC live gate**

- Run the full native gate as `CLUTCH_TEST_CONTAINER_RUNTIME=podman ./test/run-ci.sh native-live` and require the complete Podman UI/backend matrix, including native MongoDB, after the fix. The focused ERT does not replace this gate.
- Run `./test/run-ci.sh db-live` when JDBC credentials are configured as a no-regression gate for untouched JDBC behavior. Oracle CAPF/search-table work remains blocked if the issue-specific Oracle live reproduction is still absent, even if unrelated JDBC live tests pass.

### Phase 2 — Fix and test `clutch--with-object-error-capture` hygiene

**Exact scope**

- Restrict implementation work to `clutch-object.el:1350-1367`, its four callers at `clutch-object.el:1381`, `1421`, `1445`, and `1495`, and focused tests in `test/clutch-test-object.el`/`test/clutch-test-debug.el`.
- Preserve the boundary: catch only `clutch-db-error`, record buffer/connection/object/operation provenance through `clutch--remember-object-operation-error`, re-signal the original condition/data, and clear connection problem capture only after the complete body succeeds. Do not convert `user-error`, `quit`, or programmer errors into plausible object failures.
- Make the macro expansion hygienic for the confirmed literal-`err` capture; use a generated binding local to the expansion rather than a broad fallback. Add the Edebug debug specification alongside the existing `(indent 4)` because this macro is being touched, while leaving unrelated untouched test macros alone.

**Dependencies**

- Phase 0's baseline and Phase 1's native gate must remain green so an object/diagnostic failure is not confused with the CAPF change.
- Existing object tests already prove several public error and stale-record behaviors; the new test must target the missing macro invariant rather than duplicate a private implementation assertion.
- `docs/architecture.md` and `postmortem/133` require diagnostics to remain a leaf and errors to retain source provenance; no reverse dependency or callback registry is introduced.

**Acceptance criteria**

- A fail-first ERT reproducer demonstrates the hygiene failure or missing contract, then passes after the smallest macro change. It must cover a body that binds a caller variable named `err`, a `clutch-db-error` that is recorded and re-signaled with its original condition/data, a successful body that clears stale connection capture, and a non-`clutch-db-error` that propagates untouched.
- Public entry tests continue to cover `clutch-object-describe`, `clutch-describe-refresh`, `clutch-object-show-ddl-or-source`, and the document collection action path where applicable; the macro test is not the only proof of dispatch behavior.
- The expansion evaluates body forms once, does not evaluate error metadata eagerly on success, and does not swallow cleanup or diagnostic failures. The macro's debug/indent metadata is correct for Emacs tooling.

**Focused checks**

- Run the new object-capture selector before and after the fix with `CLUTCH_TEST_SELECTOR='"^clutch-test-.*object-error-capture"' ./test/run-ci.sh main`.
- Run the public object/error slices with `CLUTCH_TEST_SELECTOR='"^clutch-test-object-"' ./test/run-ci.sh main` and the debug module through `CLUTCH_TEST_MODULES=clutch-test-debug ./test/run-ci.sh main`.
- Run `./test/run-ci.sh byte-compile checkdoc` to verify the macro declaration and public documentation without accepting warnings as a hygiene pass.

**Full non-live checks**

- Run `./test/run-ci.sh main db` and `./test/run-ci.sh all`; inspect the complete diff for new catches, silent defaults, or declarations that cross the architecture boundary.

**Podman/JDBC live gate**

- Run `CLUTCH_TEST_CONTAINER_RUNTIME=podman ./test/run-ci.sh native-live` after the macro fix as the full native regression gate; object/error changes must not be validated only against SQLite mocks.
- Run `./test/run-ci.sh db-live` when configured, including Oracle/JDBC object metadata tests where available. If JDBC live credentials are unavailable, record that limitation and do not call the JDBC gate passed.

### Phase 3 — Reconcile MongoDB documentation ownership and staged/pending user vocabulary

**Exact scope**

- Make `docs/backend-support.org` the owner of support levels, the one `mongodb` backend, and the `:surface sql-interface` distinction; make `docs/mongodb-backend.org` the owner of Clutch-specific native query/object/result behavior; keep `README.md` as the landing page and concise workflow entry point; keep `docs/jdbc-backend.org` as the JDBC/SQL-Interface guide; keep `docs/architecture.md` as the module-boundary record.
- Audit `docs/mongodb-backend.org` for protocol-level claims that belong to `mongodb.el`, especially the URL/SRV/TLS/session/pooling language around its requirements and connection sections. Retain verified Clutch-owned facts such as supported helper syntax, bounded result behavior, sampled field completion, object actions, and the read-only native result boundary, while linking protocol semantics to the external package instead of duplicating them.
- Audit public configuration examples and prose for a second MongoDB backend, `:driver mongodb`, shell executable assumptions, or protocol implementation claims. Keep examples in the public shape `(:backend mongodb ...)` and `(:backend mongodb :surface sql-interface ...)`; internal JDBC driver state and tests are not user configuration examples.
- Reconcile user-facing mutation vocabulary across `README.md`, `docs/interactive-client.org`, transient/help strings in `clutch-result.el` and `clutch-edit.el`, and footer wording in `clutch-ui.el`. Use “stage/staged edit”, “staged deletion”, “staged insert”, “submit staged changes”, and “discard staged change” consistently. Do not rename `clutch--pending-*` variables merely for appearance; decide separately whether a user-visible “pending” phrase describes an unsubmitted mutation or a genuinely pending asynchronous/runtime operation.
- Keep native MongoDB's read-only result grid distinct from generated document mutation snippets: generated copy/export text is not a staged native MongoDB mutation workflow, and SQL Interface remains a separate surface of the same backend.

**Dependencies**

- Phase 1 must establish the actual completion behavior before documentation says it is cache-first or deferred; code and tests remain the source of truth.
- `postmortem/104`, `109`, `110`, and `131` define the MongoDB ownership and helper-surface stopping rules; `postmortem/001`, `004`, `028`, `035`, `115`, `159`, and `177` define staged mutation semantics and status ownership.
- Any user-visible string correction that changes command meaning requires its owning code/test review; wording-only documentation corrections do not justify product-code abstractions.

**Acceptance criteria**

- Each MongoDB capability claim has one documented owner, and Clutch docs no longer duplicate detailed external protocol behavior or imply that Clutch owns BSON/wire/auth/URI semantics.
- All public examples use the one-backend model and the documented SQL Interface surface; no user-facing configuration recommends the internal JDBC driver key.
- A reader can distinguish native MongoDB read-only document results, generated helper snippets, core SQL staged mutations, and MongoDB SQL Interface behavior without relying on historical postmortems.
- Footer, transient, help, README, and interactive-guide language uses the same staged-mutation vocabulary, while legitimate runtime “pending” state and literal data values are not mechanically renamed.

**Focused checks**

- Run the ownership scans required by `AGENTS.md`: `rg -n -P "(?<![A-Za-z0-9-])(mysql|mongodb|nerd-icons|tramp-rpc)--[A-Za-z0-9-]+" clutch*.el test/*.el`, `rg -n -P "require 'mongodb-(wire|bson|params|auth)|(?<![A-Za-z0-9-])mongodb--[A-Za-z0-9-]+|mongosh" clutch*.el test/*.el`, and `rg -n "OP_MSG|wire compression|BSON wrappers|SASLprep|server selection|load-balanced|serviceId|lsid|endSessions|speculative SCRAM" README.md docs PRD.md`.
- Run the MongoDB configuration scans `rg -n "mongodb[-_]sql(|[-_]interface)" clutch*.el test/*.el README.md docs` and `rg -n ":driver +'?mongodb|:driver +mongodb" README.md docs PRD.md`; inspect any remaining match rather than deleting it blindly.
- Run `rg -n -i "pending|staged|stage|discard" README.md docs/interactive-client.org docs/mongodb-backend.org clutch-result.el clutch-edit.el clutch-ui.el` and review each user-facing match against the glossary. Run `./test/run-ci.sh db-mongodb` for the native/SQL-Interface backend contract tests after documentation and any linked wording change.

**Full non-live checks**

- Run `./test/run-ci.sh main db` and `./test/run-ci.sh all` as regression protection. Documentation-only wording does not require a new product test unless it defines an executable command, capability, or safety contract.

**Podman/JDBC live gate**

- Run `CLUTCH_TEST_CONTAINER_RUNTIME=podman ./test/run-ci.sh native-live` so the documented native MongoDB boundary and the shared result workflows remain backed by real local services.
- Run `./test/run-ci.sh db-live` when configured; if the documentation changes SQL Interface claims, also require the separately tagged `:sql-interface-mongodb-live` endpoint when available. A local community `mongod` container is not evidence for the JDBC SQL Interface surface.

### Phase 4 — Apply only deterministic style corrections in small reviewable batches

**Exact scope**

- Correct actual production indentation hard tabs at `clutch-sql.el:530-532`, `clutch-backend.el:1738-1762`, and `clutch-mongodb.el:1845-1846` without broad reindentation. Treat tabs embedded in keybinding docstrings such as `clutch-result.el:991-1022` as a separate display-contract decision; preserve their rendered alignment unless a tested equivalent is deliberately chosen.
- Replace `(when (not CONDITION) ...)` with `unless` only where the condition and body are semantically identical, including the current candidates at `clutch-sql.el:50`, `1271`, `clutch-result.el:1879`, `clutch-edit.el:859-861`, and `clutch-ui.el:2063`. There is no literal `when-not` symbol in the current scan; the target is the redundant when/not idiom, not a new macro.
- Replace redundant `(not (null VALUE))` in production only when the surrounding expression accepts the same truthiness and no explicit boolean return is part of the contract, starting with `clutch-db-pg.el:636`, `clutch-db-pg.el:888`, and `clutch-ui.el:2309`. Do not alter tests merely to satisfy a grep result when they intentionally assert a boolean conversion.
- Do not combine these mechanical edits with function extraction, API renaming, control-flow redesign, documentation reflow, or unrelated formatting. Submit each small batch with a complete diff review.

**Dependencies**

- Phases 1–3 must have stable behavioral and wording contracts so style edits do not hide a root-cause change.
- Each candidate must have static evidence and a semantic review; a tab in a display string, a boolean coercion in a public predicate, or a conditional with side effects is not automatically redundant.
- `AGENTS.md` and the package-quality commands define the style/quality baseline; no new dependency or compatibility shim is needed.

**Acceptance criteria**

- Production code has no unintentional hard-tab indentation in the reviewed scope; any retained docstring tabs have a documented display reason and unchanged rendered behavior.
- The reviewed `(when (not ...))` forms use `unless` where safe, the reviewed redundant `not/null` forms preserve values and predicates, and no literal `when-not` or new equivalent anti-pattern is introduced.
- Each batch is easy to revert, has no unrelated diff, and passes byte compilation, checkdoc, and the relevant focused tests.

**Focused checks**

- Run `rg -n $'\\t' clutch*.el` and inspect every result; run `rg -n -U "\\(when\\s+\\(not|\\(when\\s*\\n\\s*\\(not" clutch*.el` and `rg -n "\\(not[[:space:]]+\\(null" clutch*.el` after each batch.
- Run `git diff --check`, `./test/run-ci.sh byte-compile checkdoc`, and the nearest module tests for each changed production file.

**Full non-live checks**

- Run `./test/run-ci.sh main db` and `./test/run-ci.sh all`; static cleanliness is not a substitute for the full ERT, byte-compile, package-lint, checkdoc, and architecture checks.

**Podman/JDBC live gate**

- Run `CLUTCH_TEST_CONTAINER_RUNTIME=podman ./test/run-ci.sh native-live` as regression protection after each coherent style batch that touches production code.
- Run `./test/run-ci.sh db-live` when configured, especially if a conditional in a backend adapter was corrected. With no configured JDBC endpoint, report the gate as unavailable rather than claiming that static style cleanup proves JDBC behavior.

### Phase 5 — Triage structural findings as candidates, not automatic defects

**Exact scope**

- Start with inventory and ownership review, not refactoring. Candidate high-arity functions include `clutch-result--init-state`, `clutch-result--display-select`, and `clutch-record--render-field` in `clutch-result.el`, `clutch--start-table-metadata-request` in `clutch-schema.el`, and the five-argument completion/context functions in `clutch-sql.el`. Candidate longer functions include `clutch-result-edit--open-cell`, `clutch-mongodb--execute-collection-method`, `clutch--object-entry-reader`, `clutch--agent-context-text`, and `clutch-completion-at-point`; confirm source spans with a parser rather than a line-count-only script.
- Review repeated accessors in context/render code, such as repeated plist reads around `clutch-object.el:1545-1549`, only when destructuring would remove duplication and clarify ownership. Review manual accumulation in `clutch-sql.el:1121-1131` and MongoDB field collection in `clutch-document.el:342-351` only when `cl-loop` or another shape genuinely makes the invariant clearer.
- Treat missing Edebug metadata on untouched macros—especially test-only helpers and generated syntax—as a candidate for the next time that macro is modified, not as a mass touch-only edit. `clutch--with-object-error-capture` is the Phase 2 exception because its expansion and hygiene are an active behavioral change.
- Do not refactor a generic backend method, callback signature, lifecycle boundary, or stable public contract solely because it has more than four positional arguments. Do not split a function merely to reduce line count, and do not create a general context/utility module for one call site.

**Dependencies**

- All preceding phases and their live gates must be green; a structural refactor must not be used to conceal an unresolved CAPF, error, documentation, or style defect.
- A candidate becomes implementation scope only after a maintainer names the duplicated rule or ownership problem, identifies all call sites/tests, and writes a focused red reproducer when behavior can change.
- `postmortem/130` and `133` supply the stopping rule: abstractions must reduce ownership ambiguity or real duplication, not merely improve a metric.

**Acceptance criteria**

- Every reviewed candidate is classified as keep, simplify in place, or refactor with a named owner and a reason; rejected candidates remain unchanged without a “cleanup” shim.
- Accepted API migrations preserve generic/callback contracts unless their owning contract is intentionally changed and all implementations/callers/tests are updated together. No positional-arity count alone is treated as a defect.
- Any accepted behavioral refactor has a fail-first reproducer, focused public-path tests, a full non-live pass, and the post-change native-live gate. Inventory-only conclusions have static evidence and live regression protection but make no claim that a refactor is needed.

**Focused checks**

- Re-run the arity/source-span inventory with an Emacs reader or equivalent syntax-aware tool, then use `rg` to enumerate every caller before proposing a signature change.
- For accepted candidates, run the owning module's focused selector and `./test/run-ci.sh byte-compile checkdoc architecture`; for rejected candidates, run the static inventory and record the rejection reason in review notes.
- Use `rg -n "clutch--completion-column-candidates|clutch--start-table-metadata-request|clutch-result--display-select|clutch-record--render-field" clutch*.el test/*.el` to confirm no hidden call site was missed.

**Full non-live checks**

- Run `./test/run-ci.sh main db` and `./test/run-ci.sh all` after every accepted structural change, including all package-quality and architecture checks.

**Podman/JDBC live gate**

- Run `CLUTCH_TEST_CONTAINER_RUNTIME=podman ./test/run-ci.sh native-live` after every accepted production refactor; a static arity or function-length result never replaces the full native gate.
- Run `./test/run-ci.sh db-live` when configured for any changed backend or generic contract. If no JDBC live environment exists, record that as a remaining risk and do not promote a static refactor to evidence of JDBC correctness.

## Safe migration approach for long positional argument lists

Use this sequence only for a Phase 5 candidate that survives ownership review.

1. Leave generic and callback contracts alone by default. `cl-defgeneric`/`cl-defmethod` backend APIs such as `clutch-db-build-paged-sql`, metadata callbacks, and externally dispatched functions are contracts, not private convenience functions; changing them requires a separate contract decision, all implementer/caller updates, and focused dispatch tests.
2. For a private function whose required context is stable but whose option tail is hard to read, prefer `cl-defun` keyword arguments for the option tail, keeping genuinely required identity arguments positional. Use a clearly documented plist/alist only when it crosses an existing boundary as data.
3. Use a struct only for stable context that crosses a module or lifecycle boundary and owns an invariant, such as a result/render or metadata request context that has multiple consumers. Do not create a struct for a one-use call or as a renamed accessor wrapper.
4. Use a closure or `cl-labels` for a local helper that captures one operation's context. Do not export a callback or add positional arguments merely to avoid a small local closure.
5. Sequence compatibility deliberately: inventory and update all in-repository callers and tests with the definition in one reviewable change, run the focused suite, then remove the obsolete positional form. If a genuinely external public caller prevents an atomic migration, introduce a documented, tested compatibility boundary with an explicit removal condition; never add a silent fallback or duplicate lookup. Internal private APIs should not retain a permanent compatibility shim.
6. Re-run byte compilation to catch callback/keyword arity mistakes, run the full non-live suite, and follow every behavioral migration with `./test/run-ci.sh native-live` plus any configured JDBC live gate.

## Completion condition

The plan is complete only when each accepted behavioral change has a fail-first reproducer and post-fix native-live evidence, each static/documentation change has static proof plus live regression protection, Oracle synchronous `search-tables` remains explicitly blocked until its own live reproduction exists, and every Phase 5 structural candidate has a recorded keep/simplify/refactor decision rather than an automatic rewrite.
