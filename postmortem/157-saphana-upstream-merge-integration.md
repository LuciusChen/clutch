# 157 — Integrating the SAP HANA backend across an upstream refactor merge

## Background

The `xiaobing-huang/clutch` fork carries one feature not in upstream
(`LuciusChen/clutch`): the SAP HANA JDBC backend (`clutch-db-saphana.el`,
`.hana.gpg` auth-source discovery, `currentSchema` URL mapping, HANA-tuned
timeouts and lazy schema enumeration).

Upstream advanced ~70 commits — a large ownership/dependency refactor that
introduced `clutch-diagnostics.el`, an architecture-guard CI gate
(`test/check-architecture.el`), and changed several JDBC method signatures.
Merging upstream into the fork produced six conflicting files, all of which
the SAP HANA commit had also touched.

## Decisions

**Adopt upstream structure, re-apply HANA on top.** Every conflict resolved
by taking upstream's refactored form as the base and re-inserting the HANA
additions — not the reverse. Concretely:

- `clutch-db-row-identity-candidates` adopted upstream's new
  `(conn table &optional schema catalog)` signature plus metadata-conn
  scoping; the HANA "skip the unique-not-null fallback" guard was re-inserted
  operating on the *scoped* `metadata-conn`, not the raw `conn`.
- The obsolete `clutch-jdbc--jdbc-drivers` defconst was dropped: upstream now
  registers backends by iterating `clutch-jdbc--driver-metadata`, and
  `saphana` already lives there.
- `PRD.md` was rewritten wholesale by upstream (1176 → 94 lines); took theirs
  and added HANA only to the core-SQL surface row.

**Rework the HANA feature to satisfy the new architecture guard.** The guard
(absent from the fork's base) forbids workflow modules from depending on the
composition root and caps cross-module declarations. Two HANA mechanisms
violated it:

1. `clutch--ensure-clutch-loaded` called `(require 'clutch)` from workflow
   modules. It was redundant — every interactive entry command autoloads from
   `"clutch"`, which already loads all modules (including
   `clutch-db-saphana`, registering the discovery source) before any reader
   runs. Deleted it and its call sites.
2. The console persistence-identity helpers (`clutch--console-*`) lived in
   `clutch-query.el`, forcing `clutch-connection.el` to forward-declare into
   `clutch-query` for its endpoint-drift guard. Moved the helpers *down* into
   `clutch-connection.el`; `clutch-query` reuses them through its existing
   dependency on connection. This is the correct dependency direction and
   removed the cross-declaration.

## Bugs the merge surfaced (pre-existing in the fork)

- `clutch-jdbc--apply-timeout-defaults` had been changed to a two-arg
  `(driver params)` signature but its test still called it with one arg — the
  test was already red in the fork. Fixed the test and extended it to cover
  HANA's elevated timeout defaults.
- `clutch--ensure-clutch-loaded` was silently dropped by the auto-merge
  (upstream refactored the surrounding region); byte-compile caught the
  dangling call before it was removed properly.

## Discovery error handling is a boundary, not business logic

`clutch--external-connection-entries` wraps each registered discovery source
in `condition-case`. AGENTS.md forbids `condition-case` around internal
calls — but this hook holds arbitrary user-registered functions, so it is a
genuine error boundary, analogous to `run-hook-with-args` isolating a bad
hook. One corrupt `.hana.gpg` must not break the whole picker (which also
serves static `clutch-connection-alist` entries), and the failure is surfaced
via `message`, never swallowed. Kept the behavior; added a comment tying it
to the boundary rationale, backed by
`clutch-db-test-saphana-discovery-error-surfaces`.

## Deferred / documented limitations

- HANA object enumeration skips `indexes` / `procedures` / `functions`
  (`getIndexInfo` is slow and monitor-serialized; `getProcedures` /
  `getFunctions` reference an ANSI `SPECIFIC_NAME` column HANA does not
  expose). Documented in `docs/saphana-backend.org`.
- HANA row identity stops at the primary key — no non-null-unique-key
  fallback — for the same `getIndexInfo` cost reason. `saphana-backend.org`
  was corrected to match (it had described the generic PK-then-unique policy).
- The `:secret` thunk is resolved eagerly at discovery time (stored as
  `:password`) because `~/.hana.gpg` is normally not in the global
  `auth-sources`; a connect-time lookup would fail. Documented.

## Post-merge follow-up: runtime schema switching

`clutch-switch-schema` was Oracle-only for JDBC — `clutch-db-list-schemas` and
`clutch-db-set-current-schema` had a single Oracle `cond` branch, so HANA hit
"Runtime schema switching is not available." Added HANA support:

- **Listing** uses a bounded `SELECT SCHEMA_NAME FROM SYS.SCHEMAS`, not
  `DatabaseMetaData.getSchemas`, to avoid enumerating thousands of internal
  containers on a large tenant. System / `_SYS_*` schemas are filtered.
- **Switching** was first written to reuse the agent's `set-current-schema`
  RPC (as Oracle does). **Live testing against a real HANA Cloud endpoint
  proved that wrong**: the `ngdbc` driver throws
  `SQLException: Runtime schema switching is not available for this connection`
  from the JDBC `Connection.setSchema` path the RPC uses. A direct
  `SET SCHEMA "name"` statement works and correctly changes `CURRENT_SCHEMA`.
  The implementation now issues `SET SCHEMA` via `clutch-db-query` with
  `clutch-db-escape-identifier`. This is a case where unit tests (which mocked
  the RPC) passed but the real driver behaved differently — the live run was
  essential.

