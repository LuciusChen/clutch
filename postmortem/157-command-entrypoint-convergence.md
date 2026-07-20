# 157 — Entrypoint and Namespace Capability Convergence

## Context

Three command surfaces still exposed implementation branches or overlapping workflows after the broader public-surface cleanup:

- `clutch-switch-schema` already represented the current namespace, but the ClickHouse-only reconnect implementation remained public as `clutch-switch-database`.
- `clutch-query-console` opened or reused a console by connection target, while `clutch-switch-console` separately selected open console buffers.  The first picker could not select an open ad hoc console, so users needed two commands for one console workflow.
- `clutch-execute` executed the current line from an arbitrary buffer through whichever live Clutch connection happened to be found first.  It was not bound or documented, and it bypassed the connection-local execution model retained by `clutch-execute-dwim`, `clutch-execute-region`, and `clutch-execute-buffer`.

## Decision

- Keep `clutch-switch-schema` as the only public namespace-switch command.  Backend adapters own candidate enumeration, the effective current value, in-session switching, and any connection-parameter projection.  The command layer only chooses a candidate and orchestrates either an in-session transition or replacement parameters returned by the backend.
- Treat switchability as a verified capability, not an inference from a backend having something called a database or schema.  MongoDB enumerates visible databases through the public `mongodb-list-databases` API.  DuckDB enumerates schemas only in its current file-backed catalog.  Redis exposes its current logical database but does not claim switching because it cannot reliably enumerate the configured range under ordinary ACLs.
- Make `clutch-query-console` the only public console picker.  Its candidates include open consoles, saved connections, and the existing unmatched-input path for a new temporary connection.  An open saved console and its saved profile form one candidate; open ad hoc consoles remain directly selectable.
- Remove `clutch-switch-console` rather than retaining an alias.
- Keep only `clutch-execute-dwim`, `clutch-execute-region`, and `clutch-execute-buffer` as public query execution commands.  Remove `clutch-execute`; the explicit indirect-edit workflow remains available for SQL embedded in another source buffer.

## Why

ClickHouse requires different mechanics, not a different user concept.  Its JDBC adapter supplies `SHOW DATABASES` candidates and replacement `:database` parameters, so the command layer no longer names or queries ClickHouse directly.  This preserves one namespace entrypoint without pretending that HTTP-backed ClickHouse connections support session-local `USE` semantics.

JDBC itself does not provide one portable namespace-switch model.  Products variously expose schemas, catalogs, session SQL, or connection properties, and some distinguish database switching from schema switching.  DuckDB is already a documented, user-used generic JDBC path, so leaving the unified command unavailable there would be a real capability gap.  Its file-backed current-catalog schemas can switch in place, but pure in-memory databases and attached catalogs cannot be advertised honestly: both are primary-session state while the metadata sidecar deliberately uses a second JDBC session.  Fixing that limitation would require an explicit sidecar/session design, not an Elisp metadata fallback.

Candidate strings remain canonical backend names, not pre-quoted SQL fragments.  The adapter escapes an identifier only when executing a switch.  This keeps completion values, stored state, reconnect parameters, and metadata scope in one representation.

Console selection is likewise one concept.  Combining open and saved targets makes the primary command complete instead of preserving a second buffer-only picker.  Matching open saved consoles by persistence identity also avoids presenting the same logical target twice after a profile rename.

Direct arbitrary-buffer execution had a less predictable boundary model than every visible query workflow and provided no editing checkpoint.  Removing it restores the public command set recorded in postmortem 130.  The separate indirect-edit workflow remains intentional: it opens a visible Clutch buffer, lets the user review and edit embedded SQL, and then executes through that buffer's bound connection.

## Consequence

This removes the public commands `clutch-switch-database`, `clutch-switch-console`, and `clutch-execute`.  Users should call `clutch-switch-schema`, `clutch-query-console`, and one of the three retained scope-specific execution commands.  ClickHouse database switching, MongoDB database switching, and DuckDB current-catalog schema switching all enter through `clutch-switch-schema`; open ad hoc consoles are available from `clutch-query-console`.  Attached DuckDB catalogs and Redis runtime database selection remain explicit limitations rather than misleading picker candidates.
