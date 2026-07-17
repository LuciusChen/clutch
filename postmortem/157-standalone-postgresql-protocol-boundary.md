# 157 — Standalone PostgreSQL protocol boundary

## Context

Clutch previously adapted `pg-el` while also carrying caller-side workarounds for response synchronization, type conversion, cancellation, and transaction state. Those compensations blurred ownership: protocol behavior could change only by adding more code to the UI package, while Clutch tests learned details of an external client's representation.

## Decision

PostgreSQL protocol ownership moves to the independent [`pgsql.el`](https://github.com/LuciusChen/pgsql.el) repository. It owns TCP and TLS setup, authentication, framing, response synchronization, type codecs, server errors, transaction status, and cancellation. Clutch retains only its PostgreSQL adapter, metadata SQL, query-console behavior, result conversion, and manual-transaction workflow.

The repositories evolve independently. `pgsql.el` has its own package metadata, development guide, deterministic protocol tests, live PostgreSQL tests, release history, and public `pgsql-` namespace; it must not depend on Clutch or acquire caller-specific branches.

## Opaque public boundary

Clutch may require `pgsql` lazily and call documented public `pgsql-` functions. PostgreSQL connection and result values remain opaque: Clutch must not call `pgsql--*`, inspect struct slots, manufacture protocol records in product code or tests, or duplicate wire, authentication, TLS, parser, retry, and synchronization logic. When the adapter needs a missing capability, the capability is added to `pgsql.el` as a tested public API before Clutch consumes it.

Clutch owns a small adapter connection wrapper only for Clutch lifecycle state such as the selected schema and manual-commit mode. That wrapper does not expose or reinterpret protocol transport state; `ReadyForQuery` remains authoritative through `pgsql-transaction-status`.

## Migration and rollback

The migration stays on `refactor/pgsql`, branched from Clutch main, while Clutch main retains the released PostgreSQL path. Integration CI checks out `pgsql.el` explicitly and does not install `pg-el`, so an accidental old-API dependency fails rather than hiding behind the developer's package state.

No runtime backend chooser, compatibility shim, or dual protocol stack is added. Before merge, rollback means abandoning or rebasing the integration branch without affecting Clutch main; after merge, a release-blocking regression is handled by reverting the migration as one unit, not by accumulating conditional fallbacks in the adapter.

## Merge conditions

- `pgsql.el` has a tagged, installable release with its intended public contract documented and package quality gates passing on the supported Emacs baseline.
- Deterministic and PostgreSQL live tests prove authentication, TLS modes, parameter binding, SQL NULL versus boolean false, arrays, exact values, structured errors, transaction states, cancellation, and connection reuse after every synchronized outcome.
- Clutch uses only public `pgsql-` APIs and opaque values; private-symbol and representation scans return no dependency leaks.
- Clutch's full non-live gate and native PostgreSQL integration pass, including metadata, parameterized mutation, manual transactions, query timeout, and `C-g` cancellation followed by reuse of the same session.
- Installation, CI checkout, upgrade, and rollback instructions match the code, and the old pg-el workarounds are removed instead of retained as dormant alternatives.

## Consequences

The protocol library becomes reusable outside Clutch and can be reviewed, tested, and released at the layer where PostgreSQL correctness belongs. Clutch becomes smaller and its adapter contract clearer, at the cost of one separately installed optional package and a deliberate cross-repository release gate before the migration reaches main.
