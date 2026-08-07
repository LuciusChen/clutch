# 177 — Staged Submit Follows Transaction Mode

## Context

Clutch's result editor accumulates INSERT, UPDATE, and DELETE changes locally before `C-c C-c` submits them. A fail-closed change in 0.3.0 rejected a multi-statement submission whenever the connection was in auto-commit mode and the backend lacked a dedicated batch method, requiring users to switch transaction mode before performing an ordinary grid edit. That requirement was not part of the requested workflow and diverged from the DataGrip model Clutch's transaction controls otherwise follow.

## Decision

Submitting staged result edits follows the connection's existing transaction mode:

- In auto-commit mode, `C-c C-c` submits the complete local batch and commits it automatically when the backend supports transactions; an execution failure rolls that submission back and restores auto-commit.
- In manual-commit mode, `C-c C-c` submits the complete local batch through a savepoint inside the current transaction without committing it or requiring the transaction to be clean; `C-c C-m` and `C-c C-u` remain the user's explicit commit and rollback controls.

The existing `clutch-db-call-with-atomic-batch` backend contract owns both boundaries because SQLite, native SQL clients, and JDBC expose different transaction primitives. Native MySQL and PostgreSQL use explicit short transactions in Auto mode and SQL savepoints in Manual mode; SQLite uses its transaction API; JDBC temporarily changes only the remote connection state in Auto mode and uses standard `java.sql.Savepoint` objects through opaque agent handles in Manual mode. The result workflow owns statement ordering, affected-row validation, local pending state, and refresh behavior.

Native backends reject that short-lived boundary if the server reports a transaction already opened explicitly with SQL. Starting another boundary could commit the user's MySQL transaction implicitly or cause Clutch to commit the user's PostgreSQL transaction, so the user must either finish it or switch the connection to Manual mode before submitting.

Rename the public commands to `clutch-result-submit` and `clutch-result-insert-stage` without compatibility aliases. Their former `clutch-result-commit` and `clutch-result-insert-commit` names confused submission or local staging with `clutch-commit`, which alone commits a Manual-mode server transaction. The `C-c C-c` bindings do not change.

## Failure Ownership

An Auto-mode submission uses backend transaction primitives internally, but a successful submission does not change the user-selected transaction mode. Only JDBC requires a temporary remote `setAutoCommit(false)` transition. If restoring that remote state fails after commit or rollback, the error carries the known outcome and the UI exposes the remaining Manual state honestly: committed work clears local staged state so a retry cannot duplicate it, while rolled-back work retains local staged state.

In Manual mode, the savepoint is created before the first staged statement. A later statement failure rolls back and releases that savepoint, preserving any work that entered the outer transaction before this submission. Local dirty-state publication is deferred until the complete batch succeeds, so a successfully recovered failure leaves the prior transaction state unchanged and retains the entire staged batch for a safe retry.

Statement failure, savepoint completion, and transaction completion have different evidence. A statement failure is safe to retry only after rollback succeeds. A failed savepoint release can still be made safe by rolling back to that savepoint, so Clutch attempts that recovery and then re-reports the release error. A failed `COMMIT` is different: the server may have committed before the response was lost, and a later successful rollback cannot prove otherwise. Clutch therefore does not issue rollback after a commit error.

If rollback/savepoint recovery fails, or an Auto-mode commit outcome is unknown, Clutch records an `uncertain` transaction state, retains local staging, and blocks further queries, commit, and transaction-mode changes until the user explicitly rolls back or reconnects. It does not silently roll back the outer transaction because that could discard unrelated user work.

Rollback and reconnect recover session usability, not historical knowledge. In particular, a successful rollback after a commit error cannot prove that the earlier commit did not happen. Clutch therefore does not mark prior DML as rolled back in this path, does not report a dead uncertain session as known-lost work, and tells the user to verify the database before retrying retained staging. Known dirty work on a dead manual transaction remains a separate case and is still reported as discarded.

The same commit-outcome rule applies to the public `clutch-commit` command. Keeping explicit commit on the older dirty-only path would reintroduce the same false certainty when its response is lost.

## Cleanup

The replacement statement-plist backend protocol, generic transaction-mode inference, autocommit rejection, dirty-manual-transaction rejection, and result-layer rollback helper are removed rather than retained as compatibility paths. Current documentation describes transaction-mode behavior; the 0.3.0 changelog remains a historical record of the behavior released at that time.

## Release Coordination

Agent 0.2.18 was published before enabling this JDBC path. Clutch's `clutch-jdbc-agent-version` and `clutch-jdbc-agent-sha256` were then updated together from a freshly downloaded release asset; a local build hash is not a release pin.

## Verification

Regression coverage must drive `clutch-result-submit` in both modes. Auto mode must validate inside the backend boundary, commit on success, roll back statement failures, and treat a commit error as unknown without issuing a second rollback. Manual mode must preserve earlier transaction work, undo the successful prefix of a failed submission, recover a failed savepoint release when possible, retain local staging, and retry each corrected statement exactly once without toggling or committing. SQLite retains a real-database atomicity test; unit tests exercise native and JDBC transaction/savepoint boundaries, restore outcomes, unsupported capabilities, and uncertain outcomes; native live tests cover the MySQL and PostgreSQL result workflow.
