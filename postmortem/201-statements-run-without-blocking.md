# 201 — Statements Run Without Blocking Emacs

## Evidence

Issue #25 asked for queries that do not freeze Emacs, and a PostgreSQL user raised it again. Every statement waited in `accept-process-output`, so a long query held the whole editor until it finished or the client-side idle timeout expired, and that timeout misreported outcomes. On PostgreSQL 16, an `UPDATE` that slept four seconds under a two-second `:read-idle-timeout` was reported as failed and its connection closed without a cancel, while the row was updated anyway (n went from 0 to 1). A quit whose cancel did not reach the statement in time was shown as "Query interrupted" while the `UPDATE` committed (n went from 1 to 11).

## Decision

One callback pipeline serves the console, the REPL and statement batches. `clutch-db-query-async` starts a statement and returns non-nil when the backend can wait for it without blocking. A backend that cannot returns nil, and the statement runs synchronously with its callback called before `clutch--run-db-query-async` returns, so that backend behaves as before. PostgreSQL starts statements through `pgsql-exec-async`, detected with `fboundp`, and JDBC through a foreground agent request without a client timeout. The outcome is presented from an idle timer, never inside another command's wait.

Exactly-once completion is a backend contract. pgsql.el finishes a pending request once, and every path that ends a JDBC foreground request (its reply, a cleared callback table, an exiting agent) removes the request before notifying it. Both have tests; Clutch adds no second guard.

While a statement runs, `clutch--running-queries` records it, other foreground commands on its connection are refused, and its first line shows an amber dot in the fringe (`●` in a terminal margin). `C-g` in the console, REPL or result buffer runs `clutch-cancel-query-or-quit`, which cancels through `clutch-db-interrupt-query` and shows a red square until the statement reports the server's verdict: the cancellation error, or its result if it finished first. Nothing is retried once it may have reached the server; reconnect-and-retry still happens only when the JDBC agent proves the statement did not start. A batch confirms every risky statement before the first one runs, since a prompt between asynchronous statements would come from a timer while the user may be working elsewhere.

## Rejected alternatives

### Lisp threads

A process belongs to the thread that created it, so a worker thread cannot read a connection opened by the main thread, and moving connections between threads would touch every backend.

### Synchronous and asynchronous pipelines side by side

A defcustom choosing between them would keep two execution paths, two sets of tests and two meanings of `C-g`. A backend that cannot wait without blocking already fits the single pipeline by declining.

### A client-side timeout for asynchronous statements

After the idle timeout fires, the only safe action is closing the connection, which is how committed statements were reported as failed. Without it an asynchronous statement ends with the server's answer, and the database-side statement timeout still applies.

## Consequence

Statements on PostgreSQL, with a pgsql.el that provides `pgsql-exec-async`, and on JDBC connections no longer block Emacs; other backends, including MySQL until mysql.el gains an asynchronous API, block as before. JDBC still fetches the rest of a result page synchronously after the statement finishes. The default 30-second `clutch-query-timeout-seconds`, and the JDBC drivers' network timeout, which equals `:read-idle-timeout`, still end long statements on those backends; whether to relax them is deferred. pgsql.el's synchronous API, which metadata queries still use, closes the connection on its idle timeout without cancelling the statement first.
