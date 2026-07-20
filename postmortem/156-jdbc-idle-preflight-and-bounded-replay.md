# 156 — JDBC idle preflight and bounded replay

## Context

Primary-session invalidation stopped repeated `Unknown connection id` errors, but a laptop-sleep or server-idle disconnect still made the first query after returning fail before the next command reconnected. Replaying that failure based on Oracle messages such as `No more data to read from socket` would be unsafe and vendor-specific: the same text can occur after SQL reached the server, and even a `SELECT` may have side effects.

## Decision

Clutch sends a global `validate-after-idle-seconds` threshold to every JDBC session, defaulting to 300 seconds. The agent measures wall-clock time since primary foreground activity, so system sleep naturally counts as idle, but elapsed time only triggers validation; it does not classify the cause. Immediately before `execute` or `execute-params` creates or prepares a statement, the agent calls the standard `Connection.isValid(3)`. Metadata work does not refresh that primary timestamp, and unsupported driver validation preserves the previous execution path.

When validation proves the session invalid, the agent removes only that logical connection and returns both `connection-invalidated` and `execution-not-started`. The preflight phase ends before `createStatement` or `prepareStatement`, because preparation may contact the server. The query-console, REPL, and batch-statement workflow requires both protocol facts, reconnects through the existing logical-session path, and makes one direct second attempt; staged mutations retain the typed failure without transparent replay. Clutch does not loop, recurse, pool connections, run a heartbeat, issue dialect SQL, or replay a completed prefix of a multi-statement command.

## Transaction boundary

Auto-commit sessions and manual sessions that Clutch currently marks clean may take the single retry. A manual session marked dirty stops, preserves the original failure, and reports that the transaction outcome is unknown. This reuses the product's existing transaction indicator, which tracks successful DML and backend-declared schema effects; it cannot prove the absence of state created by arbitrary calls, side-effecting selects, or `SELECT ... FOR UPDATE`. Expanding SQL classification was rejected because it would imply certainty the client does not have. Users who depend on such untracked session state should disable idle validation or explicitly commit or roll back before leaving the session idle.

## Consequences

The common query-console, REPL, or batch resume-from-sleep path now behaves like native reconnect when the dead connection is detected before the new statement starts, including the existing minibuffer `Reconnected to ...` message. A successful validation followed by a statement-creation, preparation, execution, or fetch failure remains ambiguous and is never replayed; recovery still applies to the next command. The same generic contract covers Oracle, SQL Server, DB2, Snowflake, Redshift, ClickHouse, DuckDB, MongoDB SQL Interface, and generic JDBC drivers without backend-specific branches.
