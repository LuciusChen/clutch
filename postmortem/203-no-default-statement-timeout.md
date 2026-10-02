# 203 — No Default Statement Timeout

## Evidence

Postmortem 201 made statements run without blocking Emacs and left two limits in place, deferring whether to relax them: the default 30-second `clutch-query-timeout-seconds`, which JDBC also clamped to the RPC timeout less five seconds, and the JDBC network timeout, which agent 0.2.25 set on every session from `:read-idle-timeout`. With Emacs no longer blocked and `C-g` cancelling, these limits only stopped legitimate long statements, such as reports, index builds and bulk updates: after 30 seconds on PostgreSQL, and after 25 on JDBC, where the clamp applied although nothing waited for the reply. On JDBC the network timeout could also end a statement that kept its socket silent for 30 seconds, usually taking the connection with it. MySQL statements already had no limit.

A review of the first proposal found two more limits. Agent 0.2.25 fell back to its own 29 seconds whenever a request omitted `query-timeout-seconds` or sent 0, so a nil default alone changed nothing on JDBC. And requests Emacs still waits for, such as paging, counting, exporting, prepared writes and fetching the rest of a page, relied on the clamp, which returned nothing without a configured limit.

## Decision

- `clutch-query-timeout-seconds` defaults to nil: Clutch adds no limit of its own, and a limit configured on the server still applies. PostgreSQL sets `statement_timeout` only for a number, as before, so 0 there still lifts a server-configured limit.
- A JDBC statement that Clutch runs without blocking sends its configured limit as is, or 0 without one. Agent 0.2.26 reads 0 as no limit and waits until the statement ends, is cancelled or is force-disconnected; omission keeps its 29 seconds for older clients. It also sets the network timeout on the metadata and bulk sessions only.
- A JDBC request that Emacs waits for always sends a positive timeout: the configured limit when shorter, else the RPC timeout less five seconds, but at least one second. With nil or 0 it keeps the budget it had.
- Disconnecting a JDBC connection whose statement is running uses force-disconnect. The ordinary disconnect queues behind the statement's lock in the agent, so Emacs waited five seconds and the agent kept the session. The statement then reports that its outcome is unknown, and nothing is replayed.

## Limits

- The budget does not strictly bound a waited-on request. The agent times the execution and the first batch separately, each one second past the budget, so a slow first batch after a slow statement can still reach the RPC timeout, which then retires the connection as before. A strict bound would need one deadline shared by both.
- A statement without a limit holds its connection and any locks it takes until it ends, and on JDBC also an agent request thread and one of its 16 execution slots. Force-disconnect removes the logical connection at once, but a driver that ignores both cancel and close keeps those threads until the agent restarts, as agent postmortem 028 records.
