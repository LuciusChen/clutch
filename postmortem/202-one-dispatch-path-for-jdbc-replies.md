# 202 — One Dispatch Path for JDBC Replies

## Evidence

The JDBC transport had two ways for a reply to reach its request. The agent filter handed a reply to the request registered under its id in `clutch-jdbc--async-callbacks`, which held metadata callbacks and, since postmortem 201, foreground statements, and appended every other reply to `clutch-jdbc--response-queue`. Each synchronous wait scanned that queue for its own id with `clutch-jdbc--take-queued-response` and put the rest back. Postmortem 154 already had to keep late replies out of the queue. An agent line that was not JSON became a synthetic response without an id, queued until the next synchronous wait took it, stopped the agent and reported it. Once foreground statements became asynchronous, such a line could leave a running statement waiting forever, because nothing that waited on the callback table ever looked at the queue.

## Decision

Every request registers in `clutch-jdbc--async-callbacks`, and the filter delivers each reply by its id. A synchronous wait registers a reply cell, which the filter fills directly, since no caller code runs when a cell is set; it waits until the cell is filled, the agent stops or the deadline passes, and removes its registration when it leaves, even through a quit, so a late reply is dropped. Foreground requests keep their handler and metadata requests their callbacks and timers. The queue and `clutch-jdbc--take-queued-response` are gone.

An unreadable line records its parse error in `clutch-jdbc--protocol-error`, and the filter stops the agent once it has finished with its buffer. Stopping notifies foreground requests as any agent exit does, and `clutch-jdbc--agent-exit-error-message` returns the recorded error before anything else, so synchronous waits and foreground statements both report it. Starting a new agent clears it.

The timeout rules of postmortem 171 are unchanged: a timeout with a live agent condemns only the owning connection, otherwise the agent stops, and the cancel wait still gives up on a quit without stopping the agent.

## Rejected alternative

### Deliver synchronous replies through handlers

A synchronous wait could have registered a handler like a foreground request. Handlers run from a zero-delay timer so that callers' code never runs inside the filter, and a synchronous wait would then depend on timers running during its own wait. Filling a cell inside the filter needs neither.

## Consequence

`clutch-db-jdbc.el` is 42 lines shorter. Tests feed agent output through `clutch-jdbc--agent-filter`, the path real replies take, instead of setting the queue. The registry keeps its name, `clutch-jdbc--async-callbacks`, although synchronous waits now register there as well.
