# 168 -- A Lost Transaction Stops Transaction Commands

## Background

Every transaction command started with `clutch--ensure-connection`, which reconnects a dead session from stored parameters. That is the right default for queries: the statement can simply run again on the replacement connection.

It is the wrong default for `clutch-commit`. When a manual-commit session dies, the server has already rolled its open transaction back. Reconnecting produces a session whose transaction is empty, so `COMMIT` succeeds and the command reports "Transaction committed" for work that no longer exists. `clutch-rollback` and `clutch-toggle-auto-commit` shared the same entry point and the same false success.

Ordinary reconnects had a quieter form of the same problem. `clutch--try-reconnect` cleared the dirty flag and reported only "Reconnected", so a user who had staged DML through a manual transaction saw no indication that it was gone.

## Decision

Transaction commands use `clutch--ensure-transaction-connection`, which refuses to paper over the loss: if the session is dead and was dirty, it reports the loss and stops before any RPC. It does not reconnect. The dirty flag is cleared and open DML result buffers get the existing rollback banner as part of reporting, so the state is honest and the next command reconnects normally rather than repeating the error forever.

`clutch--try-reconnect` keeps reconnecting, because query workflows should recover, but it now says that uncommitted changes were lost and marks the affected result buffers.

Detection is `clutch--lost-transaction-p`: dirty and not live. Dirty state is only ever set in manual-commit mode, so the predicate needs no separate mode check and does no I/O on a dead handle.

## Tradeoff

A user whose connection dropped mid-transaction now gets an error from `C-c C-m` instead of a success message. That is the point: the alternative silently converts data loss into apparent success. The cost is one extra keystroke to retry once the loss is acknowledged.

The guard cannot distinguish a server-side rollback from a network drop that left the transaction alive on the server. Both are reported as lost, which is the safe direction: Clutch cannot commit through a connection it no longer holds, and claiming otherwise is the failure being removed.
