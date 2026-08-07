# 169 -- Session Teardown Is One Transition, Not One Struct

## Background

[168](168-lost-transaction-blocks-reconnect.md) fixed a commit that reported success for a transaction the server had already discarded. The defect was not a wrong step; it was a missing one. `clutch--try-reconnect` wrote its own end-of-session sequence and simply never told anyone the transaction was gone.

That suggested a familiar diagnosis: connection-scoped state is scattered, so pull it into one session object. An audit does not support it. Sixteen connection-keyed hash tables exist, but they are already owned by the module whose lifecycle they follow -- eight behind `clutch--clear-connection-metadata-caches` in `clutch-schema.el`, problem records in `clutch-diagnostics.el`, object caches in `clutch-object.el`, transport and transaction state in `clutch-connection.el`. Centralizing them would move state away from its owner and give `clutch-connection.el` reasons to reach into modules the dependency whitelist deliberately keeps it out of, with diagnostics as the clearest case: it is a leaf over the backend contract and must not acquire a connection dependency.

The scatter was also not what caused the bug. A struct holds data. It cannot notice that one caller skipped a step.

## Decision

Make the transition explicit instead of the state central. `clutch--session-teardown' takes the connection and a kind -- `disconnect', `dead', or `preserve' -- and performs the whole end-of-session sequence, with the differences between kinds written as guarded lines in one body. `clutch--do-disconnect', `clutch--cleanup-dead-connection', and `clutch--preserve-dead-connection-for-reconnect' keep their names as the domain vocabulary and become one call each. `clutch--clear-connection-client-state' was a partial aggregate of the same steps and is deleted rather than left as a second way to spell them.

A test asserts the step sequence for each kind. Adding a step to one caller instead of the shared transition now fails that test, which is the property the struct would not have provided.

Replacing a session stays outside this function. `clutch--try-reconnect' and `clutch--replace-connection' move attached buffers onto a new connection instead of ending the session: they keep DML results meaningful, rebind rather than invalidate, and clear problem capture rather than forget records. Folding them in would have left three shared lines under five conditionals, so the boundary is stated in the docstring instead.

## Tradeoff

Kind flags in one body read worse than three straight-line functions, and the guards must be read together to know what any single kind does. That is the point: the differences were previously discoverable only by diffing three functions by eye, and one of them was wrong.

The two replacement paths remain duplicated. They are variants of a single transition and deserve the same treatment, but they differ on more than they share today -- whether the old connection is still live, and how problem capture is handled -- so unifying them is its own change with its own decisions.
