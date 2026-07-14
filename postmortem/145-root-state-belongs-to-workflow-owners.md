# 145 — Root State Belongs to Workflow Owners

## Problem

The package entry point still defined three pieces of query execution context:
the source window and the buffer positions of the SQL being executed.  Query
code was their only writer and policy owner, while result code consumed the
source window after already requiring query.  Keeping the definitions in
`clutch.el` made the composition root participate in implementation lifecycle
state and left direct module loads dependent on later root initialization.

Two immutable insert placeholder sentinels had the same problem.  They were
defined in the root after `clutch-ui.el` loaded, although UI was their only
production consumer.  Loading UI directly therefore left its own constants
uninitialized.

## Decision

Move the three dynamically bound execution variables to `clutch-query.el`.
They remain ordinary `defvar` forms so their dynamic binding behavior is
unchanged.  Result already requires query and no longer repeats a declaration
for the source window.

Move the two immutable placeholder constants to `clutch-ui.el`, replacing its
forward declarations with their real definitions.  Constants stay outside the
mutable-state architecture metric, but their load-time ownership is now
complete.

No dependency edge, accessor, callback, or state container was introduced.

## Consequences

- Mutable state in the composition root falls from 14 definitions to 11.
- Cross-module declarations remain 43 and the largest SCC remains two.
- Query and UI initialize the state and constants required by their direct
  consumers even when loaded independently of `clutch.el`.
- Existing dynamic bindings in query execution, tests, and result placement
  retain their previous scope and values.
