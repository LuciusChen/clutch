# 135 — The Entrypoint Assembles but Is Not Re-entered

## Problem

`clutch.el` assembled the package, but workflow modules also reached back into
it.  Connection and query readers dynamically required `clutch` so user setup
attached to `with-eval-after-load` ran before they inspected saved connections.
The query workflow also declared the root-owned SQL mode, schema switch, and
main dispatch commands.

That made the entrypoint a member of the six-module workflow cycle.  It also
made package loading depend on where an autoloaded command happened to be
implemented: public commands could load a split implementation file first,
which then repaired the load order by requiring the entrypoint from below.

## Decision

The package entrypoint is now a one-way composition root.

- Public workflow autoloads explicitly target `clutch`, so package setup runs
  before an interactive reader without a runtime child-to-root require.
- The SQL mode, REPL, and main dispatch live with the query workflow.
- Schema and database switching, including command connection context, live
  with the connection lifecycle.
- Query directly requires its SQL and UI foundations instead of relying on the
  root's incidental load order.
- Architecture checks reject every implementation dependency whose target is
  `clutch`, verify generated autoload targets, and prove direct workflow-module
  loading does not load the entrypoint.

No callback registry, setup hook, forwarding command, or accessor facade was
added.  Ownership moved with the complete workflows, while `clutch.el` remains
the public configuration and assembly surface.

## Why Autoloads Own the Boundary

The old runtime guard solved a packaging problem inside business logic.  Moving
the guard, renaming it, or adding a registration hook would preserve the wrong
dependency direction.  Autoload metadata is the actual public load boundary:
an `M-x` entrypoint should assemble the package, while an explicit internal
`require` should load only that module's declared dependencies.

Generated loaddefs are tested rather than source comments because a later bare
autoload cookie can silently override an earlier root-routed declaration.

## Test Budget

The reader test that mocked `featurep` and `require` existed only to lock in the
compensating runtime guard, so it was replaced by the autoload and no-reentry
contracts.  Schema-switch tests moved from the object suite to the connection
suite.  Oracle and MySQL success cases share one table-driven test, and the
ClickHouse test now verifies the delegation boundary instead of repeating the
already-covered connection replacement lifecycle.

## Consequences

- Dependencies into `clutch` fell to zero.
- The largest strongly connected component fell from six modules to five.
- Cross-module declarations fell from 140 to 137.
- `clutch.el` fell from 976 lines before the UI/root cleanup sequence to roughly
  520 lines and no longer owns query interaction or connection switching.
- Public key bindings and commands retain their existing behavior, including
  SQL, MongoDB, and Redis dispatch routes.

## Deferred Boundary

The remaining five-module component is the real query/result/edit/object/
connection workflow cycle.  Several buffer-local variables and public options
are still defined in `clutch.el` and consumed by their owner modules.  Those
globals do not create dependency edges in the current reader, but their
ownership should be corrected before claiming the root is purely declarative.
They are kept out of this change because relocating state lifetimes and breaking
the remaining workflow cycle require separate invariants; mixing them here
would make the autoload and composition-root result harder to verify.
