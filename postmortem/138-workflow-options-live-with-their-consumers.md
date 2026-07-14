# 138 — Workflow Options Live With Their Consumers

## Problem

After connection state left the composition root, `clutch.el` still defined
eight options that each had exactly one consuming workflow.  Their owner
modules carried `defvar` declarations while the root carried the defaults,
types, and documentation.

This split made the entrypoint look like a central configuration service and
allowed option ownership to drift without changing dependency-graph metrics.
It also meant a directly loaded owner consumed a variable whose Custom
definition depended on incidental root assembly.

## Decision

Each single-consumer option now lives with that consumer:

- query owns console persistence and yank cleanup;
- schema owns refresh scheduling and incremental cache installation;
- SQL owns completion case policy;
- edit owns insert-validation delay;
- object owns warmup delay and primary object types.

The public symbols, defaults, Custom types, groups, and documentation are
unchanged.  The root still defines the shared `clutch` group before assembling
the modules.  Direct owner loads may register options in that group without
loading the root.

No option was moved when its policy spans workflows.  In particular, timeout
configuration and result row limits remain deferred rather than being assigned
to a convenient but incomplete owner.

## Architecture Budget

The composition-root state ceiling fell from 30 definitions to 22.  The owner
test reads every module once, then verifies each protected option has exactly
one state definition of the expected kind and in the expected module.  It also
rejects a second `defvar` fallback for the newly moved options; connection
context continues to permit intentional forward declarations while requiring a
unique `defcustom` or `defvar-local` owner.

The existing standalone workflow-load test now loads query, edit, and object,
covering all five option owners without a subprocess or a new per-option test
matrix.  This keeps the guard proportional to the state it protects.

## Consequences

- `clutch.el` fell from 402 lines to 345.
- Root mutable state fell from 30 definitions to 22.
- One unused backend fallback and eight owner-side declarations disappeared.
- Cross-module declarations remain 136 and the largest SCC remains three.
- The architecture owner test still counts as one ERT case and scans source
  once, despite covering thirteen protected symbols including connection state.

## Deferred Boundary

The remaining root state includes shared result/UI policy, query/result
lifecycle state, diagnostics state, backend timeouts, and the public mode.  Each
group needs its own load and lifetime decision.  Moving those definitions
mechanically would obscure rather than clarify ownership.
