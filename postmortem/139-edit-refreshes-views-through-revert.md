# 139 — Edit Refreshes Views Through the Revert Protocol

## Problem

The staged-mutation module called two higher-level workflow functions solely to
refresh views.  After a commit it invoked query's executor directly, and after
a field edit it invoked result's Record renderer directly.  Those calls made
edit a reverse dependency of both query and result, completing the remaining
three-module strongly connected component.

Neither call represented mutation ownership.  Edit owns staging, validation,
SQL mutation order, and pending-state cleanup; result and Record buffers own
how their current view is rebuilt.

## Decision

View owners expose the standard Emacs buffer-revert protocol:

- Result mode already installed `clutch-result--revert`.  Commit now clears all
  pending state and calls `revert-buffer`, leaving query selection and rerun
  behavior to the result owner.
- Record mode installs its renderer as `revert-buffer-function`; the renderer
  accepts the two conventional revert arguments while retaining ordinary
  no-argument calls.
- Edit refreshes a live Record return buffer through `revert-buffer`, then
  restores the existing column-property point location.

No callback was injected into edit buffers, and no executor facade, refresh
registry, forwarding helper, or integration module was added.

## Behavior Consequence

The result revert handler reruns `clutch-result--effective-query`.  Therefore a
commit made from a result with an active server-side `WHERE` filter now reloads
that filtered view.  The previous direct call used the raw last query and
silently discarded the view filter.  This correction is recorded in the
changelog because it is user visible.

## Test Budget

No ERT case was added.

- The existing mutation-order test replaces its executor mock with a local
  revert handler and verifies the handler observes edits, deletes, inserts, and
  row marks already cleared.
- The existing Record edit test checks that Record mode installs its revert
  handler, still displays the edited value, and returns point to the edited
  column.
- Native PostgreSQL and MySQL mutation workflows continue to verify that a
  committed result is actually reloaded from the database.

## Consequences

- Edit no longer depends on query or result functions.
- Cross-module declarations fell from 136 to 134.
- The largest SCC fell from three modules to two.
- The remaining cycle is query and result; result still requires edit because
  its keymaps and transients compose the staged-mutation commands.

## Deferred Boundary

Query and result form a real execution/presentation subsystem: query hands
results and errors to result, while result asks query to rerun, navigate, and
preview SQL.  Breaking that two-module cycle would require moving a complete
execution workflow, not another refresh indirection.  It remains explicit at
the SCC ceiling of two.
