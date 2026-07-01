# 115 -- Result State And Action Ownership

## Problem

Result buffer behavior had two boundary leaks:

- `clutch-ui.el` defined much of the result buffer model even though
  `clutch-result.el` and `clutch-edit.el` owned the workflows that mutate it.
- SQL/document result action availability was checked through several local
  predicates spread across copy, export, and Transient menu definitions.
- `clutch-connection.el` rendered backend icons and connection header-line
  strings even though that presentation belongs with shared UI helpers.
- Foreign-key metadata loading swallowed backend metadata failures, making a
  degraded result buffer indistinguishable from a backend with no foreign-key
  metadata.

That made `clutch-ui.el` look like the result-state owner and made new data
models easy to bolt into individual menus instead of one action model.

## Decision

Do not split more files just to make the layout look cleaner.  Keep the current
module set, but move ownership to the modules that already own the workflows:

- `clutch-result.el` owns query result state, paging/filter/sort state, refine
  state, column detail metadata, row identity metadata, and the result action
  registry.
- `clutch-edit.el` owns staged mutation payloads and foreign-key edit metadata.
- `clutch-ui.el` owns rendering state only: widths, header/footer caches,
  overlays, row positions, backend icons, connection header-line rendering, and
  display helpers.

Result action availability now goes through a single registry keyed by logical
action.  Copy, export, and menu presentation use that registry instead of
duplicating SQL/document capability checks.

Foreign-key metadata failures now go through the same recoverable metadata
warning path used by other metadata workflows instead of returning an
indistinguishable nil result.

## Why Not A New File

An extra result-state file would add another `require` edge and mostly move
declarations around.  The current root problem was not file size; it was unclear
ownership and duplicated capability decisions.  Keeping the fix inside existing
workflow modules makes the data flow clearer without adding glue.

## Consequence

Future backends should add or expose result actions by registering capability
requirements in one place.  UI code should render state provided by the result
and edit workflows, not define new business state for those workflows.
