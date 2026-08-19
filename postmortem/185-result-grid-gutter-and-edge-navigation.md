# 185 — Result Grid Gutter and Edge Navigation Ownership

## Context

[Issue #41](https://github.com/LuciusChen/clutch/issues/41) identified two Result Browser navigation gaps separate from the horizontal-padding failure recorded in postmortem 184. A globally enabled native line-number mode adds a second gutter beside Clutch's own row-number column and returns whenever result refresh recreates the major mode. Standard `C-a` and `C-e` move to buffer line boundaries, which are respectively inside Clutch's non-data row prefix and beyond the last data cell.

## Decision

- Result Browser owns row numbering. Every `clutch-result-mode` initialization explicitly disables `display-line-numbers-mode`, including the mode reset performed by query refresh. This marks the globalized minor mode as handled for the buffer and prevents its after-major-mode hook from turning the native gutter back on. A user can still enable native line numbers explicitly after entering the result buffer, but refresh restores the Result Browser invariant.
- Keep standard `C-a` and `C-e` semantics unchanged. They describe physical buffer lines, while the requested operations describe the data-grid domain.
- Bind `{` and `}` to first-visible-column and last-visible-column commands. Both preserve the rendered row, exclude hidden row-identity metadata, and reuse the existing named-column jump path so point placement and horizontal centering stay consistent.
- Resolve the current row from text properties across the whole rendered line before falling back to a previous row. This lets an edge-column command recover correctly even when `C-a` has already placed point in the row-number prefix.

## Testing Contract

The mode test enables the real global line-number mode, initializes and reinitializes `clutch-result-mode`, and requires the native gutter to remain disabled. The navigation test drives the public `{` and `}` commands from the physical beginning and end of the second rendered row, with hidden metadata columns on both edges, and requires point to remain on that row in the first or last visible data cell.

## Verification

- Before the implementation, the navigation test failed because `{` was unbound and the mode test failed because native line numbers remained enabled.
- The focused regression tests pass on Emacs 29.4, 30.2, and 32.0.50.
- In a graphical Emacs 32.0.50 frame, reinitializing the Result Browser left `display-line-numbers-mode` and `display-line-numbers` nil, rendered only Clutch's virtual row-number column, and kept the second-row selection in place while actual `}` and `{` key events moved between its last and first visible data columns.
- The full CI suite passes: 553 main tests, 229 backend tests, and 13 architecture tests, plus byte compilation, package lint, and checkdoc.
