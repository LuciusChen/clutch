# 116 - Explicit NULL Edit State

## Background

Single-cell edit buffers previously treated the literal text `NULL` as a database NULL value.  That made the UI compact, but it collapsed two different values into one input: the database NULL and the string `"NULL"`.

The edit-buffer header also carried row and column identity text.  That helped orientation, but it competed with actionable edit controls as the header gained metadata tags, completion hints, JSON editing, temporal helpers, validation tokens, and now NULL handling.

## Decision

Database NULL is now an explicit edit-buffer state.  `C-c C-n` sets the current cell edit to NULL.  The edit buffer displays `<null>` using `clutch-null-face`, but the placeholder is an overlay, not buffer text.  Therefore:

- `C-c C-n` stages database NULL.
- an empty edit buffer stages an empty string.
- typing `NULL` stages the literal text `NULL`.

Result cells also display database NULL as `<null>`, using the existing NULL face.  When a cell is open in an edit buffer, the originating result cell is highlighted with the same face used for staged edits.  That moves orientation back to the result grid and lets the edit-buffer header focus on commands.

## Implementation Boundaries

This is intentionally not a general typed-cell editor.  The change keeps the state model small: one buffer-local NULL flag, one ephemeral placeholder overlay, and one result-buffer active-cell marker.  It does not add a fallback string parser, a second edit mode, or insert-buffer NULL behavior.

The staged edit model remains unchanged: `C-c C-c` in the edit buffer stages, and `C-c C-c` in the result buffer commits pending row changes.
