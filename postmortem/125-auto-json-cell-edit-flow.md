# Auto JSON Cell Edit Flow

## Context

Result cell editing normally opens a plain edit buffer.  JSON values can then be edited in a child JSON editor, where saving returns to the parent edit buffer and the user confirms the cell edit separately.

Text cells that contain JSON objects or arrays are different.  Clutch opens the JSON editor automatically because the formatted JSON view is the first editing surface the user sees.

## Decision

When the JSON editor was opened automatically for a result cell, treat that editor as owning the whole cell edit flow.  `C-c C-c` stages the cell edit and returns to the result buffer; `C-c C-k` cancels the cell edit and returns to the result buffer.

Manually opened JSON child editors keep the older two-step behavior: save or cancel returns to the parent edit buffer.

## Rationale

Automatic JSON editing should not expose an implementation buffer as an intermediate stop.  Returning to a compact parent edit buffer after cancel or save makes the workflow feel like it changed modes halfway through the command.

Keeping manual JSON sub-editing two-step preserves the existing contract for users who intentionally enter the JSON editor from an ordinary edit buffer.
