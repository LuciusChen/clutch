# Error status fringe marker

## Context

Rendering SQL execution errors in the result buffer made long backend messages easier to read, but including the failed SQL there duplicated source context. Successful SQL already uses a compact fringe marker in the query buffer.

## Decision

Keep SQL execution errors in the result buffer, but render only the error summary, optional hint, and elapsed time.  Use the existing SQL status marker in the source buffer for execution status: green for success, red for failure.

The marker remains a fringe-only visual cue in graphical frames and a compact fallback glyph in terminal frames.  It does not highlight or edit the SQL body.

## Consequences

- Error result buffers stay focused on the backend message.
- The query console remains the only place where the failed SQL is shown.
- Success and failure use one visual model instead of separate result-buffer SQL blocks and source-buffer status markers.
