# Error results in the result buffer

Superseded by 094 for the failed-SQL marker and result-buffer SQL display split.

## Context

SQL execution errors used to mark the SQL buffer with overlays. That made the failing statement visible near the source, but it also changed the editing surface after a failed query and made longer backend messages hard to read.

## Decision

Render SQL execution errors in the result buffer instead. The result buffer is already where users look for query output, supports wrapping, and can show the failed SQL plus a short hint without modifying the query console.

Connection/setup failures still use minibuffer errors because there is no query result to display yet. Debug records keep the full diagnostic payload.

## Consequences

- SQL buffers stay editable and overlay-free after failed execution.
- Backend hints are rendered as a separate line instead of being embedded in the summary text.
- The older error-position overlay design is superseded for execution errors.
