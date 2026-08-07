# 160 — Explicit DEFAULT Edit State

## Context

The single-cell edit buffer already distinguishes database `NULL` from both an empty string and the literal text `NULL`, but it cannot ask the database to restore a column's declared default. Copying insert-form default labels into the edit buffer would be misleading: omitting a column from `INSERT` and assigning `DEFAULT` during `UPDATE` are different SQL operations.

## Decision

Treat ordinary text, `NULL`, and `DEFAULT` as three explicit edit-buffer states. `C-c C-n` selects `NULL` only for nullable columns, while `C-c C-d` selects `DEFAULT` only when column metadata declares a default and the backend supports `UPDATE ... SET column = DEFAULT`. Both commands keep the normal two-step workflow: the edit buffer stages the value, and the result buffer commits the staged batch.

The staged edit value for `DEFAULT` is an internal sentinel, not the string `"DEFAULT"` and not the column's cached default expression. UPDATE construction emits `column = DEFAULT` directly and adds no bound parameter for that assignment. This keeps preview SQL identical to the execution template while preserving prepared parameters for ordinary values and row identity.

Header hints are derived from column capabilities rather than the current cell state. A nullable column continues to show `Set NULL` even while NULL is selected; a column with a declared default continues to show `Set DEFAULT` while DEFAULT is selected. Generated or otherwise non-writable columns remain blocked by the existing edit-entry checks.

## Backend Boundary

Most supported SQL mutation backends accept `SET column = DEFAULT`, but SQLite does not. SQLite therefore does not expose the action instead of substituting cached schema text or waiting for commit to fail. JDBC column metadata must carry JDBC `COLUMN_DEF` (and Oracle `DATA_DEFAULT`) so Clutch can apply the same column-driven rule to JDBC-backed databases.

## Alternatives Rejected

- Treating typed `DEFAULT` as special would make the literal string impossible to enter, repeating the ambiguity that explicit NULL state removed.
- Binding a default expression as a parameter would store the expression as data rather than execute it.
- Injecting cached default-expression text for SQLite could execute stale schema metadata and would not be the database's `DEFAULT` operation.
- Adding independent NULL and DEFAULT booleans would create an invalid state in which both are selected.

## Consequences

The edit buffer gains one explicit special-value model and one placeholder overlay. Staged rendering and re-editing must recognize the DEFAULT sentinel, while ordinary strings—including `"NULL"` and `"DEFAULT"`—remain ordinary values. No backend execution API or JDBC prepared-value protocol changes are required.
