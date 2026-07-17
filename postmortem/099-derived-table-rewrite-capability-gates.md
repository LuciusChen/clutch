# 099 -- Derived-Table Rewrites Need Capability Gates

## Background

Issue 19 exposed a gap in the earlier SQL rewrite design notes. A user query with duplicate result labels can be valid as a top-level SELECT:

```sql
SELECT a.*, b.*
FROM table_a a
JOIN table_b b ON b.id = a.id
LIMIT 10
```

Both tables can expose a column named `id`. That is legal in the top-level result set, but MySQL rejects the same projection when clutch wraps it in a derived table for pagination, filtering, or counting:

```sql
SELECT * FROM (<user-sql>) AS _clutch_page
```

The derived table must have unique column names. PostgreSQL has a similar constraint. A wrapper therefore changes the validity envelope of the user's query even when the wrapper is otherwise semantically natural.

## Root Cause

The previous mental model treated derived-table wrapping as a safe fallback for complex SQL. That was too broad. Wrapping user SQL introduces at least one additional database constraint: the wrapped projection has to be a valid table shape. A top-level result set can have duplicate labels; a derived table cannot.

SQL text alone is not enough to decide whether a rewrite is safe. The actual result metadata matters because duplicate labels are visible only after the query is executed and the backend reports column names.

## Fix

Clutch now separates two decisions:

1. Page navigation is safe only when the result query has no top-level `LIMIT`/`OFFSET`. For ordinary pageable queries, backend adapters append `LIMIT`/`OFFSET` directly instead of wrapping the query.
2. Server-side sort/filter/count rewrites require `clutch--server-rewritable-result-p`. The check is intentionally conservative: simple single-table SELECT, row-identity-compatible SQL, no page tail, and unique actual result labels.

If those checks fail, clutch still displays the query result, but disables the server-side result rewrites that would need a derived-table wrapper. This keeps the original query valid and avoids compensating in the UI after the database has already rejected generated SQL.

## Why Not Alias Duplicate Columns Automatically

Aliasing duplicate columns before wrapping sounds attractive, but it would change the user's visible projection. That affects column names shown in the result buffer, export output, sort/filter prompts, metadata lookup, and staged mutation safety. It also becomes dialect-sensitive once quoted identifiers, star expansion, computed expressions, and existing aliases enter the query.

For now, clutch treats derived-table rewriting as a capability with preconditions rather than a universal fallback. A future AST-backed rewrite could make aliasing safe by preserving a mapping from original result columns to generated wrapper columns, but that complexity is not justified for the current workflow.

## Lesson

Derived-table wrapping is not a neutral operation. It is useful when the result shape is table-safe, but it must be gated by result metadata. When a query is valid only as a top-level result, clutch should preserve that boundary and avoid inventing a server-side rewrite around it.
