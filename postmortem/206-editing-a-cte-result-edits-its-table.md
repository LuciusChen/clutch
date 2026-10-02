# 206 — Editing a CTE Result Edits Its Table

## Evidence

A result of `WITH c AS (SELECT * FROM t WHERE ...) SELECT * FROM c` was read-only: the edit gate needs a simple single-table SELECT, and a WITH statement never was one. DataGrip 2024.3.5 edits such results, CTE chains and a CTE used only in a subquery included, as observed without submitting on Oracle 11.2 and on a local Oracle 26ai Free. It also showed what to avoid. With no key in the projection, its UPDATE matched on the remaining column values, and on the local table that condition selected two rows for an edit of one. With a CTE column list, its preview set columns named after the list, which the table does not have. Clutch itself looked up a CTE's name as a table while preparing row identity for such a result, so a CTE named like a table probed that table.

## Decision

- A WITH statement is followed from its main statement through the CTEs it reads to one table. Each SELECT on the way must read one relation, with no aggregate, DISTINCT, GROUP BY, HAVING, join or set operation. A name refers to a CTE defined before the SELECT that uses it, as in SQL. A comparison whose outcome depends on how the database folds case, such as `"Orders"` against `orders`, ends the search, so an edit never reaches a table the query did not read.
- The table's row identity, its key or row locator as for a table result, is selected in the innermost SELECT and passed out as hidden columns: a `*` passes them by itself, a list of columns gains them, and so does a CTE column list. An edit therefore matches the key or locator, never column values.
- Result columns map to the table's columns through `AS` aliases, CTE column lists and `*`; a computed column stays read-only, as in a table result. A CTE column list over a `*` body maps no column, because only the table knows the order of `*`.
- A WITH statement that leads to no table has none, so no CTE name is looked up as a table.
- The scanner from postmortem 204 that finds data-modifying CTEs stays separate: it has to keep working on WITH clauses that this parser declines, such as recursive ones with SEARCH or CYCLE.

## Limits

- Derived tables, as in `SELECT * FROM (SELECT ...) s`, remain read-only, although they could take the same path.
- Recursive CTEs, and quoted names that differ only in case, leave the result read-only.
