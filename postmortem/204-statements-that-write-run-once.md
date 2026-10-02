# 204 — Statements That Write Run Once

## Evidence

Clutch decided whether to page a statement from its leading keyword: any SELECT, and a WITH whose main statement is a SELECT. Paging appends a row-limit tail, adds row identity columns to a query of one table, and runs the statement again for every further page and export. Three kinds of SELECT write, and Clutch 0.5.2 paged all three. The next page of PostgreSQL's `WITH i AS (INSERT ... RETURNING *) SELECT * FROM i` inserted its 150 rows again, natively and over JDBC, and so did H2's `SELECT * FROM FINAL TABLE (INSERT ...)`, which shares DB2's syntax. `SELECT ... INTO` a new table from a join or a CTE copied 101 of 1200 rows with a page size of 100, 501 with the default, on PostgreSQL and SQL Server, and from one table failed on the injected identity column. An UPDATE or DELETE inside a WITH clause asked for no confirmation and left Manual mode clean, and `WITH ... DELETE ... WHERE` skipped the destructive confirmation because only the leading keyword was checked.

## Decision

- A SELECT that writes is not pageable, so it runs once as written and its rows display like those of `INSERT ... RETURNING`.
- The statements embedded in SQL, the CTE bodies of a leading WITH clause and the statements of `FINAL`, `NEW` or `OLD TABLE (...)`, count like the main statement for destructive and high-risk confirmation and for Manual-mode dirtiness. A CTE body is found as a group after `AS [NOT] MATERIALIZED` before the main statement rather than by parsing the WITH clause, because nothing needs CTE names or columns yet.
- Any top-level INTO makes a SELECT unpageable. Only SELECT INTO a table marks Manual mode dirty; MySQL's INTO a file or variables does not. SELECT INTO is not treated as schema-affecting, because that branch takes the backend's DDL transaction effect, which JDBC leaves unknown for SQL Server, so Manual mode would never turn dirty there.

## Limits

- Such a statement's rows are fetched in full, natively as before and over JDBC batch by batch, so a large RETURNING result is slow to arrive.
- A SELECT that writes through a function, such as `nextval()`, is still paged.
- DB2 itself was not run; H2 was.
- `EXPLAIN ANALYZE` runs the statement it explains but is still classified as EXPLAIN.
