# 205 — CTE Results Rewrite Their Main Statement

## Evidence

A CTE result was displayed and paged, but count, server filter and server sort refused it on every backend, because the rewrite gate from postmortem 099 needs a simple single-table SELECT and a WITH statement never was one. The count and filter builders wrapped the whole statement in a derived table, which SQL Server 2022 rejects with `Incorrect syntax near the keyword 'WITH'`; PostgreSQL 16, MySQL 8 and Oracle Free accepted it. Oracle 11.2.0.1 Enterprise Edition, probed read-only through DataGrip, accepted WITH inside a ROWNUM inline view and inside a derived table, recursive WITH included, and accepted the same queries with the WITH clause in front. An Oracle XE 11g container was no substitute: under OrbStack's Rosetta translation it stops at startup with ORA-45301.

## Decision

- The count and filter builders keep a leading WITH clause in front and wrap only the main statement, on every backend. That one shape worked everywhere it was tried, so no dialect needs its own.
- The 099 gate checks the main statement: one relation, which may be a CTE, no aggregate, DISTINCT, GROUP BY, HAVING or join, no row limit of its own, and unique result labels.
- The main statement starts at the first top-level SELECT, INSERT, UPDATE, DELETE, REPLACE or MERGE, so PostgreSQL's SEARCH and CYCLE clauses stay with the WITH clause.
- Paging and server sort already appended to the statement, or wrapped it in Oracle's ROWNUM inline view, and are unchanged.

## Limits

- A CTE result still cannot be edited: Clutch does not resolve a CTE to its base table.
- A main statement in parentheses, such as `WITH ... (SELECT ...) UNION (SELECT ...)`, is not recognized as a SELECT, so it is neither paged nor rewritten.
