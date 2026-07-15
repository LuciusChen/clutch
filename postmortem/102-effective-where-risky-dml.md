# 102. Risky DML uses effective WHERE semantics

## Context

The original risky-DML guard only distinguished `UPDATE` / `DELETE` statements with no top-level `WHERE` from statements with any top-level `WHERE`.

That missed cases such as:

```sql
UPDATE users SET name = 'x' WHERE 1 = 1;
DELETE FROM users WHERE TRUE;
```

These statements have a syntactic `WHERE`, but they still affect every row.

## Decision

Risky-DML confirmation now treats an obviously true top-level `WHERE` as equivalent to no effective `WHERE`.

The guard remains intentionally limited. It detects visible tautologies such as literal truth, equal numeric literals, and top-level boolean combinations where the expression is plainly always true. It does not try to become a SQL optimizer or prove every possible tautology.

## Why

The user-facing safety model is about table-wide mutations, not the presence of the word `WHERE`. `WHERE 1=1` deserves the same typed `YES` confirmation as an omitted predicate.

Keeping the rule in the query execution layer also avoids pushing interactive safety policy into the database abstraction layer.
