# 214 — Keep Client Work Out of XTDB Transactions

## Evidence

Both cases were reproduced against a new, disposable XTDB 2.1.0 container on the combined #110 and #111 code before the fixes, and the first on c2bc7e1 too. In a new console whose first statement was `BEGIN READ WRITE`, the queued table-list query ran on the same session and returned `08P01: Queries are unsupported in a DML transaction`; the protocol status became `failed-transaction`. In Manual mode, `SELECT 1` triggered the inherited lazy `BEGIN`, selecting a read-only transaction, and the following INSERT returned `08P01: DML is not allowed in a READ ONLY transaction`.

XTDB's [query](https://docs.xtdb.com/reference/main/sql/queries.html) and [transaction](https://docs.xtdb.com/reference/main/sql/txs.html) references explain that a transaction cannot mix queries and DML, and infers its type from the first statement when it was not explicitly chosen. A catalog query is a query too, even when Clutch runs it in an idle callback.

## Decision

The adapter's existing lazy-transaction operation dispatches on the connection subtype. PostgreSQL keeps its rule; XTDB starts a Manual transaction only before DML, or an `ASSERT`, with `BEGIN READ WRITE`. An `ASSERT` guards the writes after it in the same transaction: run alone outside one, a false `ASSERT 1 = 2` failed on its own and the `INSERT` after it committed, where Manual mode had kept both in one transaction and the commit had failed. An ordinary read remains outside a transaction. Explicit BEGIN, COMMIT and ROLLBACK stay the user's commands; no automatic commit, rollback or replay is added to change a transaction's type.

XTDB's existing busy predicate also protects an open transaction from automatic metadata. The queued refresh waits through that transaction, including an undecided BEGIN and a failed transaction, and resumes after it ends. No second session or transaction-mode cache is added. Synchronous user queries still run normally and get XTDB's error when they violate an explicitly selected transaction type.

## Verification

The live XTDB tests fail without the fix. They cover a cold console's BEGIN READ WRITE, deferred metadata, successful INSERT/COMMIT, resumed schema loading, and Manual SELECT followed by INSERT, commit visibility from another connection, rollback of a second write, and a false ASSERT whose commit fails and keeps the INSERT after it out.

## Limits

Manual mode on XTDB does not make normal reads repeatable. A user who needs a read snapshot explicitly opens a read-only transaction and ends it before DML. Queries inside a write transaction and DML inside a read-only transaction remain server errors. XTDB still has no savepoints, so staged result submission remains limited to Auto mode. Native PostgreSQL retains its existing lazy BEGIN for reads and writes.
