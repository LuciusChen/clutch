# 213 — The Server Says When Work Is Uncommitted

## Evidence

Clutch marked a transaction dirty, and cleared it, by classifying SQL text, and only in Manual mode. Each case was reproduced on c2bc7e1.

- MySQL in Manual mode: an `INSERT`, then `CREATE TEMPORARY TABLE`. Clutch took the DDL for one that commits, as most MySQL DDL does, and cleared the dirty flag; the server still held the insert uncommitted, a disconnect did not ask, and the insert was gone. MySQL's manual says that `CREATE TABLE` and `DROP TABLE` statements do not commit a transaction if the `TEMPORARY` keyword is used.
- PostgreSQL and MySQL in Auto mode: after a typed `BEGIN` or `START TRANSACTION` and an `INSERT`, the server reported a transaction open and Clutch recorded nothing, so `clutch-disconnect` and killing the console did not ask, and the insert was gone.
- PostgreSQL REPL input `COMMIT; BEGIN; INSERT ...`, which the REPL sends as one string: the server reported the last transaction open, but Clutch took the input for a `COMMIT` by its first keyword and cleared the work, so a disconnect lost the row without asking. Manual mode loses it the same way on c2bc7e1.

PostgreSQL reports its transaction status in every ReadyForQuery (`pgsql-transaction-status`), and MySQL in the status flags of every OK and EOF packet (`mysql-in-transaction-p`). Clutch already read both to run staged batches, but not for this.

## Decision

- A backend method, `clutch-db-transaction-open-p`, returns t or nil from the reply to the last statement that succeeded, or `unknown`. PostgreSQL, and XTDB through its adapter, read ReadyForQuery; MySQL reads the in-transaction flag. Other backends return `unknown`.
- With a report, SQL that writes inside an open transaction marks it dirty in either mode: DML, or DDL that `clutch-db-schema-transaction-effect` says leaves work. A read marks nothing, since PostgreSQL in Manual mode and XTDB open a transaction for a `SELECT` too. A report of none open clears the work, and so does SQL that ends the transaction and chains a new one, as `COMMIT AND CHAIN` does. Without a report, SQL marks and clears it in Manual mode only, as before.
- Dirty work is asked about before the session closes in either mode, the automatic retry after an idle disconnect skips a session that holds it, and the indicator shows a transaction begun in Auto mode as `Tx: Auto*`. `clutch-commit` and `clutch-rollback` stay Manual mode commands. Switching to Manual mode adopts such a transaction with its work, as it already kept the transaction open when Clutch did not know of the work, so `clutch-commit` or `clutch-rollback` can end it there, and a staged batch, which Auto mode refuses inside a transaction begun with SQL, can run in it on a savepoint. Leaving Manual mode with work is still refused, since it would commit the work.
- SQL counts as ending the transaction only when that statement stands alone, so an input that begins with `COMMIT` and opens another transaction keeps the work of the last one.
- Statements are refused while the outcome is `uncertain`, so no report reaches that state; only an explicit rollback or reconnect clears it.

## Limits

- JDBC backends report nothing and keep classifying SQL in Manual mode, so a transaction begun with SQL in Auto mode there is still not tracked. Oracle (`DBMS_TRANSACTION.LOCAL_TRANSACTION_ID`) and SQL Server (`@@TRANCOUNT`) could be asked with one query after a statement that may end a transaction; that waits for a reproduced case.
- MySQL commits implicitly on `BEGIN` and `START TRANSACTION`, so work before a typed `BEGIN` still counts as uncommitted until the new transaction ends, as it did in Manual mode before.
- An error carries no MySQL status, so work that a deadlock rolled back stays known until the server next reports no transaction: the next statement in Auto mode, and in Manual mode, where the next statement opens a transaction again, the next commit or rollback.

## Verification

- A live test on MySQL and PostgreSQL types `BEGIN` or `START TRANSACTION` and an `INSERT` in Auto mode: the indicator shows `Tx: Auto*`, a disconnect and killing the console ask, a rollback to a savepoint keeps the work, and a typed `COMMIT` clears it and commits the row. A second such transaction, adopted by switching to Manual mode, shows `Tx: Manual*` and is committed only by `clutch-commit`. It fails on c2bc7e1, where the indicator showed `Tx: Auto`.
- A MySQL live test runs `CREATE TEMPORARY TABLE` after an `INSERT` in Manual mode: the work stays known and a disconnect asks; a `CREATE TABLE` then commits the insert and clears it. It fails on c2bc7e1, where the temporary table cleared it.
- An XTDB live test checks the same through the PostgreSQL adapter: an `INSERT` in Auto mode that marks nothing, `BEGIN READ WRITE` and an `INSERT` that do, committed for another connection to read, an `INSERT` in Manual mode, and a `SELECT` that marks nothing.
- A PostgreSQL live test types `COMMIT; BEGIN; INSERT ...` into the REPL: a disconnect asks, declining keeps the session, and another session sees the row only after the typed `COMMIT`. It fails without the single-statement check.
- Unit tests cover each SQL and report in both modes, with the fake statement publishing its report when it runs, the confirmation, the indicator, the idle retry and each backend's report.
