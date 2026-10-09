# A refused cancel still stops the workflow

## Failure

The SQL Server export probe recorded in 221 reproduced a cancellation refusal followed by a successful export that replaced its destination. A public batch execution probe reproduced the same continuation: after the backend declined cancellation, the first successful reply dispatched the next statement.

`clutch-cancel-query-or-quit` cleared `:cancelling` when the interrupt returned nil or signalled `clutch-db-error`. The reply therefore lost the user's request to stop, even though batch and export continuations already knew how to stop when that flag was set.

## Decision

Keep the existing flag until the current reply is handled. It records the user's request, not evidence that the server cancelled the statement. A refusal reports that Clutch must wait for the current result before stopping; a second C-g quits as it does after an accepted request. No new state, retry or disconnect path is needed.

The current statement retains its real outcome and transaction accounting. A successful write may already be committed in Auto mode; stopping the workflow does not undo it. Subsequent batch statements and export pages do not run. A stopped file export removes its temporary output and preserves the existing destination.

## Verification

Extend existing batch and both file-export lifecycle regressions with rejected and failed cancellation requests. They fail before the fix and pass after it, checking that no next statement or page is dispatched, successful work is recorded and export output stays atomic. Existing cancellation command coverage also checks that the request is retained and repeated C-g quits.

A JDBC live regression requests cancellation immediately before Clutch handles a real query reply, when the backend has already released its active request. It checks the actual nil cancellation result, keeps the successful first result, starts no subsequent SQL, releases the foreground reservation and preserves the export destination. Only reply timing is controlled; SQL execution and cancellation are real.

The four focused regressions failed on main and passed after the fix; restoring only the flag reset made all four fail again. The live regression also failed on main against isolated DuckDB by dispatching the second batch statement, and passed after the fix on Oracle, SQL Server, ClickHouse and DuckDB.

`./test/run-ci.sh all` passed on Emacs 29.4, 30.2 and 32.0.50: each ran 705 main tests (704 passed and one local container-forwarding test skipped by the sandbox), 284 backend tests and 13 architecture tests, with compilation, package-lint and checkdoc clean. The complete native/JDBC live matrix passed all 15 suites: 249 passed, 223 capability skips and zero unexpected results, using agent 0.2.26 (SHA-256 `2ef3e70b33a194164358808ce28bf6e5ae511c057a109067c1fa253788049fc7`) and ClickHouse 24.8.

All test containers were removed; the 26 pre-existing Docker volumes were unchanged. Tests used an isolated JDBC runtime and disposable databases, without touching production connections, user configuration or the installed plugin checkout.
