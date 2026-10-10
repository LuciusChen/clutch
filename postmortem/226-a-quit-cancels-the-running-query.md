# A quit cancels the running query

## Context

Issue #134 reported that on MS-Windows 10 with Emacs 31.1, `C-g` during `clutch-export-query` only showed `Quit` and the export kept running, when the SQL buffer had `display-line-numbers-mode` on and dape breakpoints loaded. With `debug-on-quit`, the quit came from `redisplay_internal`. The same setup on macOS cancels the export: a `C-g` typed while Emacs is busy sets `quit-flag`, and when nothing checks it first, `read_char` turns it back into the key, which runs `clutch-cancel-query-or-quit`.

Emacs on MS-Windows never delivers that key. Its input thread sets `quit-flag` and posts an empty message instead (`w32fns.c` in Emacs 31, around line 3976), so the first `maybe_quit` that runs signals the quit, wherever it is. Redisplay checks for quits, for instance while looking up a color (`w32fns.c` line 873), and a busier redisplay, as with line numbers and fringe breakpoints, is likelier to be that place. The quit then unwinds to the command loop and `clutch-cancel-query-or-quit` never runs.

## Decision

A quit that reaches the command loop while the current buffer's connection runs a query cancels it, as `C-g` on `clutch-cancel-query-or-quit` does. Clutch adds a function after `command-error-function`, the one hook such a quit passes through, with `add-function` when `clutch-connection` loads. It acts only on `quit`, and only when the buffer's connection has a running query that is not already being cancelled, so other errors, quits in other buffers and a second quit are unchanged.

The cancellation moved into `clutch--cancel-running-query`, which the command and the hook share. The command quits when it returns nil; the hook never quits, since a quit signaled inside the error handler would abort the command loop's error reporting.

The rule is not specific to MS-Windows: any quit that aborts a command in that buffer while a query runs now cancels it, which is what `C-g` means there. The error handler runs with quitting inhibited, so the cancel request cannot be interrupted there. Each backend bounds its wait: pgsql.el by the connection's connect timeout, MySQL by the cancel connection's `clutch-db-mysql-cancel-timeout-seconds`, as it already inhibits quitting itself, and JDBC by `clutch-jdbc-cancel-timeout-seconds`.

Making redisplay cheaper would not fix this: any slow or quit-checking redisplay, timer or filter can receive the quit on MS-Windows.

## Verification

A unit regression calls `command-error-function` with a quit while the buffer's query runs, with another error, from a buffer without a query, and a second time. Only the first quit requests the cancel, once, and the second neither cancels again nor quits. It failed on `main` and passes on Emacs 29.4, 30.2 and 32.0.50. The test caught each of four mutations: acting on any error, cancelling a query already being cancelled, calling `clutch-cancel-query-or-quit` from the hook, and not adding the function.

A GUI Emacs 30.2 on macOS reproduced the MS-Windows path by signaling a quit in the command loop one second into an export of 150,000 rows and 200 columns from a disposable PostgreSQL 16 container. On `main` the quit reached the command loop and the export ran to the end. With this change the quit requested one cancel and the export stopped without leaving a file, both when PostgreSQL streamed with `COPY` and when it fetched pages. Setting `quit-flag` instead, as a key typed while busy on macOS does, still ran `clutch-cancel-query-or-quit` and cancelled once. The real MS-Windows path could not be run here; the reporter offered to test.

The full non-live gate passed on Emacs 29.4, 30.2 and 32.0.50: each ran 705 main tests, 283 backend tests and 13 architecture tests, with zero compilation, package-lint or checkdoc warnings. The complete native/JDBC runner passed 15 suites with 245 passes and 223 capability skips twice, with the released pgsql.el 0.2.0 and with pgsql.el at baea1dd, which streams `COPY`. It used disposable databases, ClickHouse 24.8 and the pinned agent 0.2.26 in an isolated runtime. All test containers were removed and the Docker volume set did not change.
