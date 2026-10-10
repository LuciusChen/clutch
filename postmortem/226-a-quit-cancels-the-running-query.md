# A quit cancels the running query

## Context

Issue #134 reported that on MS-Windows 10 with Emacs 31.1, `C-g` during `clutch-export-query` only showed `Quit` and the export kept running, when the SQL buffer had `display-line-numbers-mode` on and dape breakpoints loaded. With `debug-on-quit`, the quit came from `redisplay_internal`. The same setup on macOS cancels the export: a `C-g` typed while Emacs is busy sets `quit-flag`, and when nothing checks it first, `read_char` turns it back into the key, which runs `clutch-cancel-query-or-quit`.

Emacs on MS-Windows never delivers that key. Its input thread sets `quit-flag` and posts an empty message instead (`w32fns.c` in Emacs 31, around line 3976), so the first `maybe_quit` that runs signals the quit, wherever it is. Redisplay checks for quits, for instance while looking up a color (`w32fns.c` line 873), and a busier redisplay, as with line numbers and fringe breakpoints, is likelier to be that place. The quit then unwinds to the command loop and `clutch-cancel-query-or-quit` never runs.

## Decision

A quit that reaches the command loop while the current buffer's connection runs a query cancels it, as `C-g` on `clutch-cancel-query-or-quit` does. Clutch adds a function after `command-error-function`, the one hook such a quit passes through, with `add-function` when `clutch-connection` loads. It acts only on `quit`, and only when the buffer's connection has a running query that is not already being cancelled, so other errors, quits in other buffers and a second quit are unchanged.

The cancellation moved into `clutch--cancel-running-query`, which the command and the hook share. The command quits when it returns nil; the hook never quits, since a quit signaled inside the error handler would abort the command loop's error reporting.

The rule is not specific to MS-Windows: any quit that aborts a command in that buffer while a query runs now cancels it, which is what `C-g` means there.

The error handler runs with quitting inhibited, so the cancel request cannot be interrupted there, and a backend may wait for it without a deadline: pgsql.el does for a zero connect timeout, which Clutch accepts. The hook therefore gives the request at most `clutch--quit-cancel-seconds`, ten seconds. A request that takes longer counts as refused, so the query stays marked as cancelled and its batch or export still stops after it. MySQL and JDBC give up after their own five-second cancel timeouts first, so the budget does not cut their cleanup short, and an interrupted pgsql.el cancel closes its connection. The command keeps its wait, which a second `C-g` interrupts.

Making redisplay cheaper would not fix this: any slow or quit-checking redisplay, timer or filter can receive the quit on MS-Windows.

Transient works around the same Emacs behavior while one of its menus is open: `transient--quit-kludge` adds a function around `command-error-function` that turns the first quit into a `C-g` event for the menu, citing an Emacs bug that proposes `redisplay-can-quit`. Added after Clutch's, it runs first, so a quit while a menu is open goes to the menu, as the key would.

## Verification

A unit regression calls `command-error-function` with a quit while the buffer's query runs, with another error, from a buffer without a query, and a second time. Only the first quit requests the cancel, once, and the second neither cancels again nor quits. It failed on `main` and passes on Emacs 29.4, 30.2 and 32.0.50. The test caught each of four mutations: acting on any error, cancelling a query already being cancelled, calling `clutch-cancel-query-or-quit` from the hook, and not adding the function.

A GUI Emacs 30.2 on macOS reproduced the MS-Windows path by signaling a quit in the command loop one second into an export of 150,000 rows and 200 columns from a disposable PostgreSQL 16 container. On `main` the quit reached the command loop and the export ran to the end. With this change the quit requested one cancel and the export stopped without leaving a file, both when PostgreSQL streamed with `COPY` and when it fetched pages. Setting `quit-flag` instead, as a key typed while busy on macOS does, still ran `clutch-cancel-query-or-quit` and cancelled once. The real MS-Windows path could not be run here; the reporter offered to test.

Tests that rendered Transient menus by calling `transient-setup` left the menus open, and with them Transient's function around `command-error-function`, which swallowed the new regression's quit on Emacs 32's built-in Transient. A shared helper now opens a menu and quits it with keys, as a user does, which also makes its descriptions see the buffer they describe; one menu test had passed only because the open menu kept its buffer current.

A second regression makes the backend's cancel wait five seconds while running timers, as a stalled connection does, and calls the error handler with quitting inhibited: it returns within the budget, with the query still marked as cancelled and the refusal message. It failed before the budget, taking the full five seconds, and caught both a hook without a budget and a timeout treated as success. Against a disposable PostgreSQL 16 server, a connection with a zero connect timeout ran `SELECT pg_sleep(2)` while the cancel socket was kept from connecting. Before the budget, the error handler returned only when an outside four-second guard stopped it; with a one-second budget it returned after one second without the guard, the statement finished, and the connection answered `SELECT 9`.

The full non-live gate passed on Emacs 29.4, 30.2 and 32.0.50: each ran 705 main tests, 283 backend tests and 13 architecture tests, with zero compilation, package-lint or checkdoc warnings. The complete native/JDBC runner passed 15 suites with 245 passes and 223 capability skips twice, with the released pgsql.el 0.2.0 and with pgsql.el at baea1dd, which streams `COPY`. It used disposable databases, ClickHouse 24.8 and the pinned agent 0.2.26 in an isolated runtime. All test containers were removed and the Docker volume set did not change.
