# A quit cancels the running query

## Context

Issue #134 reported that on MS-Windows 10 with Emacs 31.1, `C-g` during `clutch-export-query` only showed `Quit` and the export kept running, when the SQL buffer had `display-line-numbers-mode` on and dape breakpoints loaded. With `debug-on-quit`, the quit came from `redisplay_internal`. The same setup on macOS cancels the export: a `C-g` typed while Emacs is busy sets `quit-flag`, and when nothing checks it first, `read_char` turns it back into the key, which runs `clutch-cancel-query-or-quit`.

Emacs on MS-Windows never delivers that key. Its input thread sets `quit-flag` and posts an empty message instead (`w32fns.c` in Emacs 31, around line 3976), so the first `maybe_quit` that runs signals the quit, wherever it is. Redisplay checks for quits, for instance while looking up a color (`w32fns.c` line 873), and a busier redisplay, as with line numbers and fringe breakpoints, is likelier to be that place. The quit then unwinds to the command loop and `clutch-cancel-query-or-quit` never runs.

## Decision

A quit that reaches the command loop while the current buffer's connection runs a query is delivered as `C-g` again, so `clutch-cancel-query-or-quit` cancels the query as a command, as it does when Emacs reads the key. Clutch adds a function after `command-error-function`, the one hook such a quit passes through, with `add-function` when `clutch-connection` loads. It acts only on `quit`, only while the buffer's connection has a running query that is not already being cancelled, and only where `C-g` runs `clutch-cancel-query-or-quit`. A quit the command itself raises, a quit after the query is being cancelled and a quit in a buffer where `C-g` means something else, such as the Record view, get no key, so the key cannot loop.

Transient does the same for an open menu: `transient--quit-kludge` turns the first quit into `C-g`, so the menu handles it. Emacs's own fix, a `redisplay-can-quit` variable, has not been merged; none of Emacs 29.4, 30.2 and 32.0.50 defines it. With a menu open, Transient's function runs first and closes the menu, and the next quit cancels the query.

Cancelling from the error handler itself, as the first version of this change did, would wait for the backend's cancel request with quitting inhibited, and without a deadline where the backend sets none, as pgsql.el does for a zero connect timeout. Delivered as the key, the request runs as a command, which a second `C-g` interrupts, and the error handler returns at once.

The rule is not specific to MS-Windows: any quit that aborts a command in such a buffer while a query runs now cancels it, which is what `C-g` means there. On MS-Windows the cancel now runs from the delivered key, a path verified here only on macOS; the reporter offered to test it.

Making redisplay cheaper would not fix this: any slow or quit-checking redisplay, timer or filter can receive the quit on MS-Windows.

## Verification

A unit regression checks that the function is on the global `command-error-function`, then calls it alone on a let-bound value, so that other functions there, such as Transient's while a menu is open, cannot hide it. A quit while the buffer's query runs queues one `C-g`, whose command cancels the query once; another error, a buffer without a running query, a buffer where `C-g` runs `keyboard-quit` and a quit after the cancel queue nothing. It failed on `main` and passes on Emacs 29.4, 30.2 and 32.0.50, and it caught each of four mutations: acting on any error, delivering the key for a query already being cancelled, ignoring the key binding, and not adding the function.

GUI Emacs 30.2 and 32.0.50 on macOS reproduced the MS-Windows path by signaling a quit in the command loop one second into an export of 150,000 rows and 200 columns from a disposable PostgreSQL 16 container. The quit came back as `C-g`, `clutch-cancel-query-or-quit` ran as a command and requested one cancel, and the export stopped without leaving a file, both when PostgreSQL streamed with `COPY` and when it fetched pages; on `main` the export ran to the end. Setting `quit-flag` instead, as a key typed while busy does on macOS, ran the command once without the function. With `clutch-dispatch` open on Emacs 32, whose Transient carries the kludge, the first quit closed the menu without cancelling and the second cancelled once. The real MS-Windows path could not be run here.

The first version, run against a PostgreSQL connection with a zero connect timeout whose cancel socket never connected, kept the error handler waiting with quitting inhibited until an outside four-second guard stopped it. The error handler now only queues the key.

The full non-live gate passed on Emacs 29.4, 30.2 and 32.0.50: each ran 705 main tests, 283 backend tests and 13 architecture tests, with zero compilation, package-lint or checkdoc warnings. The complete native/JDBC runner passed 15 suites with 245 passes and 223 capability skips, using disposable databases, ClickHouse 24.8 and the pinned agent 0.2.26 in an isolated runtime. All test containers were removed and the Docker volume set did not change.
