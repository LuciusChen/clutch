# 223 — A Sent Statement Flashes

## Decision

Issue #122 asks to see which statement runs, as `C-c C-c` picks the statement at point and the status dot marks only its first line. A statement sent from a SQL buffer now flashes with the built-in `pulse`, which highlights it with an overlay and leaves point and the region alone. The flash goes where every statement with a source region is sent once confirmed, `clutch--execute-statement`, so `C-c C-c`, a region, the whole buffer and each statement of a batch in turn flash, while the REPL, result paging and exports, which have no source region, do not; a reconnect that retries a statement does not flash it again. The status dot is unchanged and shows the outcome once the flash fades.

`pulse` fades its highlight from timers, and a backend that runs SQL synchronously, as SQLite does, blocks both timers and redisplay until the statement returns, so the flash would only appear afterwards, or not at all once its fade time had passed. Clutch redisplays right after starting the flash, so the statement stays highlighted while such a backend runs it. `pulse-flag` set to `never` turns the flash off; Clutch adds no option of its own.

## Verification

- A unit test runs `C-c C-c` on a statement of an in-memory SQLite buffer: the flash covers that statement only, a redisplay follows it before the statement's SQL is sent, point stays and no region becomes active, and SQL run without a source region does not flash. A second test runs a batch and sees each statement flash as it is sent. Both fail on main.
- Removing the redisplay, flashing without a source region, or flashing once the reply arrives each fails a test.
- The fading itself is pulse's and was not checked on a graphical display.
