# 211 — One Gate for Query Activity Replies

## Evidence

0.5.2 moved statements off the foreground, and 0.5.5 moved paging, server-side sorting, counting and exporting there too. Five flows then ran SQL without blocking: a statement, a batch, the REPL, a result's page, sort or count, and an export. Each decided alone what to do with a reply that arrived after its buffer had moved on, and between #95 and #99 about ten bugs were each one flow missing one of those decisions:

- A reply for a buffer that had moved to another connection replaced that buffer's result: a page (844b797), then a page's failure (16dbb29).
- A batch kept the connection it started with after an idle reconnect (cca0769), then followed its buffer to another database (cd99cf1); a cancel that met a finished reply was ignored by a result query (d04af89).
- A continuation was not protected against a nonlocal exit, in the batch (378a0cd) and then in the export (955d94a), and a batch whose buffer was killed said "nil" (cb7f166).
- An earlier activity's end cleared a later one's time (ad38b26).

Unifying the flows found four more of the same kind, each confirmed by a test that fails without its change: a statement's late failure drew its error page in the result buffer that the console's new connection names and bound it to the old connection, as did a REPL SELECT and a batch statement in flight; an export's late failure drew an error page in a result buffer that had lost its connection. The indirect edit ran its SQL in a source-code buffer that held no connection, so its activity began in a buffer without its connection.

## Decision

- Every asynchronous reply goes through `clutch--query-activity-reply`. A reply counts while its activity's buffer is live and holds the connection the reply came from. That is the connection the statement ran on, which after an idle reconnect is the new one; the activity's own `:connection` stays the reservation it made. Otherwise the flow's `:moved` or `:killed` runs while the activity's markers still place its statements, and then the activity ends; a handler that exits nonlocally ends it too.
- A flow decides only what moving means for it. A result query shows nothing; an export stops and removes its temporary file; a statement or a batch marks its line and reports its outcome in the echo area, and the REPL prints it, each recording a failure for diagnostics. None draws a result or error page.
- An activity starts in a buffer that holds its connection, so `clutch-indirect-execute` runs its SQL in such a buffer and `clutch--execute` takes no connection of its own.
- The synchronous path keeps no checks: nothing can kill or move a buffer inside the command that runs it. Cancel stays with the running query: a batch or an export starts its next step inside the callback that handled the previous one.

## Consequence

The five flows share one decision instead of five copies, and their own buffer and connection checks are gone. AGENTS.md requires a new flow to use the gate and names the cases its tests cover.
