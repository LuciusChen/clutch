# 218 — C-c C-e Follows the Session Everywhere

## Evidence

216 moved a console's session on `C-c C-e` and left the other buffers that connect, a buffer with `clutch-mode`, the REPL and an indirect edit, on the old path, which connects the buffer alone. Each case below was reproduced on MySQL and PostgreSQL on main (185828f), with the connection picked being the one the session was on unless said otherwise.

- A buffer with `clutch-mode` in Manual mode came back in Auto mode. Over a live session its result lost its connection; over a lost one the result kept the dead connection, and refreshing it reconnected to a session of its own, so the server held two sessions for the buffer.
- The REPL in Manual mode came back in Auto mode.
- An indirect edit opened from a console disconnected the console's session, leaving the console with no connection, and connected the edit alone in Auto mode. Picking another connection in the edit disconnected the console too, as reproduced on MySQL.

## Decision

- Every buffer keeps the target of the parameters its session connected with, `clutch--session-target`, renamed from `clutch--console-target`: a console from its first connection, any buffer from each `C-c C-e` that connects it, and an indirect edit from the session it borrows, as found in the buffers that hold the connection.
- `clutch-connect` reads the parameters first, the console's own or the ones picked, and moves the session through `clutch--replace-connection` when they have the session's target, as in a console: in the commit mode the session was in, with its results and other buffers. Otherwise the buffer connects alone, in the mode the connection starts in.
- An indirect edit only borrows its console's session, as `clutch--disconnect-on-kill` already took it, so connecting it elsewhere neither ends that session nor clears its metadata, and asks nothing about it. The question about uncommitted work is now asked once the parameters are known, and only when the old session will close.
- The rule set for `C-c C-e`, that connecting the session's own entry again keeps the mode and picking another starts in that entry's mode, now holds wherever `C-c C-e` is pressed.

## Verification

- Live tests on MySQL and PostgreSQL fail on 185828f and pass here: a buffer with `clutch-mode` in Manual mode, picking its connection again over a live and a lost session, stays in Manual mode, and its result moves with it and stays on its connection after a refresh, while picking another connection connects it alone in Auto mode and leaves the result without a connection; the REPL keeps Manual mode; and an indirect edit picking its console's connection moves the console with it in Manual mode, while picking another leaves the console on its live connection.
- Unit tests cover the target kept by a buffer's first connection, moving or not by the target picked, the indirect edit taking its console's target, and an indirect edit connecting elsewhere without closing, releasing or clearing the console's connection or asking about it.
- Each part, undone on its own, fails a test: keeping the target after connecting alone, the indirect edit's target, leaving the console's session to it, asking only when the old session closes, and leaving the console's metadata.
