# 216 — A Console Keeps One Session

## Evidence

Each case below was reproduced on MySQL and PostgreSQL with #114 (5f0b2bb); on main (fbe31b0), the first was reproduced on both and the other two on MySQL.

- `C-c C-e` in a console bound only the console. After the session was lost, the console's result kept the dead connection, and refreshing it reconnected it to a session of its own, so the server held two sessions for one console. Over a live session, `C-c C-e` went through the disconnect teardown, which leaves the console's results with no connection, and refreshing one failed with `cl-no-applicable-method`. Both times the new connection started in Auto mode, though the console was in Manual mode.
- A temporary console that followed a `USE` or a `SET search_path`, picked again in `clutch-query-console`, opened a second console on a second connection: the picker named it by its connection parameters, which follow the session, while the console is stored under the parameters it was opened with.
- An indirect edit opened from a buffer outside Clutch took a console's connection but none of its parameters. A `USE` in the console then gave it `(:database "x")`, and once the session was lost, running SQL in the edit failed with "Connection params require :backend".

## Decision

- `C-c C-e` in a console that has a connection replaces it through `clutch--replace-connection`, as the automatic reconnect does: the new connection is in the commit mode the console was in, every buffer attached to the old connection moves to it, and work lost with the old one is reported. It connects with the console's own saved or temporary parameters, read again, so a namespace the session moved to gives way to theirs, as the guide already said.
- `clutch--try-reconnect` now goes through `clutch--replace-connection`, which marks the results of lost work and returns the old connection's transaction state for the automatic reconnect and `C-c C-e` to report. The automatic reconnect, reopening a console, a schema switch that reconnects and `C-c C-e` in a console share it. Keeping the mode follows the rule set for `C-c C-e`: the mode stays when it connects the console's own entry again, which in a console it always does, and picking another entry happens only outside consoles.
- The picker names an open temporary console by the parameters it was opened with, `clutch--console-ad-hoc-params`, and returning to the console, live or after its session was lost, keeps them.
- `clutch-edit-indirect` takes the parameters and SQL product of the connection it takes from the buffers that hold that connection.
- The session record the session-state plan proposed, holding a session's parameters, its restore point, its mode, its transaction state and its buffers, is not built: each fix needed one of them only, and the record waits for a case that needs them together.

## Limits

- `C-c C-e` outside a console, in a buffer with `clutch-mode` or the REPL, keeps its picker and connects that buffer alone, in the mode the chosen connection starts in: over a live session the buffers that shared it lose their connection, and over a lost one they reconnect to sessions of their own. Keeping the mode when the same entry is picked again is not done there, since those buffers do not keep the parameters their session was opened with, which telling the same entry apart needs.
- A saved console whose entry was removed from `clutch-connection-alist` keeps no parameters it was opened with, so the picker still names it by those that follow its session, as before.

## Verification

- Live tests on MySQL and PostgreSQL fail on 5f0b2bb and pass here: `C-c C-e` over a lost and over a live session moves the console's result to the console's new connection, which stays in Manual mode; picking a temporary console that followed a namespace switch returns to it, live and after its session was lost, with its temporary parameters kept; and an indirect edit opened outside Clutch reconnects after the console followed a namespace switch.
- Unit tests cover `C-c C-e` in a console over a live session and over a lost one with uncommitted work, the picker, and the indirect edit.
- Each part of the change, undone on its own, fails a test: the picker's parameters, keeping them when returning to a live or to a lost console, moving the results, keeping the mode, marking lost work, and the indirect edit's parameters.
