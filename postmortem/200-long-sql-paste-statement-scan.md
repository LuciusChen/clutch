# 200 — The Statement Scan Skips Plain Text and Survives Edits

## Evidence

A long SQL paste stalled Emacs, and the Oracle metadata work of postmortems 197 to 199 did not touch it: the stall is in Clutch's own SQL analysis and every backend pays it. A byte-compiled probe with a stub connection traced what runs around a `yank` in a `clutch-mode` console. During the command itself only `clutch--console-yank-cleanup` runs, 13 ms per MiB of pasted text. The cost comes later, on the paths a paste arms: the Eldoc idle timer, which runs `clutch--eldoc-function` half a second after point stops, and `completion-at-point`, which with `corfu-auto` runs on every keystroke once three characters are typed.

Both paths asked `clutch--statement-bounds` for the statement at point. It copied the whole buffer and ran `clutch-db-sql-statement-breaks` over it, whose `clutch-db-sql-scan-code` called `clutch-db-sql-skip-literal-or-comment` and then the caller's function at every character. On a 4 MiB paste that scan cost 285 ms, and nothing reused it: a completion request in an untyped slot scanned once for its bounds, again for the top-level token before point (through the same per-character scan), and a third time when the table cache was refreshed for the same tick. Masking the current statement for its tables and aliases walked its characters the same way, 190 ms for a 4 MiB statement. Measured on the merged main (4f72f56) with a 4 MiB paste: Eldoc on a trailing identifier of one long statement 490 ms, `completion-at-point` just after the terminating semicolon 1.65 s, and each further keystroke 270 ms in a script of statements or 575 ms inside one long statement. The pre-merge baseline (069cf1e) was worse only in the request that scanned twice.

The dialect rules were the constraint. Strings, `--` and `/* */` comments, double-quoted, backtick and bracket identifiers with doubled delimiters, PostgreSQL dollar quotes and MySQL backslash escapes all decide where a statement ends, and execution splits statements through the same scanner.

## Decision

`clutch-db-sql-scan-code` keeps its contract and gains a restriction. A caller names the characters it wants to see, or hands over the candidate positions that `clutch-db-sql-code-match-positions` already collects, and the scanner jumps between literal openers, parentheses and those characters with one regexp search, calling `clutch-db-sql-skip-literal-or-comment` only where a literal or comment can start. Every existing caller wanted either a character set (`;`, `,`, parentheses) or a position set (clause keywords), so all of them use the restriction; the per-character mode remains for a caller that matches a keyword at each position itself, and a test checks that both modes report the same code positions across the dialect corpus. Literal and comment bodies are searched for their terminator instead of walked, with match data saved. Masking jumps to the next possible opener the same way, and blank-line bounds visit only blank lines.

The context features share one scan per buffer state. `clutch--statement-scan` keeps the accessible text and its breaks, keyed by modification tick, restriction and dialect, so the bounds, the top-level token and the table cache of one request read one scan. After an edit, the old and new text are compared once; breaks before the first differing character are still valid, because scanner state at an offset depends only on the text before it, and the scan resumes just past the last of them, where the text is top-level code. Typing at the end of a script rescans its last statement only. Execution keeps its own uncached split, since it needs no cross-request state; it is faster only through the scanner.

Completion past a terminating semicolon returns nothing. That position belongs to no clause of the statement before it, and offering that statement's columns there was wrong as well as the most expensive request.

## Rejected alternatives

### Track edits with `after-change-functions`

Recording the lowest changed position would avoid comparing the texts, but it needs a hook installed by the mode, state that must be reset by every consumer, and a fallback for changes made with modification hooks inhibited. Comparing 4 MiB of text costs 3 ms and needs no state outside the cache. The price is memory: the cache keeps one copy of the accessible text per buffer until the next request replaces it, and two copies exist for the moment of a comparison. For a console holding a 4 MiB paste that is one more 4 MiB string, which the hook approach would avoid; accepted for now.

### Skip analysis above a statement size

Eldoc and completion could give up on statements over some size. After the scanner change a single 4 MiB statement costs about 60 ms per keystroke, which is the cost of finding its tables at all; a threshold would remove that hint for the statements where it is least known, to save a cost that is no longer a stall. Deferred until a measurement shows it is needed.

### Remove the per-character scan mode

Only `clutch--risky-dml-trivially-true-expression-p` still uses it, matching a keyword at each top-level position under the buffer's own syntax table. Converting it to collected positions would change which word boundaries it sees, in the path that decides whether a full-table `UPDATE` or `DELETE` asks for confirmation. Left alone.

## Consequence

On the same 4 MiB paste: Eldoc on a trailing identifier 63 ms, completion after the terminating semicolon 29 ms, the next keystroke 3 ms in a script and about 60 ms inside one long statement, and `clutch-execute-buffer` prepares its statements in 33 ms instead of 290 ms. The first request after a paste still copies the buffer and scans it once (34 ms at 4 MiB), and sql-mode's own `syntax-propertize` still runs over the pasted text once for redisplay (150 ms at 4 MiB when it contains doubled quotes), which Clutch does not control. `test/clutch-bench.el` gained `clutch-bench-run-sql-context`, which times these paths on a synthetic script without a database.
