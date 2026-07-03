# REPL SELECT Result Display

## Context

The REPL printed `SELECT` results as a static inline table in the comint buffer.
That preserved history, but wide results became hard to inspect and bypassed the
main result buffer's header line, horizontal scrolling, CJK alignment, sort,
copy, export, and staged edit workflows.

## Decision

Successful REPL `SELECT` queries now open the standard result buffer and leave a
short row-count summary in the REPL.  DML summaries, errors, prompts, and
multi-line input stay inline.

## Rationale

The result buffer already owns table inspection.  Reusing it avoids a second
wide-table renderer in the REPL and keeps future result UX fixes in one place.
Keeping only the summary in comint preserves command history without making the
REPL unreadable for wide rows.
