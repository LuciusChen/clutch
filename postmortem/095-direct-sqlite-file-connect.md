# Direct SQLite File Connect

Date: 2026-05-14

## Context

SQLite is file-based, so the common ad hoc workflow is "open this local
database file and inspect it".  clutch already supported manual SQLite
connection params, but only when `clutch-connection-alist` was empty.

Once a user configured any saved connection, ordinary `C-c C-e` showed only the
saved connection picker.  That made SQLite feel heavier than its underlying
connection model.

## Initial Decision

The first slice kept query consoles saved-connection-only and added a direct
`SQLite file...` candidate to the ordinary `clutch-connect` picker when saved
connections exist.

The new candidate returns the same params shape as a saved SQLite profile:

```elisp
(:backend sqlite :database "/path/to/file.db")
```

## Correction

That was still the wrong workflow.  It let users connect a buffer to a SQLite
file, but did not create a place to write SQL.  Opening the `.db` file itself,
turning on `clutch-mode`, and then connecting is backwards: the database file
is data, not the query editor.

The corrected workflow is query-console-first:

- `M-x clutch-query-sqlite-file` prompts for a SQLite file and opens a connected
  SQL console.
- `M-x clutch-query-console` lists saved connections; pressing RET with no
  matching connection starts a temporary connection flow for any supported
  backend.
- The ad hoc SQLite console stores its file params buffer-locally, so `C-c C-e`
  inside that console reconnects to the same file without requiring a saved
  profile.

## Why

Query consoles originally used saved names for buffer identity and SQL
persistence.  Ad hoc SQLite consoles can still keep that model by using the
file-based connection identity for persistence and a display name derived from
the path.

This keeps SQLite inspection aligned with how users actually work: choose a
database file, then type SQL in a clutch-owned query buffer.
