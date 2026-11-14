# ER view of a table

`clutch-er-diagram`, `r` in the object actions, shows one hop of relations around a table: its key columns in a box, the tables its foreign keys reference and the tables whose foreign keys reference it, one line for each key. This is the first stage of an ER feature; user-defined relations and a wide layout are deferred, as listed at the end.

## Keys stay in one contract

A composite key must be one relation, so the view needs to know which columns form one key. `clutch-db-foreign-keys` returned one entry per column with no key identity. A new generic returning grouped constraints was considered and rejected: it would have put a second query and a second cache entry beside the per-column one, and every consumer of the cache would have had to choose between them. Instead the per-column contract gains `:constraint`, a value the columns of one key share, listed together in the key's order. PostgreSQL reports the constraint name, MySQL the constraint name, and SQLite the key's number in its table. The cache, Follow FK, describe buffers and completion hints are unchanged; a column in two keys has two entries, and per-column readers keep the first, as they did. JDBC does not report `:constraint` yet, and a missing key identity is left missing rather than invented: a JDBC connection refuses the view, since it also has no `clutch-db-referencing-keys`.

Reading the keys again showed three defects, each fixed with its own regression. PostgreSQL joined `information_schema` views by constraint name alone, which crossed the columns of a composite key, mixed in a same-named key of another table, dropped keys into other schemas and hid every key from a user who did not own the referenced tables; the catalogs (`conkey`/`confkey`) have none of these problems. MySQL did not report the referenced database, so Follow FK across databases opened a table of the same name in the current one. SQLite left the referenced column empty for `REFERENCES parent`, which means the parent's primary key, and Follow FK failed on it.

`clutch-db-referencing-keys` is new. Its default refuses with a `user-error` rather than returning nil, since an empty list would claim that nothing references the table. PostgreSQL leaves out the copies of a key that partitions inherit (`conparentid`). SQLite has no index of incoming keys, so it reads the keys of every table in the database; the view states that cost.

## Loading and drawing

The view loads synchronously, as describe does: native PostgreSQL and MySQL metadata already runs on the main thread when idle, so an asynchronous path would have added state without freeing Emacs. It refuses while a statement runs and waits for a background metadata request, as foreground SQL does. Columns and foreign keys come from the schema cache (`clutch--ensure-foreign-keys` is the synchronous twin of the async loader and runs the same update hook); the referencing keys are read each time the view loads, since only the view uses them. The model lives in the view's buffer, which binds the connection like any attached buffer, so `clutch--replace-connection` moves it.

Moving point only moves overlays: each highlightable line records the relations it belongs to, and the post-command hook acts only when that set changes. The buffer is drawn again only for a new table, a column toggle or a refresh.

Drawing uses two pure functions added to `clutch-viz.el`, `clutch-viz-columns` and `clutch-viz-box`. Widths come from `string-width`, and padding is real spaces carrying `(space :align-to COL)`, as the plan chart's is. Alignment is exact in a monospace font whose box and arrow glyphs are one column wide, in a terminal or a graphical frame, CJK names included, which take two columns. `clutch-viz-charset` now checks the extra glyphs a caller draws with (`◆ → ← ↔ …`); when any is two columns wide, as with `cjk-ambiguous-chars-are-wide`, the view draws with ASCII. A terminal whose own width for these glyphs differs from Emacs's cannot be detected. With a proportional font, `:align-to` keeps column starts and the box's right edge in place on graphical frames, but the top edge's run of `─` after a title can still drift, and a cell is not guaranteed to fit its column.

## Deferred

- User-defined relations in a versioned `relations.json`, created from a result column or the view, with endpoints whose schema may be left out to mean the table an unqualified name resolves to (`clutch-db-resolution-context`), and the default file keyed by the saved connection name rather than host and port, which change under SSH tunnels.
- A wide three-column layout with drawn connectors.
- JDBC: the agent's foreign-key operations must report key identity and the keys that reference a table.
- Follow FK in the record view still builds a one-column condition for a composite key.
- `clutch-db-primary-key-columns` on SQLite lists primary-key columns in table order rather than key order; the view's implicit-reference lookup sorts by key position itself.
