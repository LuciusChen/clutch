# clutch MongoDB Backend Guide

`:backend mongodb` connects to standard MongoDB deployments through the external `mongodb.el` package and evaluates common MongoDB Shell / MQL helpers in Emacs query buffers (it is a helper parser, not a general JavaScript runtime or SQL engine). For Atlas or Enterprise deployments with MongoDB SQL Interface, add `:surface sql-interface` to use JDBC instead (see [backend-support.md](backend-support.md)).

## Requirements

- **Native document mode (default):** `mongodb.el` on `load-path`. No Java, `mongosh`, or JDBC jar needed.
- **SQL Interface (`:surface sql-interface`):** Java 17+, the JDBC agent, and the official MongoDB SQL Interface driver (saved to `~/.emacs.d/clutch-jdbc/drivers/mongodb-jdbc.jar`):

```emacs-lisp
(clutch-jdbc-ensure-agent)
(clutch-jdbc-install-driver 'mongodb)
```

## Connection Examples

```emacs-lisp
(setq clutch-connection-alist
      '(("local-mongodb" . (:backend mongodb
                           :host "127.0.0.1"
                           :port 27017
                           :database "app"))

        ("local-mongodb-auth" . (:backend mongodb
                                :host "127.0.0.1"
                                :port 27017
                                :database "app"
                                :auth-database "admin"
                                :user "root"
                                :pass-entry "local-mongodb-auth"))

        ("atlas-sql" . (:backend mongodb
                         :surface sql-interface
                         :url "jdbc:mongodb://cluster0.a.query.mongodb.net/admin"
                         :database "analytics"
                         :user "reporter"
                         :pass-entry "atlas-sql"))))
```

Unauthenticated local instances need no `:user`, `:password`, or `:pass-entry`; for authenticated connections, prefer `:pass-entry` (`auth-source-pass` / `auth-source`). Use structured `:host` / `:port` if you want Clutch-managed SSH or TRAMP forwarding (opaque `:url` values are not rewritten). See [`mongodb.el`](https://github.com/LuciusChen/mongodb.el) for native protocol and URI options.

## Background Metadata

With a current `mongodb.el`, collection discovery, sampled field metadata (`clutch-mongodb-schema-sample-size`, default 20), and index warmup run asynchronously through Clutch's background metadata scheduler. Foreground commands take priority between requests. Each wire command enforces `mongodb-timeout-seconds`, and a timeout or broken transport closes the connection. Older `mongodb.el` versions fall back to idle synchronous metadata reads.

## Query Buffers

Native MongoDB buffers use `clutch-mongodb-mode` (from `clutch-document.el`) with the usual execute (`C-c C-c`), object (`C-c C-j`/`d`/`o`), database-switch (`C-c C-l`), completion (`C-c TAB`), and transient (`C-c ?`) keys, plus `C-c C-p` to explain the `find`, `findOne`, or `aggregate` call at point as formatted JSON.

### Supported helpers

- **Reads:** `find`, `findOne`, `aggregate`, `countDocuments`, `distinct`, `explain` (cursor chains: `sort()`, `skip()`, `limit()`, `maxTimeMS()`, `allowDiskUse()`, `explain()`)
- **Writes:** `insertOne` (one document), `insertMany` (document array), `updateOne` (top-level update operators like `{$set: ...}` only; update pipelines are unsupported), `replaceOne` (plain document, no top-level operators), `deleteOne` (requires an explicit filter)
- **Commands & literals:** `db.runCommand(...)`, `ObjectId(...)`, `ISODate(...)`

`find()` results are capped at `clutch-mongodb-find-result-limit` (default 1000; smaller explicit `.limit(N)` values are honored, while zero, negative, or larger limits are rejected). Unsupported shell syntax such as regex literals fails clearly rather than calling an external shell.

### Completion

Completion suggests collections, sampled field paths, common stages/operators, cursor chains after `find(...).` and `aggregate(...).`, and lightweight templates (`find({}).limit(20)`, projected `find`, `aggregate` and `$lookup` skeletons).

## Result Grid and Object Actions

Clutch maps MongoDB objects onto its shared browser:

- Databases → schemas; collections → tables; sampled field paths → columns; nested documents/arrays → JSON cells (viewable with `v`).
- Result grids are **read-only** (no staged SQL row editing or manual transactions), while copy/refine (`c`), CSV/TSV/Org export (`e`), page filter (`/`), and numeric aggregation (`A`) work as usual. Copy/export on collection query results can also generate `insertOne`, `insertMany`, `replaceOne`, `updateOne`, and `deleteOne` snippets.
- Jumping to a collection (`C-c C-j`) inserts `db.getCollection(...).find({}).limit(20);`. Object actions (`C-c C-o` or Embark) on a collection support:
  - **Describe (`d`):** JSON schema profile (sampled field paths, presence/type counts, sample values, indexes)
  - **Show definition (`s`):** collection metadata JSON
  - **Index insight (`i`):** index definitions combined with `$indexStats`
  - **Stats (`t`):** `$collStats` summary (`count`, `avgObjSize`, `storageSize`, `totalIndexSize`, `totalSize`, `indexSizes`)
  - **Explain sample (`e`):** `find({}).limit(1)` with `executionStats`
  - **Validation (`v`):** collection `validator` / `validationAction` / `validationLevel`

## Live Tests

Run native MongoDB live tests (`:mongodb-live`) via the test runner (starts or reuses a local container):

```sh
./test/run-ci.sh native-live
```

Or start a local container manually:

```sh
docker run -d --name clutch-mongodb-live -p 27017:27017 mongo:7
```

MongoDB SQL Interface JDBC tests are tagged `:sql-interface-mongodb-live` and require an Atlas or Enterprise Advanced SQL Interface endpoint (see [jdbc-backend.md](jdbc-backend.md)).
