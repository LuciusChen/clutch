# 215 — A Reconnect Returns to the Namespace, or Says It Cannot

## Evidence

A namespace switch records parameters for the automatic reconnect, and each case below was reproduced on fbe31b0 (0.5.5).

- ClickHouse given by a `:url` naming `dba`: the table list showed the tables of `default`. After `clutch-switch-schema` to `dbb`, Clutch showed and listed `dbb`, but the new connection was opened with the unchanged `:url`, so `currentDatabase()` stayed `dba`, an unqualified `INSERT` went there, and the automatic reconnect said "Reconnected to …/dbb" and stayed there too.
- MongoDB given by a `:url` naming `dba`: after a switch to `dbb`, the automatic reconnect returned to `dba`, since mongodb.el takes the URL's database over `:database`, and the next insert landed there. Against a MongoDB 7 whose user is defined in `admin`, connected with structured parameters or with a URL that names no database, the reconnect after a switch failed with "Authentication failed": the switch wrote `:database`, which mongodb.el also takes for the authentication database when no `authSource` names one.
- DuckDB moved into an attached database with `USE att`, or working in an in-memory database: after the connection was lost, the automatic reconnect opened the URL's database file, or a new, empty in-memory database, ran the next statement there and said "Reconnected to ?:?".

## Decision

- ClickHouse takes its database from `clutch-db-database`, which reads it from a `:url` in any form the driver takes, with a protocol, credentials, an IPv6 address, a list of hosts or tags, and from its last `database` property, which the driver prefers to the path, for its table list and metadata scope, and its switch rewrites the database the `:url` names in the parameters it reconnects with, every `database` property when there is one, so the server and Clutch agree. A first version rewrote only the path, and a review found that a `:url` such as `…/default?database=dba` became `…/dbb?database=dba`: Clutch showed and listed `dbb` while unqualified SQL ran in `dba`. The generic JDBC URL parser, which the other backends' host and port come from, is left as it was.
- A MongoDB switch records the database in `:schema`, which `clutch-mongodb-connect` works in, and leaves `:database` and the `:url` to say where mongodb.el authenticates.
- `clutch-db-unreachable-namespace` names the namespace a connection is in when no new connection can return there: on DuckDB an attached or in-memory database, once the connection has said where it is. The automatic reconnect, and reopening a console whose connection was lost, refuse with a message naming it, and `C-c C-e` connects anew; reopening shows the console before it reconnects, so the refusal leaves the user where that key works, and the retry after an idle disconnect does not retry such a session. A relative URL file is resolved against the JDBC agent's directory, which Emacs does not know, so with one only an in-memory database is refused: on 0.5.5 a mismatch there only left the parameters as they were, and refusing every reconnect would have been worse.
- One live test, run against every backend with a switch, moves a console with `clutch-switch-schema` and checks that a connection with its parameters is where the server says the console is. MongoDB has a test of its own in the backend suite.
- The `clutch-db-namespace` protocol the session-state plan proposed, uniting the namespace shown, its identity and its restore parameters, is not built: each fix needed one of them only, and the protocol waits for a case that needs them together.

## Limits

- The reconnect message still names a URL-only JDBC connection as `?:?`.
- A ClickHouse `database` property written URL-encoded, such as `%5F` for `_`, is read as written, though the driver decodes it.
- With a relative DuckDB URL, a move into an attached database is not told apart, and the automatic reconnect returns to the URL's file, as before.
- Redis is not in the shared live test, whose suites do not run against it; its switch records `:database`, which redis.el applies when connecting.

## Verification

- Live tests fail on fbe31b0 and pass here: a ClickHouse console opened with a `:url` lists that database's tables, is moved by a switch and reconnects there, with the database in the path and, failing on the first version of this change too, in a `database` property; a DuckDB console in an attached database, in an in-memory one and in a file attached to an in-memory one refuses to reconnect, naming it; and a MongoDB connection with the parameters a switch recorded works in the database switched to.
- The shared live test passes on MySQL, PostgreSQL, DuckDB and ClickHouse, and in the Oracle suite.
- Against a MongoDB 7 with its user in `admin`, structured parameters, a URL without a database and a URL naming `dba` with `authSource=admin` all reconnect into `dbb` after a switch.
- Unit tests cover the ClickHouse table list for each URL form and the URL rewrite, MongoDB's recorded parameters and working database, DuckDB's unreachable namespaces with absolute, relative and in-memory URLs, the refusal, reopening a console whose reconnect is refused, and the idle retry.
