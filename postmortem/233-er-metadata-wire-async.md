# ER metadata over the asynchronous wire APIs

The ER prototype initially reused synchronous object metadata. Existing native metadata methods with `-async` names mostly schedule a synchronous read at idle time, so calling those methods alone would still hold the command loop while the database answers. ER loading now uses `pgsql-exec-async` and `mysql-query-async`, retaining synchronous fallback for SQLite and libraries without those APIs. The JDBC ER integration remains separate.

## Ownership and reuse

The adapters own SQL and decoding. New public metadata operations use the existing success-callback/error-callback contract; transport sequences retain one internal outcome callback so native wire requests and JDBC RPCs can use the same ordering and continuation rule. Existing synchronous primary-key, column and foreign-key methods share their SQL builders and row decoders with the new asynchronous metadata operations. Cold loads fetch primary keys, outgoing keys, columns and incoming keys sequentially on the same connection; warm loads only fetch incoming keys. MySQL 5.6/5.7 retains the existing dictionary lookup and its standard-query fallback for missing privileges or encoded names. No connection, permanent cache or worker thread is added.

The activity lifecycle helpers and their source-window binding moved unchanged from `clutch-query.el` to `clutch-connection.el`, beside the reservation and running-query state they use. ER and SQL workflows now share the same buffer/connection reply guard, cancellation flag and once-only release. A metadata sequence checks its continuation before every request, so cancellation stops later requests even when the backend refuses to cancel the current one. An already running background request is polled asynchronously before sending metadata. A killed or moved view stops the sequence and drops its reply. The metadata cache is updated only after the complete load succeeds in the owning view.

Native asynchronous reads have no client read-idle deadline, as asynchronous statements do; server-side timeouts and explicit cancellation still apply. This differs from the synchronous fallback's read-idle timeout. Metadata reads do not open a Clutch-managed Manual transaction or run the foreground namespace-following logic.

## Local measurement

PostgreSQL 16 and MySQL 5.6 ran in disposable OrbStack containers. A local TCP proxy delayed each server-to-client data block by 50 ms. The fixture was a three-column parent and a four-column child with one single-column and one composite foreign key. The connection context was initialized before timing, and cold/warm refers to table metadata. Each table entry records one local run, not a latency percentile. Measurements used byte-compiled code under batch Emacs 32.0.50, with the synchronous public ER command from `ed8eadf` as the baseline.

| Backend/cache | Sync command return | Async command return | Sync total | Async total |
| --- | ---: | ---: | ---: | ---: |
| PostgreSQL cold | 298 ms | 0.9 ms | 298 ms | 248 ms |
| PostgreSQL warm | 58 ms | 0.4 ms | 58 ms | 62 ms |
| MySQL cold | 398 ms | 0.8 ms | 398 ms | 354 ms |
| MySQL warm | 175 ms | 0.9 ms | 175 ms | 187 ms |

The benefit is returning control while the database answers; this does not promise faster total loading. The asynchronous run's longest 5 ms heartbeat gap was about 9 ms. Synchronous network waits also service regular timers, so heartbeat gaps alone do not establish interactive responsiveness. These are batch measurements, not graphical frame-time measurements.

The same real servers returned identical synchronous and asynchronous metadata. Cancellation, killing the view and replacing its connection each stopped after one metadata request, released the reservation and left the original connection able to execute `SELECT 1`. With a 200 ms background statement already running, the ER command returned in about 0.3–0.4 ms and completed after that statement. Focused workflow tests also cover a dispatch failure, refused cancellation and a nonlocal handler exit; the existing MySQL dictionary tests exercise both synchronous and asynchronous paths.

Metadata status notifications update the ER execution/header state without reverting its model. A real record-navigation regression showed that reverting an open ER view from the generic status hook started an asynchronous ER activity during another command's metadata load, reserving the connection before that command sent its SQL. Explicit ER refresh and navigation remain the owners of metadata reloads. The existing status-hook test covers this boundary alongside stale and healthy derived buffers.
