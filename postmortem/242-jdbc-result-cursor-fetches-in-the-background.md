# JDBC result cursor fetches in the background

An asynchronous JDBC execution used to collect the remaining cursor rows in one synchronous callback. A 4,001-row SQL Server query and CSV export both held input through eight fetches on a delayed connection. Local DuckDB also blocked when exporting about 40 MB.

Keep one JDBC cursor collector for foreground results, schema refresh and scoped table completion. Foreground fetches use the existing request-id owner and do not release the connection or publish rows between batches. Each fetch retains its driver timeout and client deadline. A timeout or `C-g` during a foreground fetch retires the connection as before. A failed metadata fetch only cleans up its cursor. Export keeps the existing atomic file writer and Cassandra retains its bounded preview.

In an isolated macOS Emacs, a movement during SQL Server fetches reached redisplay within 1 ms for both the query and export. Oracle, SQL Server, ClickHouse and DuckDB retained their row and fetch counts. Oracle and SQL Server cancellation, delayed replies and disconnects left existing export files unchanged. The delayed-reply probe held a real fetch response past the client deadline before offering it again.
