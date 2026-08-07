# 167 - JDBC BLOB Data Interface Release

## Context

The typed binary parameter envelope fixed Oracle `ORA-01465`, but agent 0.2.15 bound every non-null BLOB with `PreparedStatement.setBlob(InputStream, length)`. Oracle JDBC 19.21 implements that setter by creating, filling, binding, and later freeing a temporary BLOB before ordinary statement execution. The extra locator lifecycle added database round trips to each staged BLOB mutation.

The perceived Result Browser submit time has a second, independent component: after executing a staged batch, Clutch reruns the original result query so filters, triggers, generated values, ordering, and row membership reflect database truth. A query containing a full LOB scan can dominate that refresh even after parameter binding is faster.

## Decision

Pin published agent 0.2.16 and its verified release-asset checksum. Non-empty BLOB parameters use length-bearing `PreparedStatement.setBinaryStream`; typed SQL NULL still uses `setNull`, and a non-null zero-byte BLOB uses an explicitly managed empty `Blob` because Oracle otherwise stores an empty binary stream as SQL NULL.

Keep the existing post-mutation refresh contract. Do not hide its cost by leaving Result Browser rows stale or by assuming that a successful UPDATE cannot change result membership or computed values.

## Evidence

On the real Oracle 19.21 connection, repeated 871-byte same-value BLOB updates fell from 66–76 ms with agent 0.2.15 to 26.6–29.7 ms with the 0.2.16 candidate. Base64 encoding was approximately 0.029 ms per value and was not the bottleneck.

A rollback-only matrix verified exact non-null contents at 0, 1, 1,389, 32,765, 32,766, 32,767, 32,768, and 65,536 bytes. Every update affected one row, preserved non-NULL state and exact length/content, and was rolled back. The published jar was downloaded after release and matched SHA-256 `43cca03a539c5df1591afba5149c6aa466db0fc70d77add564d8d193bd325f9e`.

The measured original LOB-scan query still took 4.94–5.12 seconds. For that query, the roughly 40 ms binding saving is less than one percent of end-to-end submit latency; the remaining cost is truthful query refresh, not parameter serialization or JDBC binding.

## Alternatives Considered

- Keeping `setBlob(InputStream, length)` preserves type intent but retains the temporary-LOB lifecycle on every non-empty value.
- Skipping automatic result refresh makes the command feel faster but can display rows that no longer match the query or omit trigger/generated-column effects.
- Optimistically patching visible cells locally cannot preserve query semantics for joins, filters, ordering, inserts, deletes, or computed projections without a much larger result-reconciliation design.

The agent's driver-level rationale and timeout-safe empty-Blob lifecycle are recorded in [postmortem 020](https://github.com/LuciusChen/clutch-jdbc-agent/blob/v0.2.16/postmortem/020-blob-data-interface-binding.md).
