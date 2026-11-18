# 241 - Keep BLOB viewing bounded

The value viewer works from the result already in memory. SQLite marks its dynamically typed result columns so the viewer can distinguish each BLOB from text in the same column. The JDBC adapter decodes the optional byte prefix into an incomplete value preview, keeping the existing copy, export and mutation safeguards. Decoded JSON/XML retains its existing text and encoding.

Small structured BLOBs keep their JSON or XML viewer. Larger raw values use the existing text and hex previews before attempting structured parsing. A 4 MiB SQLite JSON BLOB previously took about 182 ms to toggle to hex and back. With bounded previews, 1,000 toggle pairs averaged 0.042 ms per pair and issued no SQL. The [JDBC protocol](../docs/jdbc-agent-protocol.md) describes when a byte prefix is available.
