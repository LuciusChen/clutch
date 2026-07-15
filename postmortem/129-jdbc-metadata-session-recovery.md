# 129 - Recover only the failed JDBC metadata session

## Context

The JDBC agent isolates foreground SQL from metadata traffic with two server sessions. An idle timeout can therefore close metadata while the primary session and its transaction remain healthy. Treating the logical connection as lost leaves schema refresh failed and discards usable foreground state.

## Decision

Metadata connection recovery belongs in the agent. It retains the connection configuration and logical schema, replaces only metadata after a JDBC connection failure or failed liveness check, restores the schema, and retries the metadata operation once.

Clutch does not restart the process, reconnect the primary session, or add UI timing workarounds. A second failure crosses the normal error boundary and remains visible in diagnostics.

## Consequences

Schema and object refresh can recover after metadata idle timeout without changing foreground transaction state. Recovery is bounded to one retry and does not become connection pooling or a general query retry mechanism.
