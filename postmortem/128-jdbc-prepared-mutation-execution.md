# 128 - JDBC staged mutations keep their parameter boundary

## Context

Clutch's mutation builders already produce SQL templates plus positional values. Native backends bound those values, but JDBC used the generic literal-rendering fallback, so the parameter boundary was lost before execution.

## Decision

The JDBC adapter sends `execute-params` with a positional `values` array. The agent prepares the SQL and binds each value. Preview remains rendered SQL because its purpose is to show the user the effective mutation, not to define the wire format.

The wire format carries values only. Clutch parameter type tags are backend-specific, and generic JDBC metadata does not provide a portable binding type for every driver. An ignored type field would create a false contract.

## Consequences

JDBC staged updates, deletes, and inserts no longer depend on literal escaping. The Clutch unit test verifies the emitted RPC, while the agent integration test drives JSON and a real JDBC prepared statement through execution and result serialization.
