# 155 — JDBC primary-session invalidation

> Superseded for failures proven before statement creation by [156 — JDBC idle preflight and bounded replay](156-jdbc-idle-preflight-and-bounded-replay.md). Failures with an unknown execution outcome still follow this record.

## Context

The shared JDBC agent can remain healthy after Oracle or the network destroys one server session. Agent 0.2.9 correctly began poisoning unsafe primary sessions, but its error response did not communicate that lifecycle mutation. Clutch therefore kept the old `conn-id` in its local live map and every later command sent an id the agent had already removed.

## Decision

Agent 0.2.11 emits `diag.connection-invalidated=true` whenever an error response references a logical primary connection absent from its authoritative connection map. This covers both the failure that removes the session and later requests if the first response was ignored. The JDBC adapter treats that marker as authoritative, removes only the matching local handle and request state, and preserves the original structured failure. It does not classify Oracle messages, JDBC exception names, SQLState values, vendor codes, or error text in Elisp.

The query workflow keeps the dead connection object and buffer-local parameters as a reconnect anchor while clearing transaction state, metadata caches, and any old transport. The failed command is displayed once and is never replayed. On the next user command, the existing connection guard creates one new session and rebinds every attached buffer before executing once.

## Rationale

Replaying even a `SELECT` is unsafe because SQL can advance sequences, call functions with side effects, acquire locks, or fail after the server already executed it. Retaining a dead logical anchor separates safe session reconstruction from unsafe command replay and matches the native backends' rule: reconnect before a command when death is already known, but never replay a command whose outcome is ambiguous.

## Consequences

Laptop sleep and idle-timeout failures no longer degrade into repeated `Unknown connection id` errors. Other JDBC connections in the shared JVM remain live, the first socket error stays available in diagnostics, staged client-side edits remain attached, and a dirty manual transaction is cleared with an explicit unknown-outcome warning.

## Deliberate limit

This is not connection pooling, a periodic keepalive, or a general retry framework. A failure first discovered after SQL was sent still fails that command; automatic recovery applies to the next user command.
