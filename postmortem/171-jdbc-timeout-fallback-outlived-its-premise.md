# 171 -- The JDBC Timeout Fallback Outlived Its Premise

## Background

The sync RPC timeout fallback killed the shared agent JVM, and its comment explained why: the agent is likely blocked on a dead JDBC call, so requests would pile up behind the stuck op. That was true when the agent served requests on one thread. The agent has dispatched on a 48-thread pool for some time, so a silent request says nothing about the other connections' sessions -- but the fallback and its comment survived the architecture change. One slow metadata call, the easiest trigger because metadata RPCs have no server-side statement timeout, destroyed every JDBC connection at once together with their open transactions; Oracle sessions default to manual commit, so that loss is silent. The async RPC path had already been scoped for years: its timeout just errbacks and ignores the late reply.

## Decision

A timeout with a live agent condemns only the owning connection: retire its registrations, ask the agent to drop it, ignore its late reply. The process-wide reset remains for the two cases where nothing narrower exists -- the agent process actually died, or the silent request has no owning connection (the startup handshake, whose fresh process never became ready, and connect, whose logical connection has no id yet that a scoped release could name).

The scoped release then needed a second fix, found by the same audit: the ordinary `disconnect` op queues behind the connection's foreground and metadata locks in the agent -- the very locks the stuck call is holding -- so the release could never land, and each retry pinned another request thread behind the same lock. The agent already had the right primitive, `ConnectionManager.poison`, which removes the session from the map first and closes its JDBC resources off-thread where a non-cooperative driver cannot delay invalidation; it was used internally on connection failures but never exposed as a protocol op. `force-disconnect` (agent 0.2.17) dispatches unlocked and runs that path. The integration test wedges a connection inside its own foreground lock with a sleeping statement and asserts the forced release returns promptly. Older agents answer the unknown op with an error the client ignores, no worse than the old behaviour.

## Tradeoff

A genuinely wedged-but-alive JVM -- a GC death spiral, say -- is no longer killed automatically; connections retire one by one and the process lingers. The old fallback could not distinguish that case either, it just destroyed everything always, so the scoped behaviour trades a rare unrecovered zombie for the common case of innocent connections surviving.

The lesson worth keeping: a fallback's justifying comment is a premise statement. When the architecture that made it true changes, the fallback does not update itself -- audits of "obviously correct" recovery paths should start by checking whether their premises still hold.
