# 113 Redis Basic Key/Value Backend

## Context

Clutch added MongoDB as a basic document backend through an external protocol package. Redis raised the same boundary question, but it is not a document database: its primary surface is command-oriented key/value and data-structure access.

Existing Emacs Redis packages were evaluated before implementation. `eredis` implements RESP in Elisp and can still run basic commands against Redis 7, but it is old, uses deprecated APIs, returns Redis errors as ordinary strings, has a global current-process fallback, and does not provide the error and connection model Clutch needs. `redis.el` from MELPA is primarily a `redis-cli` wrapper, which would repeat the shell dependency problem Clutch avoided for MongoDB.

## Decision

Implement a small standalone `redis.el` protocol package and integrate it lazily from Clutch's `redis` backend.

Clutch owns:

- saved connection params and auth-source/pass resolution
- Redis command query buffers
- key discovery and object browsing
- Redis result-grid shaping
- user-facing support-level documentation

`redis.el` owns:

- RESP2 command encoding and response parsing
- TCP connection lifecycle
- `AUTH` and `SELECT`
- structured Redis error conditions

## Boundaries

The Redis backend is basic key/value support. It deliberately does not claim:

- SQL row identity or staged SQL edit/insert/delete
- joins, relational rewrite/refine semantics, or SQL pagination
- pub/sub loops
- pipelining
- transactions
- cluster or Sentinel management
- stream consumer workflows
- RESP3 or TLS

Those features should not be added as ad hoc Clutch branches. If they become necessary, the protocol capability belongs first in `redis.el`; Clutch should then expose only the user workflow it can support cleanly.

## Consequences

The project now has three top-level data models in the backend registry: relational, document, and key/value. Redis keys are represented as `KEY` object entries. They are browseable like tables/collections from the user's point of view, but they do not receive MongoDB document actions or SQL mutation actions.

The adapter is intentionally small enough to keep in `clutch-redis.el`. If future key/value systems are added and real duplication appears, introduce a shared key/value contract then. Do not create a speculative generic key/value layer before there are two concrete backends.
