# 198 — Schema-Wide Listings Off the Oracle Metadata Session

## Evidence

After postmortem 197 took the schema-wide index enumeration out of row identity, a `clutch-debug-mode` trace of the first query on the same Oracle connection still showed `row-identity ... Elapsed: 5.798s` for a table whose identity resolves in well under a second on its own. The same trace showed the background object warmup's schema-wide `get-indexes` (4461 indexes) in flight at that moment; on this database that one statement takes about seven seconds.

The agent serializes every metadata operation of a logical connection on one metadata session behind one lock. Postmortem 057 added that session so metadata traffic could not disturb the primary session, and it did not distinguish between a listing of the whole schema and a lookup of one named object. A warmup listing and a pre-query identity lookup are both metadata operations, so the identity lookup waited for the listing, and the query waited for the identity lookup. The cost belonged to neither call; it was the queue.

A local reproduction measured the queue directly: Oracle Free in a container, 400 tables with 1628 indexes, five samples per case. With agent 0.2.23 a row identity resolution that takes 11 ms alone took 41 ms while a 34 ms schema-wide index listing was running, that is, it waited for the whole listing. With agent 0.2.24 the same overlapped resolution takes 10 ms.

## Decision

The agent runs schema-wide listings on Oracle on a third session that opens on first use: `get-tables`, `get-sequences`, `get-procedures`, `get-functions`, and `get-indexes`/`get-triggers` without a `table`. Named-object lookups (`search-tables`, `get-primary-keys`, `get-columns`, table-scoped `get-indexes`, and the rest) keep the metadata session, so a listing can no longer stand between a query and its identity lookup. Cursors opened by a listing fetch on that session, and a bulk session broken by a connection failure is dropped and the listing retried once on a fresh one, which mirrors what the metadata session already did. The session is disposable in the other direction too: a schema switch drops it rather than switching it, and the next listing reopens it with the remembered schema, so the switch cannot half-succeed across three sessions.

Clutch does not change how it issues requests. The agent classifies a request by its operation and parameters, so the client cannot route a listing to the wrong session and the protocol gains no field. The client-side change is the pinned agent version and its checksum.

## Other JDBC backends

Only Oracle has shown this cost. Its dictionary views make a full listing take seconds on a large schema, and clutch warms object categories in the background shortly after connecting, so the listing lands exactly when the first query needs the metadata session. No other product has shown a listing slow enough for the queue to matter, and a third logon per connection would cost each of them a round trip and a server session for no measured gain. The agent therefore decides eligibility from the driver's product name at connect time and keeps two sessions everywhere else; their request path, lock order and failure handling are unchanged, which the agent's tests pin (`schemaWideListingStaysOnMetadataSessionWithoutBulkEligibility`, `bulkSessionStaysClosedForOtherProducts`).

## Rejected alternatives

### Delay or throttle the background warmup

Hides the contention only when the warmup starts late enough to miss the first query, and slows every browse to pay for a queue that exists only while a listing runs. The queue, not the warmup, was the defect.

### Run row identity on the primary session

Puts a metadata call back on the foreground session, which postmortem 057 removed because Oracle metadata traffic destabilized foreground statements. It would also serialize identity with any long-running foreground fetch.

### Open the bulk session at connect

Simpler in the agent, but every Oracle connection would pay a third logon whether or not the schema is ever listed. Opening on first use moves that logon to the first listing, which the background warmup issues off the user's critical path.

## Consequence

On Oracle a query issued while the object warmup runs no longer waits behind it. Metadata the query needs still runs synchronously before it, so the per-table costs recorded in postmortem 197 remain; only the queueing behind unrelated listings is gone. A logical Oracle connection now holds up to three database sessions, and the third appears at the first listing. It counts against the account's session limits. If Oracle refuses it (`ORA-02391` under `SESSIONS_PER_USER=2`), that connection stops using a bulk session: the listing is dispatched again under the metadata lock, later listings go there directly as on other products, and the logon is not attempted again. A first version reported the refusal instead, which on such an account failed every listing, and with it the schema cache and the object views that 0.2.23 served on two sessions. The reroute keeps the rule that a request runs only on the session its lock serializes; it releases the bulk lock before taking the metadata lock rather than borrowing the metadata session under the wrong lock.

The bulk session is idle between listings by design, and an idle connection is what a NAT or firewall drops without a word. Only the primary was validated after idling, so a listing on such a session waited out the 30-second network timeout; Clutch's request timeout is also 30 seconds, so it gave up first and force-disconnected the whole connection, losing an open transaction. The metadata session always had the same exposure; it is simply used more often. Reproduced against Oracle Free through a proxy that silences one connection without closing it: both failed after 30 s with the uncommitted row gone. The agent now checks an idle metadata or bulk session with `isValid(3)` after `validate-after-idle-seconds` and replaces it on failure, so the same requests answer in about 3.5 s on a fresh session with the row intact.
