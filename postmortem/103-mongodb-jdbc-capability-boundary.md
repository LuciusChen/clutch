# 103 -- Native MongoDB and SQL Interface Boundary

_Updated by 113: Redis now has its own basic key/value backend and does not share the MongoDB document contract._

## Context

MongoDB has two practical surfaces that look similar from connection metadata but behave very differently:

- ordinary MongoDB deployments speak the MongoDB protocol and are normally driven through MongoDB Shell / driver APIs
- MongoDB SQL Interface exposes a JDBC SQL surface for Atlas / Enterprise Advanced environments

DataGrip treats MongoDB as a first-class document database connection and also has SQL translation features.  Treating the official SQL Interface JDBC driver as the only `mongodb` implementation would make Clutch fail for normal local `mongod` and common `mongodb://` / `mongodb+srv://` connections.

## Decision

Keep one user-facing backend symbol:

- `mongodb` is the backend for ordinary MongoDB deployments
- `:surface sql-interface` selects MongoDB SQL Interface for Atlas / Enterprise Advanced environments
- MongoDB SQL Interface is a surface of the `mongodb` backend, not another backend or user-visible driver

The ordinary document surface uses the external `mongodb.el` native MongoDB protocol package.  Clutch owns the adapter and the supported MongoDB Shell/MQL helper syntax, then translates that helper surface into `mongodb.el` command/helper calls.  This avoids making a local `mongod` depend on Atlas / Enterprise SQL Interface availability, while still keeping the protocol code outside the Clutch UI package.

The JDBC backend keeps MongoDB SQL Interface-specific behavior behind the surface selection:

- the JDBC URL uses the `jdbc:mongodb://...` prefix
- `:database` becomes the driver's required `database` property
- `:auth-database` / `:auth-source` optionally populate the MongoDB URL path
- pagination uses SQL Interface `LIMIT/OFFSET`
- manual transaction controls are not exposed

## Consequences

Default `mongodb` connections use native `mongodb.el` command execution, not SQL and not a shell JavaScript runtime.  Collections map to Clutch table metadata, sampled top-level document keys map to columns, and nested document/array values render as JSON cells.

The SQL Interface surface is useful only for environments that actually provide MongoDB SQL Interface.  A local community `mongod` container is enough for the ordinary `mongodb` live suite, but it is not enough for SQL Interface live coverage.

The UI should keep using backend capability predicates instead of scattering MongoDB conditionals.  Future non-relational backends should follow the same pattern: register a concrete backend symbol, advertise only the capabilities it really supports, and keep vendor-specific behavior inside the adapter.

Redis is not a follow-on implementation of the MongoDB adapter.  It needs a separate key/value command contract, value viewers, TTL metadata, and carefully bounded edit semantics before clutch should claim even basic support.

The protocol/client boundary is now enforced by public API names:

- Clutch may call public `mongodb-` symbols
- Clutch must not call private `mongodb--*` symbols from the external package
- protocol details and protocol capability docs belong in `mongodb.el`

## Non-goals

- pretending MongoDB Shell JavaScript is SQL
- making native MongoDB result buffers editable through SQL row identity
- exposing SQL transaction controls where the backend cannot support them
