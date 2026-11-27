# clutch Architecture

Current module map and boundaries for `clutch`. Design history lives in [`postmortem/`](../postmortem/).

## Layered Module Architecture

```mermaid
flowchart TB
  Entry["clutch.el<br/>entry point, customization,<br/>public commands, mode assembly"]

  subgraph Workflow["Workflow, UI, and query-buffer modules"]
    direction TB
    subgraph QueryPath["Query buffer path"]
      direction LR
      SQL["clutch-sql.el<br/>SQL context, completion,<br/>Eldoc, xref"]
      Document["clutch-document.el<br/>document query-buffer modes,<br/>current MongoDB helper syntax"]
      Query["clutch-query.el<br/>query consoles, execution,<br/>statement boundaries"]
    end

    subgraph WorkflowModules["Workflow and UI modules"]
      direction LR
      Conn["clutch-connection.el<br/>connection lifecycle, auth,<br/>SSH/TRAMP transport, transactions"]
      Result["clutch-result.el<br/>result lifecycle/state owner,<br/>action registry, record/value workflows"]
      Object["clutch-object.el<br/>object discovery, describe buffers,<br/>object actions"]
      ER["clutch-er.el<br/>ER view of a table:<br/>its keys in both directions"]
      Edit["clutch-edit.el<br/>staged mutation state,<br/>validation and submission"]
      Schema["clutch-schema.el<br/>schema refresh lifecycle,<br/>metadata caches"]
      UI["clutch-ui.el<br/>grid and header rendering helpers,<br/>footer, navigation"]
    end
  end

  Diagnostics["clutch-diagnostics.el<br/>leaf diagnostics service:<br/>redacted debug capture, problem records"]

  Viz["clutch-viz.el<br/>leaf text charts:<br/>pure functions over Lisp data"]

  Facade["clutch-backend.el<br/>generic database API,<br/>result struct, capability gates,<br/>shared SQL helpers"]

  subgraph Adapters["Backend adapters"]
    direction LR
    MySQL["clutch-db-mysql.el<br/>MySQL adapter"]
    PG["clutch-db-pg.el<br/>PostgreSQL adapter"]
    SQLite["clutch-db-sqlite.el<br/>SQLite adapter"]
    Mongo["clutch-mongodb.el<br/>MongoDB document adapter"]
    Redis["clutch-redis.el<br/>Redis key/value adapter<br/>and clutch-redis-mode"]
    JDBC["clutch-db-jdbc.el<br/>JDBC adapter and sidecar client"]
  end

  subgraph External["External packages"]
    direction LR
    MySQLExt["mysql.el"]
    PGExt["pgsql.el"]
    SQLiteExt["Emacs sqlite-*"]
    MongoExt["mongodb.el"]
    RedisExt["redis.el"]
    Agent["clutch-jdbc-agent.jar"]
    Drivers["JDBC driver jars"]
  end

  style Workflow fill:#f6f8fa,stroke:#6e7781,stroke-width:2px
  style Adapters fill:#f6f8fa,stroke:#6e7781,stroke-width:2px
  style External fill:#f6f8fa,stroke:#6e7781,stroke-width:2px

  Entry --> Workflow
  Workflow --> Diagnostics
  Workflow --> Facade
  Diagnostics --> Facade
  Facade --> Adapters

  SQL --> Query
  Document --> Query

  MySQL --> MySQLExt
  PG --> PGExt
  SQLite --> SQLiteExt
  Mongo --> MongoExt
  Redis --> RedisExt
  JDBC --> Agent
  Agent --> Drivers
```

Workflow modules call generic `clutch-db-*` operations in `clutch-backend.el` rather than protocol packages directly; adapters own database-specific parameters, metadata, execution, and type mapping.

- **Query modes:** `clutch-query.el` owns console execution and `clutch-mode`, while `clutch-sql.el` owns SQL parsing, completion, Eldoc, and xref. `clutch-document.el` provides `clutch-mongodb-mode` through backend registry metadata; `clutch-redis.el` defines the smaller `clutch-redis-mode` in the adapter layer using lazy `declare-function` references.
- **Results, edits, and UI:** `clutch-result.el` owns result lifecycle, paging/filter/sort, refine mode, query plans, and the result action registry; `clutch-edit.el` owns staged mutation payloads and validation; `clutch-ui.el` provides grid/header rendering helpers.
- **Atomic submission:** `clutch-edit.el` handles statement order and row-count checks while backends implement `clutch-db-call-with-atomic-batch` (Auto-mode transaction or Manual-mode savepoint; JDBC exposes opaque savepoint IDs). Failed savepoint release can roll back cleanly, whereas an unknown commit outcome sets `uncertain` until explicit rollback or reconnect.
- **ER view:** `clutch-er.el` reads cached columns/outgoing keys, queries incoming keys (`clutch-db-referencing-keys`) asynchronously on native PG/MySQL and JDBC via `clutch-connection.el`'s activity lifecycle, draws with `clutch-viz.el`, and persists manual browsing-only JSON relations (also exposed to Record view via `clutch-er-outgoing-relations`).
- **Leaf services:** `clutch-viz.el` draws pure-Lisp text charts with zero Clutch dependencies; `clutch-diagnostics.el` owns redacted debug capture (`*clutch-debug*`) and problem records over the backend facade without depending on `clutch-connection.el`.

## Module Dependencies

```mermaid
flowchart LR
  Backend["clutch-backend.el"]
  Diagnostics["clutch-diagnostics.el"]
  Schema["clutch-schema.el"]
  UI["clutch-ui.el"]
  SQL["clutch-sql.el"]
  Conn["clutch-connection.el"]
  Query["clutch-query.el"]
  Document["clutch-document.el"]
  Edit["clutch-edit.el"]
  Object["clutch-object.el"]
  Result["clutch-result.el"]
  ER["clutch-er.el"]
  Viz["clutch-viz.el"]

  Diagnostics --> Backend
  Schema --> Diagnostics
  UI --> Schema
  SQL --> Schema
  Conn --> UI
  Query --> Conn
  Query --> SQL
  Document --> Query
  Edit --> Conn
  Object --> Conn
  Object --> SQL
  Object --> Viz
  Result --> Query
  Result --> Edit
  Result --> Object
  Result --> Viz
  Result --> ER
  ER --> Object
  ER --> Conn
  ER --> Viz
```

Arrows mean `require` (transitive edges omitted). Loading `clutch.el` pulls in the SQL and result workflows; `clutch-document.el` loads on demand.

## Backend And Surface Model

```mermaid
flowchart LR
  Registry["Backend registry<br/>:support-level<br/>:data-model<br/>:query-mode<br/>:surfaces"]

  subgraph Relational["Relational SQL data model"]
    NativeSQL["Native SQL adapters"]
    JdbcSQL["SQL via JDBC"]
  end

  Cassandra["Cassandra<br/>CQL via JDBC<br/>read-only result grid"]

  subgraph DocumentDB["Document data model"]
    MongoBackend["mongodb backend<br/>basic document support"]
    MongoNative["native document surface<br/>clutch-mongodb-mode"]
    MongoSQL["SQL Interface surface<br/>:surface sql-interface<br/>clutch-mode"]
  end

  subgraph KeyValue["Key/value data model"]
    RedisBackend["redis backend<br/>basic key/value support"]
    RedisNative["native command surface<br/>clutch-redis-mode"]
  end

  Registry --> NativeSQL
  Registry --> JdbcSQL
  Registry --> Cassandra
  Registry --> MongoBackend
  Registry --> RedisBackend

  MongoBackend --> MongoNative
  MongoBackend --> MongoSQL
  MongoNative --> MongoExt["mongodb.el native client"]
  MongoSQL --> JDBCPath["clutch-db-jdbc.el<br/>MongoDB JDBC driver"]

  RedisBackend --> RedisNative
  RedisNative --> RedisExt["redis.el native RESP client"]
```

Native backends register in `clutch-backend--registry`; `clutch-db-jdbc.el` adds entries from `clutch-jdbc--driver-metadata` on load (see [backend-support.md](backend-support.md)).

- `mongodb` is a single backend whose default surface is native document (`mongodb.el`) and optional `:surface sql-interface` routes through JDBC.
- `redis` is a separate key/value backend; SQL-only result/mutation actions are gated by the registered data model or SQL Interface surface.
- `xtdb` registers its own backend reusing the PostgreSQL adapter with subtype overrides (postmortem 208); DuckDB currently enters through generic `jdbc`.

## Connection Flow

```mermaid
flowchart TB
  Saved["Saved connection params<br/>or manual reader params"]
  Canon["Canonicalize params<br/>backend aliases, surface symbols"]
  Auth["Materialize credentials<br/>:password, :pass-entry,<br/>auth-source/pass"]
  Transport{"Transport requested?"}
  Structured{"Structured<br/>:host/:port?"}
  URLBlocked["user-error<br/>structured forwarding requires<br/>:host/:port, not :url"]
  SSH["OpenSSH local forward<br/>through :ssh-host"]
  TRAMP["TRAMP TCP forward<br/>ssh-like or container relay"]
  Direct["Direct connection params"]
  Forwarded["Forwarded params<br/>host 127.0.0.1<br/>port LOCAL_PORT"]
  BackendParams["Backend-facing params<br/>strip Clutch-only keys"]
  Connect["clutch-db-connect<br/>through facade"]
  Adapter["Backend adapter<br/>validation/defaults"]
  Live["Live connection<br/>transport and remote params cached"]

  Saved --> Canon
  Canon --> Auth
  Auth --> Transport
  Transport -- no --> Direct
  Transport -- ssh/tramp --> Structured
  Structured -- no, opaque :url --> URLBlocked
  Structured -- yes, ssh --> SSH
  Structured -- yes, tramp --> TRAMP
  SSH --> Forwarded
  TRAMP --> Forwarded
  Direct --> BackendParams
  Forwarded --> BackendParams
  BackendParams --> Connect
  Connect --> Adapter
  Adapter --> Live
```

SSH and TRAMP forwarding sit below the backend layer and rewrite only structured `:host` / `:port` endpoints; opaque `:url` entries are passed through unchanged.

## Query And Object Flow

```mermaid
sequenceDiagram
  participant Buffer as Query buffer (SQL, MongoDB, Redis)
  participant Query as clutch-query.el
  participant Backend as clutch-backend.el
  participant Adapter as Backend adapter
  participant Result as clutch-result.el
  participant UI as clutch-ui.el

  Buffer->>Query: Execute statement, region, or buffer
  Query->>Query: Find statement bounds and execution context
  Note over Query,Adapter: A batch holds the connection between its statements
  Query->>Backend: clutch-db-query-async
  Backend->>Adapter: Dispatch by connection/backend type
  alt Adapter runs SQL without blocking (MySQL, PostgreSQL, JDBC)
    Adapter-->>Query: Started, and the statement shows as running
    Adapter-->>Query: clutch-db-result or error, from an idle timer
  else Adapter cannot (SQLite, MongoDB, Redis)
    Query->>Backend: clutch-db-query, blocking Emacs
    Adapter-->>Query: clutch-db-result or error
  end
  Query->>Result: Install result state
  Result->>UI: Render shared grid, header, and footer
  opt Result-grid action needs backend support
    Result->>Result: Resolve action through result action registry
    Result->>Backend: Capability-gated refine, edit, export, or native mutation
    Backend->>Adapter: SQL-surface or native-surface operation
    Adapter-->>Result: Rewritten SQL, DML/export text, or native helper
    Result->>UI: Re-render affected state
  end
```

```mermaid
sequenceDiagram
  participant ObjectBuf as Object browser / describe buffer
  participant Object as clutch-object.el
  participant Backend as clutch-backend.el
  participant Adapter as Backend adapter
  participant QueryBuf as Matching query buffer
  participant Output as Describe/action/result buffer

  ObjectBuf->>Object: Jump, describe, browse, or object action
  Object->>Backend: Metadata, definition, browse, or action API
  Backend->>Adapter: Dispatch by backend and object type
  alt Describe or metadata action
    Adapter-->>Object: DDL, source, JSON metadata, stats, validation, explain
    Object->>Output: Render describe/action buffer
  else Browse object
    Adapter-->>Object: Backend-owned browse command text
    Object->>QueryBuf: Open SQL, MongoDB, or Redis query buffer
  end
```

Execution always starts through `clutch-db-query-async`: async adapters (MySQL, PostgreSQL, JDBC) return immediately and deliver results from an idle timer, while synchronous adapters fall back to `clutch-db-query`. Multi-statement batches reserve their connection across statements, so background metadata and Eldoc check `clutch-db-in-use-p` (not just `clutch-db-busy-p`) before querying. Object browsing asks adapters for metadata, definitions, or backend-specific browse commands opened in the matching query mode.

## JDBC Runtime Shape

```mermaid
flowchart LR
  ClutchConn["One logical clutch connection"]
  JDBCAdapter["clutch-db-jdbc.el"]
  Agent["clutch-jdbc-agent.jar"]
  Primary["Primary JDBC session<br/>foreground SQL, transactions, DDL"]
  Metadata["Metadata JDBC session<br/>schema/object introspection"]
  Bulk["Bulk JDBC session<br/>Oracle only, opened on first use<br/>schema-wide listings"]
  Driver["JDBC driver jar"]
  DB["Database endpoint"]

  ClutchConn --> JDBCAdapter
  JDBCAdapter --> Agent
  Agent --> Primary
  Agent --> Metadata
  Agent -.-> Bulk
  Primary --> Driver
  Metadata --> Driver
  Bulk -.-> Driver
  Driver --> DB
```

The JVM sidecar separates foreground SQL from metadata introspection, and on Oracle opens a third `bulk` session lazily for schema-wide listings so large scans do not stall interactive lookups (see [jdbc-agent-protocol.md](jdbc-agent-protocol.md)).
