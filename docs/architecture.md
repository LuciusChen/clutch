# clutch Architecture

This document is the current architecture review map for `clutch`. It describes
the module boundaries that should hold after the relational SQL, document
database, and JDBC surface refactor. Historical rationale lives in
[`postmortem/`](../postmortem/); this file should describe the current design.

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
      RedisQuery["clutch-redis-mode<br/>Redis command buffers<br/>(defined in clutch-redis.el)"]
      Query["clutch-query.el<br/>query consoles, execution,<br/>statement boundaries"]
    end

    subgraph WorkflowModules["Workflow and UI modules"]
      direction LR
      Conn["clutch-connection.el<br/>connection lifecycle, auth,<br/>SSH/TRAMP transport, transactions"]
      Result["clutch-result.el<br/>result lifecycle/state owner,<br/>action registry, record/value workflows"]
      Object["clutch-object.el<br/>object discovery, describe buffers,<br/>object actions"]
      Edit["clutch-edit.el<br/>staged mutation state,<br/>validation and commit"]
      Schema["clutch-schema.el<br/>schema refresh lifecycle,<br/>metadata caches"]
      UI["clutch-ui.el<br/>grid and header rendering helpers,<br/>footer, navigation"]
    end
  end

  Facade["clutch-backend.el<br/>generic database API,<br/>result struct, capability gates,<br/>shared SQL helpers"]

  subgraph Adapters["Backend adapters"]
    direction LR
    MySQL["clutch-db-mysql.el<br/>MySQL adapter"]
    PG["clutch-db-pg.el<br/>PostgreSQL adapter"]
    SQLite["clutch-db-sqlite.el<br/>SQLite adapter"]
    Mongo["clutch-mongodb.el<br/>MongoDB document adapter"]
    Redis["clutch-redis.el<br/>Redis key/value adapter"]
    JDBC["clutch-db-jdbc.el<br/>JDBC adapter and sidecar client"]
  end

  subgraph External["External protocol/runtime packages"]
    direction LR
    MySQLExt["mysql.el"]
    PGExt["pg-el"]
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
  Workflow --> Facade
  Facade --> Adapters

  SQL --> Query
  Document --> Query
  RedisQuery --> Query

  MySQL --> MySQLExt
  PG --> PGExt
  SQLite --> SQLiteExt
  Mongo --> MongoExt
  Redis --> RedisExt
  JDBC --> Agent
  Agent --> Drivers
```

This diagram shows primary runtime/workflow ownership, not every `require` form.
Arrows between the large groups show layer boundaries. Adapter-to-runtime
arrows are expanded only at the bottom layer, where they identify the external
protocol package or runtime each adapter delegates to.
The facade is the database contract boundary. Workflow modules call generic
`clutch-db-*` operations instead of protocol packages. Backend adapters own
database-specific connection params, metadata, object definitions, query
execution, and type mapping.

`clutch-query.el` is query-console workflow, not the SQL layer. SQL-specific
analysis and completion live in `clutch-sql.el` and are installed by
`clutch-mode`, whose major-mode definition remains in `clutch.el`. Document
query-buffer behavior is selected through backend registry metadata.
`clutch-document.el` currently provides `clutch-mongodb-mode` for MongoDB and
reuses the shared query workflow for execution. Future document backends should
register their own query mode instead of adding MongoDB branches to generic
workflow modules. Redis registers `clutch-redis-mode` from its adapter because
the mode is Redis-specific and small; protocol work still lives in `redis.el`.
`clutch-result.el` owns result buffer lifecycle state, paging/filter/sort state,
refine state, and the result action registry. `clutch-edit.el` owns staged
mutation payloads. `clutch-ui.el` is a shared rendering/helper module for
result grids and connection header-line presentation; it is not a separate
workflow entry point or result-state owner, so its same-layer helper edges are
omitted from this overview.

## Backend And Surface Model

```mermaid
flowchart LR
  Registry["Backend registry<br/>:support-level<br/>:data-model<br/>:query-mode<br/>:surfaces<br/>:normalize-fn"]

  subgraph Relational["Relational SQL data model"]
    MySQLCore["mysql<br/>core SQL"]
    PGCore["pg<br/>core SQL"]
    SQLiteCore["sqlite<br/>core SQL"]
    OracleCore["oracle<br/>core SQL via JDBC"]
    SQLServerCore["sqlserver<br/>core SQL via JDBC"]
    GenericJDBC["jdbc, clickhouse,<br/>snowflake, redshift, db2<br/>basic SQL / query-first"]
  end

  subgraph DocumentDB["Document data model"]
    MongoBackend["mongodb backend<br/>basic document support"]
    MongoNative["native document surface<br/>clutch-mongodb-mode"]
    MongoSQL["SQL Interface surface<br/>:surface sql-interface<br/>clutch-mode"]
  end

  subgraph KeyValue["Key/value data model"]
    RedisBackend["redis backend<br/>basic key/value support"]
    RedisNative["native command surface<br/>clutch-redis-mode"]
  end

  Registry --> MySQLCore
  Registry --> PGCore
  Registry --> SQLiteCore
  Registry --> OracleCore
  Registry --> SQLServerCore
  Registry --> GenericJDBC
  Registry --> MongoBackend
  Registry --> RedisBackend

  MongoBackend --> MongoNative
  MongoBackend --> MongoSQL
  MongoNative --> MongoExt["mongodb.el native client"]
  MongoSQL --> JDBCPath["clutch-db-jdbc.el<br/>MongoDB JDBC driver"]

  RedisBackend --> RedisNative
  RedisNative --> RedisExt["redis.el native RESP client"]
```

`mongodb` is one backend. Ordinary MongoDB uses the native document surface.
MongoDB SQL Interface is a `:surface sql-interface` path on the same backend.
It is not a second public backend, driver, feature, or manual chooser entry.
`redis` is a separate key/value backend with a native command surface. It is
not a document backend and should not reuse MongoDB collection/document actions.
SQL-only result and staged-mutation actions are gated by the registered
relational data model or by an explicit SQL Interface surface; native document
and key/value surfaces keep only backend-neutral grid actions unless their
adapter exposes a dedicated capability.
DuckDB currently has a JDBC driver source and URL/runtime helpers, but no
registered backend symbol; use it through the generic `jdbc` path.

## Connection Flow

```mermaid
flowchart TB
  Saved["Saved connection params<br/>or manual reader params"]
  Canon["Canonicalize params<br/>backend aliases, surface symbols,<br/>:tramp alias"]
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

Transport is below the backend data model. SSH and TRAMP rewrite only
structured TCP endpoints. Opaque JDBC URLs and MongoDB `mongodb://` /
`mongodb+srv://` URLs are not parsed or rewritten by Clutch.
Adapter-owned validation and defaults include backend-specific checks such as
removed timeout option rejection, SSL/TLS normalization, and JDBC timeout
defaults.

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
  Query->>Backend: clutch-db-query
  Backend->>Adapter: Dispatch by connection/backend type
  Adapter-->>Backend: clutch-db-result
  Backend-->>Query: Rows, columns, and result context
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

The result grid is shared across SQL, document, and key/value query results.
Query buffers differ by language helper and statement-boundary rules, but query
execution always converges in `clutch-query.el` before calling the generic
backend API. Object browsing is intentionally separate: `clutch-object.el` asks
the adapter for metadata, definitions, native actions, or browse command text.
Browse command text is opened in the matching query-buffer mode instead of
pretending that every backend has SQL tables. Result-buffer actions use a single
action registry owned by `clutch-result.el`, so SQL rewrite/edit/export stays on
SQL surfaces while native document/key/value surfaces expose only
adapter-supported operations.

## JDBC Runtime Shape

```mermaid
flowchart LR
  ClutchConn["One logical clutch connection"]
  JDBCAdapter["clutch-db-jdbc.el"]
  Agent["clutch-jdbc-agent.jar"]
  Primary["Primary JDBC session<br/>foreground SQL, transactions, DDL"]
  Metadata["Metadata JDBC session<br/>schema/object introspection"]
  Driver["JDBC driver jar"]
  DB["Database endpoint"]

  ClutchConn --> JDBCAdapter
  JDBCAdapter --> Agent
  Agent --> Primary
  Agent --> Metadata
  Primary --> Driver
  Metadata --> Driver
  Driver --> DB
```

JDBC uses a JVM sidecar because those databases are exposed through JDBC
drivers, not through pure Elisp protocol packages. The sidecar keeps foreground
queries separate from metadata refresh where the driver/database benefits from
separate sessions.
