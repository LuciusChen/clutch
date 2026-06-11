# clutch Architecture

This document is the current architecture review map for `clutch`. It describes
the module boundaries that should hold after the relational SQL, document
database, and JDBC surface refactor. Historical rationale lives in
[`postmortem/`](../postmortem/); this file should describe the current design.

## Layered Module Architecture

```mermaid
flowchart LR
  Entry["clutch.el<br/>entry point, customization,<br/>public commands, mode assembly"]

  subgraph Workflow["Workflow, UI, and query-buffer modules"]
    direction TB
    Conn["clutch-connection.el<br/>connection lifecycle, auth,<br/>SSH/TRAMP transport, transactions"]
    subgraph QueryPath["Query buffer path"]
      direction LR
      SQL["clutch-sql.el<br/>SQL context, completion,<br/>Eldoc, xref"]
      Document["clutch-document.el<br/>document query-buffer modes,<br/>current MongoDB helper syntax"]
      RedisQuery["clutch-redis-mode<br/>Redis command buffers<br/>(defined in clutch-redis.el)"]
      Query["clutch-query.el<br/>query consoles, execution,<br/>statement boundaries"]
    end
    Result["clutch-result.el<br/>result, record, value,<br/>sort/filter/export commands"]
    Object["clutch-object.el<br/>object discovery, describe buffers,<br/>object actions"]
    Edit["clutch-edit.el<br/>staged edit, insert, delete,<br/>validation and commit"]
    Schema["clutch-schema.el<br/>schema refresh lifecycle,<br/>metadata caches"]
    UI["clutch-ui.el<br/>grid rendering, header/footer,<br/>navigation, JSON metadata display"]
  end

  Facade["clutch-backend.el<br/>generic database API,<br/>result struct, capability gates,<br/>shared SQL helpers"]

  subgraph Adapters["Backend adapters"]
    direction TB
    MySQL["clutch-db-mysql.el<br/>MySQL adapter"]
    PG["clutch-db-pg.el<br/>PostgreSQL adapter"]
    SQLite["clutch-db-sqlite.el<br/>SQLite adapter"]
    Mongo["clutch-mongodb.el<br/>MongoDB document adapter"]
    Redis["clutch-redis.el<br/>Redis key/value adapter"]
    JDBC["clutch-db-jdbc.el<br/>JDBC adapter and sidecar client"]
  end

  subgraph External["External protocol/runtime packages"]
    direction TB
    MySQLExt["mysql.el"]
    PGExt["pg-el"]
    SQLiteExt["Emacs sqlite-*"]
    MongoExt["mongodb.el"]
    RedisExt["redis.el"]
    Agent["clutch-jdbc-agent.jar"]
    Drivers["JDBC driver jars"]
  end

  style Workflow fill:#f6f8fa,stroke:#6e7781,stroke-width:2px

  Entry --> Workflow
  Workflow --> Facade
  Facade --> Adapters
  Adapters --> External

  SQL --> Query
  Document --> Query
  RedisQuery --> Query
```

This diagram shows primary runtime/workflow ownership, not every `require` form.
Arrows between groups show layer boundaries; per-adapter runtime bindings are
intentionally not expanded in this overview.
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
`clutch-ui.el` is a shared rendering/helper module, not a separate workflow
entry point, so its same-layer helper edges are omitted from this overview.

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
    MongoSQL["SQL Interface surface<br/>:surface sql-interface/sql<br/>clutch-mode"]
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
MongoDB SQL Interface is a `:surface sql-interface` path on the same backend,
with `:surface sql` accepted as a short alias.  It is not a second public
backend, driver, feature, or manual chooser entry.
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
flowchart TB
  subgraph Consoles["Query consoles"]
    SQLConsole["clutch-mode<br/>SQL buffers"]
    MongoConsole["clutch-mongodb-mode<br/>MongoDB helper/MQL buffers"]
    RedisConsole["clutch-redis-mode<br/>Redis command buffers"]
  end

  subgraph LanguageHelpers["Language-specific buffer helpers"]
    SQLHelpers["clutch-sql.el<br/>SQL context/completion/Eldoc/xref"]
    DocumentHelpers["clutch-document.el<br/>current MongoDB highlighting,<br/>indentation, completion, explain command"]
    RedisHelpers["clutch-redis.el<br/>Redis command completion,<br/>line-oriented execution"]
  end

  subgraph ObjectUI["Object workflow"]
    Jump["Object lookup / jump"]
    Describe["Describe object"]
    Browse["Browse object"]
    Actions["Capability-gated actions<br/>profile, index insight,<br/>stats, validation, explain"]
  end

  Facade["clutch-backend.el<br/>generic API"]

  QueryWorkflow["clutch-query.el<br/>query-at-point/region/buffer,<br/>execution and marking"]
  QueryAPI["clutch-db-query<br/>clutch-db-build-paged-sql"]
  DefinitionAPI["clutch-db-object-definition"]
  BrowseAPI["clutch-db-object-browse-query"]
  MetadataAPI["schema/list/column APIs"]
  DocumentMetadataAPI["document capability APIs<br/>profile, index insight,<br/>validation, stats, explain"]
  KeyValueMetadataAPI["key/value metadata APIs<br/>key listing, type metadata,<br/>type-aware browse command"]

  Adapter["Backend adapter<br/>SQL, JDBC, document,<br/>or key/value"]
  ResultStruct["clutch-db-result"]
  ResultGrid["Result grid<br/>shared table renderer"]
  BrowseText["Backend-owned browse text<br/>SQL SELECT, document helper,<br/>or Redis read command"]
  DescribeBuffer["Describe buffer<br/>DDL/source or JSON metadata"]

  SQLHelpers --> SQLConsole
  DocumentHelpers --> MongoConsole
  RedisHelpers --> RedisConsole
  SQLConsole --> QueryWorkflow
  MongoConsole --> QueryWorkflow
  RedisConsole --> QueryWorkflow
  QueryWorkflow --> QueryAPI
  QueryAPI --> Facade
  Facade --> Adapter
  Adapter --> ResultStruct
  ResultStruct --> ResultGrid

  Jump --> MetadataAPI
  Describe --> DefinitionAPI
  Browse --> BrowseAPI
  Actions --> DocumentMetadataAPI
  Browse --> KeyValueMetadataAPI
  MetadataAPI --> Facade
  DefinitionAPI --> Facade
  BrowseAPI --> Facade
  DocumentMetadataAPI --> Facade
  KeyValueMetadataAPI --> Facade
  Adapter --> DescribeBuffer
  Adapter --> BrowseText
  BrowseText --> SQLConsole
  BrowseText --> MongoConsole
  BrowseText --> RedisConsole
```

The result grid is shared across SQL, document, and key/value query results.
Object definition, browse text, document object actions, and key/value metadata
are backend-owned so native non-SQL backends do not fall back to table-oriented
SQL behavior or MongoDB special cases. The object workflow asks the backend
whether an action is supported for the selected object, then calls the
corresponding generic API. Redis uses the same object workflow for KEY entries,
but its backend builds Redis read commands from key type metadata instead of
using document collection actions. Result-buffer actions use the same
capability boundary: SQL rewrite, SQL INSERT/UPDATE export, and staged SQL
mutation stay on SQL surfaces, while native document results keep
backend-neutral grid operations and ask the document adapter to build native
mutation snippets such as MongoDB helper calls.

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
