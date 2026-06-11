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
      Document["clutch-document.el<br/>document query-buffer modes,<br/>MongoDB helper syntax"]
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
    JDBC["clutch-db-jdbc.el<br/>JDBC adapter and sidecar client"]
  end

  subgraph External["External protocol/runtime packages"]
    direction TB
    MySQLExt["mysql.el"]
    PGExt["pg-el"]
    SQLiteExt["Emacs sqlite-*"]
    MongoExt["mongodb.el"]
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
query-buffer behavior currently lives in `clutch-document.el`; it provides
`clutch-mongodb-mode` and reuses the shared query workflow for execution.
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

  subgraph Future["Unsupported / future contract"]
    Redis["Redis-style key/value systems<br/>need a separate contract"]
  end

  Registry --> MySQLCore
  Registry --> PGCore
  Registry --> SQLiteCore
  Registry --> OracleCore
  Registry --> SQLServerCore
  Registry --> GenericJDBC
  Registry --> MongoBackend

  MongoBackend --> MongoNative
  MongoBackend --> MongoSQL
  MongoNative --> MongoExt["mongodb.el native client"]
  MongoSQL --> JDBCPath["clutch-db-jdbc.el<br/>MongoDB JDBC driver"]

  Registry -. no claimed support .-> Redis
```

`mongodb` is one backend. Ordinary MongoDB uses the native document surface.
MongoDB SQL Interface is a `:surface sql-interface` path on the same backend,
with `:surface sql` accepted as a short alias.  It is not a second public
backend, driver, feature, or manual chooser entry.
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
  end

  subgraph LanguageHelpers["Language-specific buffer helpers"]
    SQLHelpers["clutch-sql.el<br/>SQL context/completion/Eldoc/xref"]
    DocumentHelpers["clutch-document.el<br/>MongoDB highlighting,<br/>indentation, completion, explain command"]
  end

  subgraph ObjectUI["Object workflow"]
    Jump["Object lookup / jump"]
    Describe["Describe object"]
    Browse["Browse object"]
    Actions["Backend-specific actions<br/>schema profile, index insight,<br/>stats, validation, explain"]
  end

  Facade["clutch-backend.el<br/>generic API"]

  QueryWorkflow["clutch-query.el<br/>query-at-point/region/buffer,<br/>execution and marking"]
  QueryAPI["clutch-db-query<br/>clutch-db-build-paged-sql"]
  DefinitionAPI["clutch-db-object-definition"]
  BrowseAPI["clutch-db-object-browse-query"]
  MetadataAPI["schema/list/column APIs"]
  DocumentMetadataAPI["document metadata APIs<br/>profile, index insight,<br/>validation, stats, explain"]

  Adapter["Backend adapter<br/>SQL, JDBC, or document"]
  ResultStruct["clutch-db-result"]
  ResultGrid["Result grid<br/>shared table renderer"]
  BrowseText["Backend-owned browse text<br/>SQL SELECT or document helper"]
  DescribeBuffer["Describe buffer<br/>DDL/source or JSON metadata"]

  SQLHelpers --> SQLConsole
  DocumentHelpers --> MongoConsole
  SQLConsole --> QueryWorkflow
  MongoConsole --> QueryWorkflow
  QueryWorkflow --> QueryAPI
  QueryAPI --> Facade
  Facade --> Adapter
  Adapter --> ResultStruct
  ResultStruct --> ResultGrid

  Jump --> MetadataAPI
  Describe --> DefinitionAPI
  Browse --> BrowseAPI
  Actions --> DocumentMetadataAPI
  MetadataAPI --> Facade
  DefinitionAPI --> Facade
  BrowseAPI --> Facade
  DocumentMetadataAPI --> Facade
  Adapter --> DescribeBuffer
  Adapter --> BrowseText
  BrowseText --> SQLConsole
  BrowseText --> MongoConsole
```

The result grid is shared across SQL and document query results. Object
definition and browse text are backend-owned so native document backends do not
fall back to table-oriented SQL behavior.

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
