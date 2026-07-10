# Schema-Qualified Row Identity Must Keep Its Namespace

## Background

Clutch reduced a simple SQL source to its bare table name before asking a
backend for row identity metadata.  That was sufficient for current-schema
queries, but it broke the identity invariant for a source such as
`APP.REPORTS`: JDBC metadata could inspect `CURRENT_SCHEMA.REPORTS`, and staged
UPDATE or DELETE SQL could target that current-schema table as well.

The same identity path also offered Oracle `ROWID` whenever a name appeared in
table discovery.  Oracle dictionary relations such as `ALL_TABLES` are views,
so injecting `ROWID` into their SELECT list produced ORA-01445.

## Decision

The query preparation path preserves both the parser-validated source token and
its schema.  JDBC row identity lookup uses a metadata-only copy of the
connection parameters with that schema, so primary keys, unique indexes, and
Oracle base-table checks all use one namespace without changing the live
session.  Final row identity metadata retains the source token, and staged
UPDATE and DELETE builders use it as the target.

Oracle classifies the source before any identity metadata lookup.  Primary-key,
column, index, and `ROWID` probes run only when discovery confirms an entry
whose name, schema, source schema, and type identify a base table in that same
metadata scope.  Views and synonyms therefore remain read-only.

## Rationale

Row identity is not just a set of column names; it belongs to one concrete
relation.  Keeping the namespace beside the identity makes metadata discovery,
hidden-column injection, and mutation target selection agree.  A copied JDBC
connection value is sufficient because metadata requests already derive their
schema filters from connection parameters; no session mutation or protocol
operation is needed.

The original source token comes from Clutch's SQL parser rather than user text
concatenation, so retaining it preserves valid quoting while keeping mutation
SQL within the parser's single-table safety boundary.

## Alternatives Considered

- Blacklist Oracle dictionary view names.  This would only cover known views
  and would fail for user views and synonyms.
- Retry without `ROWID` after ORA-01445.  This would hide an invalid identity
  decision behind an extra failed query and leave staged mutation targeting
  unresolved.
- Change the Oracle session schema before metadata lookup.  This would mutate
  shared connection state for a read-only metadata question and could affect a
  concurrent foreground query.
- Add schema-specific JDBC agent operations.  Existing metadata requests
  already accept schema filters, so new protocol surface would duplicate the
  established contract.

## Resulting Invariant

For a simple schema-qualified JDBC SELECT, identity metadata, any injected
locator, and staged UPDATE or DELETE SQL all refer to the same relation.  Oracle
identity metadata is never requested for a view or synonym, and ROWID is never
inferred from a same-named table in another schema.
