# 104 - Backend query-mode facet

## What changed

Clutch now lets backend registry metadata choose a query-console major mode.
The generic console asks the selected backend for its query mode instead of
checking for MongoDB directly.

MongoDB's native query buffer UI moved into `clutch-document.el`, the document
query-console layer.  The adapter in `clutch-mongodb.el` remains focused on
translating Clutch database calls to public `mongodb-` APIs.  SQL completion
stays in `clutch-sql.el`.

## Why

The previous MongoDB branch added direct MongoDB checks to the generic query
console and mixed MongoDB completion into `clutch-sql.el`.  That worked for one
document database, but it would not scale to Redis or other non-SQL backends.
The generic console should not know whether a backend is SQL, document, or
key-value.  It should only ask the backend registry which console mode applies.

We considered a top-level query-surface layer, but that would add a broad
platform abstraction before multiple surfaces need to share behavior.  A
backend-owned facet is smaller: each backend registers the mode it needs, while
shared SQL behavior remains a reusable SQL module.

## Current boundary

- `clutch-backend.el` owns backend metadata and generic dispatch.
- `clutch-query.el` owns the generic console workflow and query-buffer local
  state.
- `clutch-sql.el` owns SQL editing/completion/eldoc/xref.
- `clutch-mongodb.el` owns MongoDB connection, execution, and result
  normalization through public `mongodb-` APIs.
- `clutch-document.el` owns MongoDB query-buffer syntax,
  indentation, and completion.

## Deferred work

The existing schema/object/edit APIs still use relational names such as table
and column.  MongoDB maps collections and sampled fields through those APIs for
basic support.  A future Redis or broader document-database change should
introduce a neutral catalog model before expanding object workflows further.
