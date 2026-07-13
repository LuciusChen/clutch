# 131 — MongoDB Helper Surface Convergence

## Context

The native MongoDB console had grown from a small document-query adapter into a
partial MongoDB Shell compatibility layer.  It accepted dedicated helpers for
database administration, collection and index creation/deletion, database-level
aggregation, multi-document mutation, cursor tuning, and nine BSON constructor
spellings.  Documentation and tests described that surface, but those records
showed compatibility cost rather than product value.

Clutch's actual document workflows are narrower:

- browse, profile, and explain use `find`, `findOne`, and `aggregate`;
- result copy/export generates `insertOne`, `insertMany`, `updateOne`,
  `replaceOne`, and `deleteOne` snippets;
- `countDocuments` and `distinct` are common read operations;
- `runCommand` provides a direct public-API escape hatch for uncommon commands;
- `ObjectId` and `ISODate` cover the common non-JSON query literals.

Collection metadata, indexes, and storage statistics already use public
`mongodb.el` APIs behind Clutch object actions.  Database selection already has
the shared `clutch-switch-schema` workflow.  Re-exposing those operations as a
second shell-shaped surface duplicated product ownership.

## Decision

Keep one intentionally small helper surface:

- database: `runCommand`;
- reads: `find`, `findOne`, `aggregate`, `countDocuments`, and `distinct`;
- generated mutations: `insertOne`, `insertMany`, `updateOne`, `replaceOne`,
  and `deleteOne`;
- `find` chains: `sort`, `skip`, `limit`, `maxTimeMS`, `allowDiskUse`, and
  `explain`;
- `aggregate` chains: `maxTimeMS`, `allowDiskUse`, and `explain`;
- constructors: `ObjectId` and `ISODate`.

Remove dedicated admin/index helpers, `getSiblingDB`, database aggregation,
multi-document update/delete, cursor `batchSize`/`comment`, and BSON numeric or
timestamp constructor emulation.  Users switch databases through Clutch.  An
uncommon MongoDB command can be sent through `db.runCommand(...)` instead of
growing another dedicated helper branch.

Clutch metadata code calls public `mongodb.el` functions directly; it does not
round-trip internal operations through query-console text.  The parser remains
inside the adapter because it translates the retained console syntax, but it is
not extracted into a parser framework or another file.

## Consequences

This is a pre-1.0 breaking cleanup for users of removed helper spellings.  It
reduces parser dispatch, completion, tests, required `mongodb.el` APIs, and
documentation together.  Unsupported shell helpers fail explicitly.
Chains and document arguments are validated before a client API is called, so
unsupported syntax cannot be accepted and then silently ignored.

The stopping rule is workflow-based: new dedicated helpers require a Clutch
workflow that cannot be expressed clearly with the retained surface or a public
`mongodb.el` call.  Similarity to `mongosh` alone is not sufficient.
