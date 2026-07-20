# 106 - MongoDB Validation Action

## What changed

MongoDB collection object actions now include `Show validation`.  The action loads `getCollectionInfos({name: ...})` metadata through the native MongoDB adapter and displays:

- `validator`
- `validationAction`
- `validationLevel`

The action is read-only.  It does not create or edit validation rules.

## Why

MongoDB GUI clients treat collection validation as a first-class collection workflow because it describes document shape constraints that are not part of SQL table metadata.  Clutch should expose that document-specific signal, but it should not copy SQL staged-edit workflows into MongoDB just to look consistent.

The consistent part is the UI carrier:

- `C-c C-o` remains the object-action entry point
- the action opens a read-only metadata buffer
- Embark and Transient present the same action registry

The backend semantics remain MongoDB-specific.

## Boundary

The object layer calls the generic `clutch-db-collection-validation` contract. The MongoDB adapter implements that contract by translating public `mongodb.el` collection metadata into display JSON.  Clutch does not parse MongoDB connection strings, call `mongodb--*`, or implement protocol behavior.

Editing validation rules is deliberately deferred.  That would require a separate write workflow with confirmation, preview, and error handling rather than a read-only object metadata action.
