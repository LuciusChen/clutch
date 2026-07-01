# MongoDB High Value Document Workflows

## Context

Native MongoDB support was useful once query execution and basic object actions
worked, but it still forced users to remember too much raw helper syntax and
left collection metadata weaker than the SQL object/result workflow.

The tempting directions were to add a MongoDB-specific query builder, depend on
snippet packages, or move more behavior into the object UI. Those approaches
would make MongoDB feel like a separate product inside Clutch and would grow UI
code around one backend before a second document database proves a shared
contract.

## Decision

Keep MongoDB as a basic document backend with five targeted workflow upgrades:

- collection schema profile from sampled documents;
- current query explain for `find`, `findOne`, and `aggregate`;
- index insight combining `listIndexes` with `$indexStats`;
- completion-first query and aggregation templates;
- better document result viewing by marking mixed nested values as JSON.

The backend adapter owns MongoDB-specific metadata through generic facade
methods: `clutch-db-collection-profile`,
`clutch-db-collection-index-insight`, and `clutch-db-explain-query`. The object
UI calls those generic methods and renders JSON metadata, but it does not know
how to build MongoDB commands.

The query console stays buffer-first. Completion offers common helper templates
and sampled field paths, while users still edit normal MongoDB helper/MQL text.
No transient query builder, yasnippet dependency, tempel dependency, or arbitrary
JavaScript runtime was added.

## Code Budget

Two bloat risks were found and reduced during implementation.

First, JSON metadata display was duplicated between object describe buffers and
MongoDB explain buffers. That was moved to `clutch-ui.el` so object and document
workflows share the same JSON mode selection and pretty-print path.

Second, collection profiling originally walked sampled documents twice and used
a one-use wrapper for scalar checks. The profile walker now records path order
and field statistics in one pass, and array fields keep their own `array` type
while nested object fields still expand to paths such as `items.sku`.

Known debt remains outside this MongoDB workflow: the staged JSON edit
sub-editor still contains older recovery-oriented initial-text formatting code.
That is not part of native MongoDB query/profile/explain/index workflows and
should be handled in a separate edit-buffer cleanup.

## Consequences

MongoDB gets the high-value inspection workflows without turning Clutch into
`mongosh` or a backend-specific GUI builder. Future document backends can add
their own collection profile, index insight, and explain behavior behind the
same facade methods if the concepts fit.

The shared result grid remains the display surface for document query output.
Document-specific UX is limited to query-buffer syntax/completion and JSON
metadata buffers, which keeps the relational SQL, document database, and JDBC
surface structure readable.
