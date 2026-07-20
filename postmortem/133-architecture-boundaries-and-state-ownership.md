# 133 — Architecture Boundaries and State Ownership

## Context

After diagnostics extraction, Clutch still relied on a dense workflow cycle, display-label cache identity, composition-root load order, root-owned state with hidden consumers, and many declarations that pretended mandatory dependencies were lazy. Tests often reproduced those implementation seams with private-function mocks, so the architecture and test suite reinforced each other's glue.

Treating each edge or variable as an independent cleanup produced useful local changes but also produced too many near-identical design notes. This record captures the decisions and stopping criteria for the complete boundary refactor; the commit history preserves the mechanical sequence.

## Decisions

### Identity and lifecycle

Connection-scoped schema, object, warmup, and async metadata state is keyed by the live connection object in `eq` stores. Display labels remain presentation only, so two sessions with the same endpoint text cannot share lifecycle state or refresh each other's buffers.

Schema is a metadata producer. Connection owns lifecycle reactions and attached-buffer refresh; object owns object-cache reactions. Existing hooks and the standard buffer revert protocol carry those transitions without a new cache framework or event bus.

### One-way composition and truthful dependencies

`clutch.el` is a one-way composition root: public autoloads enter through it, but implementation modules never require it from below. Query modes and commands live with the query workflow, while schema switching and live connection context live with connection.

Stable mandatory relationships use top-level `require` forms. `declare-function` and `defvar` declarations remain only for genuine lazy, optional, standalone-load, or cyclic boundaries. The architecture reader verifies dependency direction, generated autoload targets, declaration use, direct module loading, and the absence of root re-entry.

Two direct cycles remain deliberately: query/result and backend/JDBC. Replacing them with registries or callback plumbing would add more ownership ambiguity than it removes.

### State ownership

Mutable state and public options live with the workflow that owns their lifecycle: connection owns saved connection policy and attached connection context; query, result, schema, SQL, edit, object, UI, and diagnostics own their workflow and buffer state; backend owns shared timeout policy.

`clutch-result-max-rows` remains the composition root's one mutable contract because it coordinates query lookahead, result trimming and export, and UI row positions. Creating a configuration module for one genuinely shared value would be metric-driven glue.

Diagnostics stores source-buffer provenance with each problem and derives redacted endpoint labels through the backend contract. It has no callback registry, reverse dependency on connection, or silent catch around internal backend access.

### Rendering and policy boundaries

Connection projects semantic state into attached buffers; UI formats that state and does not pull connection policy through private accessors. Spinner state remains separate from stable connection state so animation does not rebuild unrelated chrome.

Connection no longer calls query or object presenters. Console naming uses a pure renderer, and describe buffers refresh through `revert-buffer`. Edit-driven refresh uses the same protocol, keeping connection and edit out of presentation ownership.

Object resolution and Eldoc metadata eligibility were separated into small pure decisions because each had multiple real branches. No general context object, provider layer, or policy framework was added.

### Test budget

Tests moved toward public dispatch, real metadata, table-driven policy matrices, and focused effect boundaries. Repeated reconnect and completion harnesses were consolidated; mocks that only recreated private parser, cache, or presenter internals were removed.

The goal was not fewer tests by count. The rule is that a test must fail when its public workflow or architecture invariant breaks, and a new production abstraction is not justified merely to make mocking easier.

## Outcome

- Cross-module declarations fell from 168 to 17, with zero stale declarations.
- The largest strongly connected component fell from nine modules to two.
- Once root-state ownership became a checked metric, composition-root mutable definitions fell from 35 to one.
- UI and diagnostics are no longer members of workflow dependency cycles, and implementation dependencies into `clutch.el` are rejected.
- Connection identity now isolates same-label sessions, while transaction, disconnect, schema, and describe refreshes update the right attached views.
- Completion and reconnect coverage exercises real dispatch with substantially less repeated mocking.

## Deliberate limits

Large modules were not split solely to reduce line counts. Distinct metadata stores were not collapsed into a state object, direct stable dependencies were not hidden behind accessors, and the two remaining two-module cycles were not replaced by framework glue.

Future architecture work should start from a concrete ownership or behavior defect. The checked declaration, SCC, root-state, and dependency-direction budgets are ceilings, not targets that justify abstraction by themselves.
