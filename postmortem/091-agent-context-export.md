# External Agent Context Export

## Problem

The earlier AI direction tried to put a chat workflow inside clutch.  That did not match the actual workflow: the user already talks to ChatGPT, Claude, or DeepSeek and only needs clutch to make database context easy to hand over.  A clutch-owned chat buffer would duplicate assistant UI, configuration, and provider handling without solving the repetitive manual copying of SQL, schema metadata, and sample result rows.

The useful boundary is not "clutch talks to an LLM".  It is "clutch can package the current database context so an external agent can reason about it".

## Decision

`clutch-copy-context-for-agent` copies Markdown to the kill ring.  It includes:

- connection/backend/schema context
- the current SQL region or statement in `clutch-mode`
- the effective result query in `clutch-result-mode`
- referenced table metadata from the existing object describe path: comments, primary keys, columns, indexes, and any foreign keys or reverse references present in metadata
- a bounded sample of rows already present in the current result buffer, or in the latest result buffer produced by the query console when the SQL matches; hidden row identity columns are omitted

The command is available through `M-x` and as `k` in the main and result transients.

## Why This Layer

This belongs in `clutch.el` because it is a UI handoff workflow.  It composes existing backend metadata APIs and already-rendered result-buffer state; it does not add a new backend protocol operation, provider abstraction, chat mode, or optional LLM dependency.

The command does not execute the SQL being copied.  It only reads metadata and uses result rows already present in a matching result buffer, which keeps the first slice safe and predictable.

## Rejected Options

### Embed a Chat UI

Rejected because clutch would become responsible for prompt history, provider configuration, auth-source handling for LLMs, streaming, formatting, retries, and tool execution.  Those are already owned better by external assistants.

### Register gptel Tools

Rejected for this first slice because it still routes the experience through a gptel-managed buffer and does not help users who work primarily in ChatGPT, Claude, or DeepSeek.  gptel integration can be reconsidered later if the copied context format proves insufficient.

### Add Backend-Specific Agent APIs

Rejected because the current metadata contract already exposes the information needed for a useful handoff.  Adding protocol operations before the workflow is validated would make the first slice harder to test and maintain.

## Guardrails

Metadata failures are written into the copied Markdown instead of being hidden, so the external agent and user can see which context is missing.  Result samples are bounded by `clutch-agent-context-max-result-rows`, and individual cell text is bounded by `clutch-agent-context-max-cell-width`.  Table metadata is rendered through the existing object describe path instead of a second formatter.
