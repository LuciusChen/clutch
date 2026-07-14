# 149 — Timeout Policy Belongs to the Backend Contract

## Problem

The composition root defined four public timeout options even though it never
applied them.  Connection setup and the PostgreSQL, MySQL, and JDBC adapters
consumed the values, then repeated ten `defvar` fallbacks to support their own
load order.  The root therefore owned backend policy while every actual owner
carried duplicate defaults.

Splitting the options among adapters would make their Customize visibility
depend on which optional backend had been loaded.  In particular, the JDBC RPC
timeout is public even before the lazy JDBC implementation is needed.

## Decision

Define connection, read-idle, query, and JDBC RPC timeouts together in
`clutch-backend.el`.  It is the lowest mandatory dependency shared by every
consumer and is loaded by the public package entry point, so the existing
Customize contract remains visible without loading optional protocol code.

Delete the ten adapter and connection fallbacks.  Keep the MySQL cancel timeout
and the JDBC cancel and disconnect timeouts in their adapters: those values
govern private recovery boundaries rather than the public cross-backend
connection contract.

## Consequences

- Mutable state in the composition root falls from six definitions to two.
- Four real definitions replace fourteen definitions and fallbacks.
- Timeout defaults and dynamic test bindings retain the same values and special
  variable behavior under direct backend loads.
- Cross-module declarations remain 43 and the largest dependency SCC remains
  two.
