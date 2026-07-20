# 132 — Generic JDBC Oracle Validation

## Context

A generic `:backend jdbc` connection can load `oracle.jdbc.OracleDriver` and connect to a `jdbc:oracle:` URL, but the driver class only selects the Java driver.  Clutch still owns a generic JDBC connection and therefore applies the generic pagination dialect.  The first result query can then fail with an Oracle syntax error even though connection setup succeeded.

## Decision

Reject `jdbc:oracle:` URLs when the selected backend is generic JDBC and tell the user to configure `:backend oracle`.  Validate before driver setup or the sidecar connection RPC.  Do not infer or replace the backend from the URL.

## Why

Oracle already has a first-class backend that owns its pagination, metadata, row-identity, and transaction behavior.  Inferring that backend from a URL would hide invalid configuration and leave the explicit connection identity in conflict with runtime behavior.  A broad URL-to-backend inference registry would add another dispatch model without solving a current need.

## Consequence

The invalid configuration now fails at connection time with an actionable message.  Correct Oracle and generic JDBC configurations are unchanged.
