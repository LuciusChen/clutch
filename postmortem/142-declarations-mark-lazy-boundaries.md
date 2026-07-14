# 142 — Declarations Mark Lazy Boundaries

## Problem

Cross-module `declare-function` forms served two different roles.  Some were
real compile-time contracts for modules intentionally not loaded yet; others
repeated a contract already established by an earlier top-level `require` in
the same file.

The second kind added no loading or compiler guarantee.  It duplicated function
names and signatures, inflated the architecture metric, and obscured which
edges were genuinely lazy or cyclic.  Because declarations look harmless, the
duplication could keep growing even while dependency direction stayed stable.

## Decision

A Clutch declaration is redundant only when its source has already executed a
mandatory, top-level `require` for the owner.  The architecture check now walks
top-level forms in order and rejects that duplication.

The rule is deliberately conservative:

- nested requires remain lazy and do not cover declarations;
- optional `(require FEATURE nil t)` forms do not guarantee an owner loaded;
- quoted data and autoload metadata do not count;
- a require appearing after a declaration does not retroactively cover it;
- a require on the reverse side of a cycle does not cover the declaring source.

This removed 69 duplicate declarations without adding eager dependencies or
altering the query/result cycle.  The remaining declarations describe actual
composition-root, lazy adapter, optional integration, and cycle boundaries.

## Alternatives Rejected

Counting only declarations without deleting them would make the metric smaller
without removing the duplicated maintenance surface.  Deleting every
declaration whose owner appears anywhere in the dependency graph would break
independent compilation of lazy paths.  In particular, MongoDB loads its native
client and JDBC bridge inside runtime branches; removing those declarations
produces unknown-function byte-compile failures.  Eagerly requiring those
optional implementations would change startup and dependency behavior merely
to simplify a metric.

## Consequences

- Cross-module declarations fell from 134 to 65.
- The largest SCC remains two and root state remains 14.
- The guard has synthetic coverage for mandatory, optional, nested, quoted,
  late, autoload, and reverse-cycle cases.
- Full byte compilation continues to validate the real signatures supplied by
  mandatory owners, while declarations remain where loading is intentionally
  deferred.
