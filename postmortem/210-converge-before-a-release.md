# 210 — Converge Before a Release

## Evidence

The review before 0.5.5 covered the changes since the last cleanup, from 1f44fba to 434d42d: 113 commits that added 2,567 production lines and removed 1,080. Each change had been tested and reviewed on its own, yet three kinds of fault had built up between them.

- A mechanism copied and then fixed in one copy only. The export page loop (#88) copied the statement batch's continuation, and d87c9b7 widened the export's handlers to catch a quit. The batch's copy still caught errors only, so a quit there left the connection reserved and the elapsed time counting. The FINAL/NEW/OLD TABLE pattern likewise kept its own whitespace class after cb872cc introduced `clutch-db-sql--keyword-gap-regexp` for the temporal and FETCH patterns, so a comment between the keywords hid an INSERT that each page then reran.
- A guard or record that no longer protected anything. After postmortem 202 sent every JDBC reply through the request registry, nothing read `clutch-jdbc--ignored-response-ids`, though six places still wrote to it. `clutch-db-with-foreground-connection` lost its last caller when paging and counting became query activities.
- A behavior lost in a rewrite. Moving statements off the foreground (0.5.2) made the batch capture its connection once, so after an idle reconnect its later statements ran on the closed connection.

The same review also found that a declaration needed on an existing boundary had to be weighed against the cross-module declaration baseline in `test/check-architecture.el`, which had only ever fallen.

## Decision

- Keep copies of one mechanism in step. A fix to one copy reaches the others, or its commit says why it does not. Two copies stay separate; the shared part is extracted when a third would appear.
- Raise the declaration baseline only for a declaration that compilation needs on a boundary that already exists, with the reason in the commit message, and lower it when declarations go away. #94 raised it from 17 to 18 for `C-c C-z`, the eighth declaration on the query-to-result boundary.
- Before a release, review the range since the previous release tag for guards that no longer protect anything, copies that have drifted apart and code without callers, and converge them in commits of their own. Bugs found this way are fixed first, each with a failing test.

## Consequence

The 0.5.5 review produced eight fixes in one pull request, and a separate convergence that removed 57 production lines and 19 test lines net. One removal there, a trim that the status marker seemed to repeat, moved where a running statement's markers sit; review caught it where the gates could not, and it was restored with a test. AGENTS.md carries the three rules.
