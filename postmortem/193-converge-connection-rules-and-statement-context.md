# 193 — Converge connection rules and statement context

## Findings

The project-wide redundancy audit found two rules with no clear single consumer boundary. The optional ob-clutch bridge duplicated saved password-source defaults and backend aliases. Its copy of the password rule predated `:profile-entry`, so it injected a connection-name `:pass-entry` beside a profile; core then correctly treated that injected field as an explicit override and discarded the profile secret.

SQL context caching retained both `:statement-tables` and a fallback `:tables` list covering the whole buffer. Production completion, Eldoc and xref consumed only statement tables and aliases. Only tests still called the wrapper reading `:tables`, but cache construction kept paying for the fallback scan whenever the current statement had no table. A cached, tested implementation had outlived its consumer.

## Decision

The separate ob-clutch repository now calls `clutch-saved-connection-params` and `clutch-backend-normalize` directly. Babel retains header coercion, language defaults, connection caching and Org result formatting. Saved backends still override language defaults; no implicit profile-only backend selection change is bundled with the credential fix.

Remove the unconsumed SQL cache field, whole-buffer scanner, its separate cache and the test-only query wrapper. Keep the statement cache and its schema identity, character tick, statement boundaries and buffer restriction checks. Completion candidates must remain unchanged, including when the current statement has not reached FROM.

Remove the PG adapter timeout-restoration method with no production caller and the JDBC driver-name identity wrapper. PG timeout behavior remains tested at the real connect adapter boundary, including explicit values overriding global defaults. JDBC installation hints retain their existing tests. No replacement compatibility shims or generic helper layers are introduced.

## Verification and test migration

The Babel profile-secret execution test and uppercase-symbol alias case failed before the change. The profile test drives the public Org executor with real Clutch preparation and substitutes only the password-store input and database execution boundary. It also covers explicit password/pass-entry precedence and saved-entry immutability. Existing SQLite cache identity coverage remains intact.

Delete two tests solely asserting the obsolete whole-buffer fallback. Migrate four statement-cache tests to the production table-context consumer, retaining cache-hit, edit, statement-switch, restriction and blank-line coverage. Extend the installed SQLite completion workflow with incomplete statements, both with and without a preceding statement. Do not infer test quality from the number of tests removed or added.

## Scope and remaining debt

This convergence deliberately leaves protocol parsing, transaction outcome state, JDBC primary/metadata session isolation, MongoDB authentication algorithm deduplication and JDBC request validation unchanged. The previous audit's cold-cache timing is evidence of avoidable work, not a universal performance guarantee.

The Babel bridge still owns legacy language-default behavior and contains older transport-header/documentation conventions. Those are separate compatibility decisions, not reasons to expand this password-source fix or add compensating fallbacks. Future boundary changes should be tested through public execution before changing defaults.
