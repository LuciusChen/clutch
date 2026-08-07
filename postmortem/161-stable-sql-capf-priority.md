# 161 — Stable SQL CAPF Priority

## Context

Clutch installed identifier completion before the inherited CAPF boundary but appended keyword completion after it. A global fallback could therefore handle SQL text before the keyword CAPF, and reinstalling the CAPFs from `corfu-mode-hook` only compensated for one completion UI and load order.

## Decision

The SQL mode owns both CAPFs buffer-locally. It removes stale local entries, then prepends keyword and identifier completion in reverse order so dispatch is identifier, keyword, and finally inherited global CAPFs. Both Clutch CAPFs remain non-exclusive. This expresses priority through the standard CAPF hook contract and is tested through `completion-at-point`.

## Consequences

Clutch no longer needs a Corfu-specific hook. A caller may still deliberately prepend another buffer-local CAPF; that is normal hook composition rather than a load-order fallback.
