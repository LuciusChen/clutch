# 118 -- First Query Metadata Scheduling

## Problem

Native MySQL and PostgreSQL schema refresh uses foreground protocol connections
and runs from Emacs timers.  After opening a query buffer, Clutch queued schema
refresh with `run-with-idle-timer 0`.  In an interactive Emacs session this can
run immediately after connection activation and before the user's first query.

That made the first query feel slow even for tiny or empty tables, because
background metadata could occupy the same connection and main thread before
foreground execution started.

## Decision

Automatic schema refresh after connect is low-priority background work.  It now
waits for a small wall-clock delay before entering the idle queue.  Manual schema
refresh and completion-triggered table metadata keep their existing behavior:
they still start immediately because they are direct user or editor requests.

Row identity candidates now also stop after the first usable locator.  Clutch's
SELECT path uses only the first candidate, so scanning lower-priority unique
indexes or row locators after finding a primary key spent metadata work that the
current result rendering could not use.

## Why Not Only Idle Timer Delay

`run-with-idle-timer` measures Emacs idle time.  If Emacs is already idle when a
timer is scheduled, a non-zero idle delay can still fire immediately.  A
wall-clock timer followed by an idle timer gives foreground commands a real
first-query window while still running metadata only when Emacs is idle.

## Consequence

Backend metadata refresh remains automatic, but it no longer competes with the
first foreground query immediately after connection activation.  If future
metadata warmups are added, they should choose explicitly between foreground
priority, immediate idle work, and delayed background work instead of defaulting
to `run-with-idle-timer 0`.
