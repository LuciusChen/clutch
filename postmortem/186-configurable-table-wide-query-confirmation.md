# 186. Table-wide query confirmation is configurable

## Context

Clutch required typed `YES` for an `UPDATE` or `DELETE` without an effective `WHERE`, but treated `TRUNCATE` as an ordinary destructive query. An unbounded `DELETE` also passed through both the generic destructive prompt and the typed prompt, so the strongest guard produced two confirmations while `TRUNCATE` received only the weaker one.

## Decision

`TRUNCATE` now shares the table-wide high-risk classification with unbounded `UPDATE` and `DELETE`. The default `clutch-high-risk-query-confirmation` value, `typed`, requires the exact token `YES`; users may select `yes-or-no` or nil. A high-risk statement follows only that configured policy and does not also enter the generic destructive-query prompt.

## Why

The safety boundary is whether a statement can visibly target every row, not whether SQL classifies it as DML or DDL. One explicit confirmation is easier to understand than stacked prompts, while a public `defcustom` lets users choose the amount of friction appropriate for their environment without weakening the default.

This extends the effective-`WHERE` model recorded in postmortem 102 and supersedes the `TRUNCATE` confirmation tier described in postmortem 005.
