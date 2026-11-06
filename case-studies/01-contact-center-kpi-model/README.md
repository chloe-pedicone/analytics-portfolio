# CS1 · Contact-center Silver layer and KPI model

**Roles:** Power BI Developer · Data Analyst · Analytics Engineer · Fabric / Data Engineer
**Stack:** Microsoft Fabric (PySpark, Delta, Lakehouse) · Power BI (Direct Lake, calculation groups, field parameters)

> Anonymized case study. No data, screenshots or code from my employer. Code in this folder is a from-scratch rewrite of the patterns, with generic names.

## Situation
Contact-center call data from Cisco UCCE (6 fact tables and 2 dimensions) feeds a KPI report used by operations leaders. Business-hours rules differ by department: 24/7 teams, 08:00–17:00 teams and 07:00–19:00 teams. Those rules lived inside DAX `FILTER` expressions repeated across measures.

## Task
- Load Bronze → Silver incrementally and reliably.
- Make the KPI model fast and consistent, so every measure applies the same business-hours rule.

## Action
| Layer | What I built | File |
|---|---|---|
| Silver loads | Config-driven watermark + lookback Delta MERGE per fact table | [`incremental_merge.py`](../../shared/fabric_utils/incremental_merge.py) |
| Dimensions | Hash-based SCD Type 1 loads, so corrected attributes overwrite in place | [`scd1.py`](../../shared/fabric_utils/scd1.py) |
| Business rule | Moved business hours from DAX to a materialised `HourCategory` column; rules stored as data | [`hour_category.py`](hour_category.py) |
| Join keys | Converted fact ↔ dimension keys from string/double to BIGINT on both sides | — |
| Semantic model | Business-day-aware averages, YoY service level, KPI and date-grain field parameters, a Period calculation group | [`measures.dax`](measures.dax) |

## Result
- Query duration on the main KPI page: `[fill in: before → after]`
- Measures re-implementing the business-hours rule: `[fill in: N → 0]`
- New departments are now a row in a rules table, not a DAX edit.

## What I'd do next
Add data-quality checks (interval completeness per queue per day) and alert on gaps before the report refreshes.
