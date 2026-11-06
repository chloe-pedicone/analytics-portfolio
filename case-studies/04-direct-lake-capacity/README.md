# CS4 · Direct Lake semantic model capacity investigation

**Roles:** Power BI Developer · Fabric / Data Engineer
**Stack:** Fabric Capacity Metrics app · VertiPaq Analyzer (DAX Studio) · Direct Lake · Delta

> Anonymized case study. Capacity, workspace and model names are removed, and there are no screenshots.

## Situation
Interactive use of one semantic model drove the capacity to about **315% of base** in a 30-second window. Several heavy queries from one user started at the same moment (a page load), on top of XMLA reads that were still smoothing in.

## Diagnosis
| Evidence | Finding |
|---|---|
| Capacity Metrics → timepoint detail | Interactive `Query` operations from one model dominated. Background notebook load was separate. |
| VertiPaq Analyzer | Relationship columns on **string keys** were the hottest columns in the model |
| VertiPaq Analyzer | **Auto date/time** tables held about 38% of resident memory, despite a proper date dimension |
| Filter pane review | Hidden include/exclude columns narrowed slicers on every query |
| DAX review | Business-hours logic ran as a row-by-row `FILTER` over the fact table in several measures |

## Fixes
1. Converted fact and dimension join keys to BIGINT, on both sides in the same run.
2. Turned off auto date/time and kept one marked date table.
3. Materialised business hours in Silver (see [CS1](../01-contact-center-kpi-model/)).
4. Applied the include/exclude logic in Silver instead of as a standing report filter.

See [`direct-lake-tuning-checklist.md`](direct-lake-tuning-checklist.md).

## Result
- Peak interactive utilization for the same page load: `[fill in: ~315% → X%]`
- Resident model memory: `[fill in]`
