# CS3 · Tableau Prep → PySpark migration (referral-to-appointment matching)

**Roles:** Analytics Engineer · Fabric / Data Engineer
**Stack:** Tableau Prep · PySpark on Microsoft Fabric · Delta

> Anonymized case study. This is generic code written from scratch; no source data or employer code is included.

## Situation
A 79-node Tableau Prep flow produced a network-integrity dataset that linked referrals to the appointments that fulfilled them. It had to move to Fabric, so it could run on the same Lakehouse and schedule as the rest of the platform.

## Action
1. **Mapped the DAG node by node:** sources, cleans, joins, unions, aggregates and outputs, each to a PySpark step.
2. **Reconciled against the Prep output.** That surfaced gaps in source filters, join order and specialty normalization.
3. **Rebuilt the fuzzy-match branch.** It links un-encountered referrals to unlinked appointments within a time window. I found and fixed four bugs: [`time_window_match.py`](time_window_match.py)

| # | Bug | Symptom | Fix |
|---|---|---|---|
| 1 | End-date `COALESCE` arguments reversed | Windows too long or too short | Explicit end date first, expiration as fallback |
| 2 | No fallback when both end dates are null | Referrals silently dropped | `StartDate + N days` default |
| 3 | Ranked before filtering to unlinked appointments | Valid matches lost | Filter first, then rank |
| 4 | Appointment side joined to an undeduplicated table | Duplicate matches | Deduplicate on `AppointmentID` before joining |

## Result
- Row-level reconciliation with the Tableau Prep output: `[fill in: e.g. 99.x% of keys match; remaining differences explained]`
- Runs on the same Fabric schedule as the upstream tables, so no separate Tableau server job.

## Interview angle
How I validate a migration: row counts per step, key-level anti-joins in both directions, and a diff of aggregates per category.
