# Direct Lake tuning checklist

Use this checklist when a Direct Lake model is slow or spikes capacity.

## Model
- [ ] Relationship keys are integers (BIGINT) on **both** sides, with no string or double keys
- [ ] Auto date/time is off (File → Options → Current file → Data load), and one date table is marked
- [ ] No calculated tables or columns that duplicate what Silver/Gold could provide
- [ ] High-cardinality columns that no visual uses (GUIDs, free text, timestamps to the second) are removed from the model
- [ ] Row-level business rules (business hours, include/exclude flags) are materialised as columns, not `FILTER(fact, …)` in measures

## Delta tables
- [ ] V-Order is enabled for tables the model reads
- [ ] Small files are compacted (`OPTIMIZE`) after incremental MERGE loads
- [ ] Row counts and column counts are within the capacity SKU's Direct Lake guardrails (otherwise the model falls back to DirectQuery)

## Report
- [ ] Each page has a reasonable number of visuals; heavy visuals sit behind a bookmark or drill-through
- [ ] Standing report filters are documented, or pushed into the data
- [ ] Field parameters and calculation groups are tested with Performance Analyzer

## Diagnose
1. Capacity Metrics app → find the spike's timepoint → split interactive from background.
2. Find the operation, user and start time → reproduce the page load.
3. Performance Analyzer → copy the slowest query → DAX Studio with Server Timings.
4. VertiPaq Analyzer → check temperature/residency and the largest columns.
