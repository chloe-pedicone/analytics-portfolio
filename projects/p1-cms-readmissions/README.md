# P1 · Hospital readmissions benchmarking (CMS HRRP)

**Roles:** Data Analyst · Power BI Developer
**Stack:** SQL (T-SQL) · Power BI (star schema, DAX ranking and percentiles, decomposition tree)
**Data:** [CMS Hospital Readmissions Reduction Program](https://data.cms.gov/provider-data/dataset/9n3s-kdb3) (public, hospital-level, no patient data)

**Live report:** `[link after Publish to web]` · **Demo GIF:** `[docs/assets/p1.gif]`

## Business questions
1. Which conditions drive excess readmissions nationally and in NJ / PA?
2. How does a given hospital rank within its state and nationally?
3. Which hospitals improved or worsened their excess readmission ratio year over year?

## Data model
```
DimHospital (FacilityID, FacilityName, State)        DimMeasure (MeasureKey, MeasureName, Condition)
            \                                          /
             ---------- FactReadmission (FiscalYear, FacilityID, MeasureKey,
                         Discharges, Readmissions, ExcessReadmissionRatio, …)
                                       |
                              DimFiscalYear (FiscalYear)
```

## Build steps
1. Download the current and archived HRRP CSVs. Load them into `stg.hrrp_raw` with a `fiscal_year` column.
2. Run [`sql/01_build_model.sql`](sql/01_build_model.sql). It builds the dimensions and the fact, cleans "Too Few to Report" values to NULL, and runs the DQ checks.
3. Import into Power BI. Add the measures from [`dax/measures.dax`](dax/measures.dax).
4. Pages: **Overview** (KPI cards, condition bar chart, state map) · **Hospital profile** (rank, percentile, trend) · **Drivers** (decomposition tree).

## Key findings
`[fill in after building: 3 bullet insights with numbers]`

## Notes and caveats
- An excess readmission ratio above 1 means more readmissions than CMS expected for that hospital's case mix.
- Small hospitals often show "Too Few to Report". These are excluded from averages, not treated as zero.
