# Analytics Portfolio · Chloe [Last Name]

**Power BI Developer · Data Analyst · Analytics Engineer · Microsoft Fabric**

I build end-to-end analytics on Microsoft Fabric: incremental PySpark/Delta pipelines from a healthcare data warehouse up to fast, trusted Power BI models. Before analytics engineering I spent four years in retail merchandising, so I start from the business question.

🌐 **Website:** `[https://<your-github-username>.github.io/analytics-portfolio/]` · 💼 **LinkedIn:** `[link]` · ✉️ `[email]`

---

## Public-data projects (live demos)
| Project | What it shows | Stack |
|---|---|---|
| [P2 · NYC 311 service performance](projects/p2-nyc311-service-performance/) | Incremental API ingestion, business-hours classification, SLA and YoY KPIs, field parameters | PySpark · Delta · Power BI |
| [P1 · Hospital readmissions benchmarking](projects/p1-cms-readmissions/) | SQL cleaning, star schema, ranking and percentile DAX on CMS data | T-SQL · Power BI |
| [P3 · Synthetic patient medallion lakehouse](projects/p3-synthea-medallion/) | Bronze/Silver/Gold, data-quality gate, unit-tested 30-day readmission logic | PySpark · pytest · Power BI |

## Case studies from my work (anonymized)
| Case study | Problem → outcome |
|---|---|
| [CS1 · Contact-center Silver layer and KPI model](case-studies/01-contact-center-kpi-model/) | Business-hours rules duplicated in DAX → one materialised column, faster measures |
| [CS2 · Surgical pipeline refactor](case-studies/02-surgical-pipeline-refactor/) | 3 notebooks repeating the same cleaning → shared spine plus a parallel DAG and incremental loads |
| [CS3 · Tableau Prep → PySpark migration](case-studies/03-tableau-prep-migration/) | 79-node flow ported; 4 matching bugs found and fixed |
| [CS4 · Direct Lake capacity investigation](case-studies/04-direct-lake-capacity/) | ~315% capacity spike diagnosed → key, date-table and model fixes |

## Reusable code
- [`shared/fabric_utils`](shared/fabric_utils/): watermark + lookback Delta MERGE, hash-based SCD1 loads, a `runMultiple` DAG builder with cycle checks
- [`tests/`](tests/): pytest suite (`python -m pytest -q tests projects/p3-synthea-medallion/tests`)

## Skills
**Power BI:** star schema, DAX (time intelligence, calculation groups, field parameters), Direct Lake, Performance Analyzer, DAX Studio / VertiPaq Analyzer
**Engineering:** Microsoft Fabric (Lakehouse, notebooks, pipelines, variable libraries), PySpark, Delta Lake, SQL (T-SQL), Python
**Domain:** healthcare operations (Epic Caboodle, contact center, surgical services), retail merchandising
**Education:** MS Business Analytics, Rutgers University (4.0 GPA)
