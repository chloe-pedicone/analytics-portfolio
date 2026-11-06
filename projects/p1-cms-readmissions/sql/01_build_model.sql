/* ===========================================================================
   P1 · CMS Hospital Readmissions Reduction Program (HRRP) -> star schema
   Dialect: T-SQL (Fabric Warehouse / SQL Server). Works in Azure SQL too.

   Load each fiscal year's CSV from
   https://data.cms.gov/provider-data/dataset/9n3s-kdb3 (current) and the
   Hospital Data Archive (prior years) into stg.hrrp_raw, all columns
   VARCHAR, plus a fiscal_year column you set per file.

   Column names below follow the CSV header (spaces -> underscores, lower
   case). CHECK them against the file you download: CMS occasionally renames
   columns between releases.
   =========================================================================== */

-- Staging table (all text; cleaning happens below)
-- CREATE TABLE stg.hrrp_raw (
--     fiscal_year                 INT,
--     facility_name               VARCHAR(200),
--     facility_id                 VARCHAR(10),
--     state                       VARCHAR(2),
--     measure_name                VARCHAR(50),
--     number_of_discharges        VARCHAR(20),
--     footnote                    VARCHAR(50),
--     excess_readmission_ratio    VARCHAR(20),
--     predicted_readmission_rate  VARCHAR(20),
--     expected_readmission_rate   VARCHAR(20),
--     number_of_readmissions      VARCHAR(20),
--     start_date                  VARCHAR(20),
--     end_date                    VARCHAR(20)
-- );

/* ---------- Dimension: measure (condition) -------------------------------- */
DROP TABLE IF EXISTS dbo.DimMeasure;
CREATE TABLE dbo.DimMeasure (
    MeasureKey   INT         NOT NULL,
    MeasureName  VARCHAR(50) NOT NULL,
    Condition    VARCHAR(40) NOT NULL
);
INSERT INTO dbo.DimMeasure (MeasureKey, MeasureName, Condition)
SELECT ROW_NUMBER() OVER (ORDER BY m.measure_name), m.measure_name,
       CASE
           WHEN m.measure_name LIKE '%-AMI-%'      THEN 'Heart attack (AMI)'
           WHEN m.measure_name LIKE '%-HF-%'       THEN 'Heart failure'
           WHEN m.measure_name LIKE '%-PN-%'       THEN 'Pneumonia'
           WHEN m.measure_name LIKE '%-COPD-%'     THEN 'COPD'
           WHEN m.measure_name LIKE '%-CABG-%'     THEN 'CABG surgery'
           WHEN m.measure_name LIKE '%-HIP-KNEE-%' THEN 'Hip/knee replacement'
           ELSE 'Other'
       END
FROM (SELECT DISTINCT LTRIM(RTRIM(measure_name)) AS measure_name
      FROM stg.hrrp_raw WHERE measure_name IS NOT NULL) AS m;

/* ---------- Dimension: hospital (latest name/state per facility) ---------- */
DROP TABLE IF EXISTS dbo.DimHospital;
CREATE TABLE dbo.DimHospital (
    FacilityID    VARCHAR(10)  NOT NULL,
    FacilityName  VARCHAR(200) NOT NULL,
    State         VARCHAR(2)   NOT NULL
);
INSERT INTO dbo.DimHospital (FacilityID, FacilityName, State)
SELECT facility_id, facility_name, state
FROM (
    SELECT LTRIM(RTRIM(facility_id))   AS facility_id,
           LTRIM(RTRIM(facility_name)) AS facility_name,
           UPPER(LTRIM(RTRIM(state)))  AS state,
           ROW_NUMBER() OVER (PARTITION BY LTRIM(RTRIM(facility_id)) ORDER BY fiscal_year DESC) AS rn
    FROM stg.hrrp_raw
    WHERE facility_id IS NOT NULL
) AS h
WHERE rn = 1;

/* ---------- Fact --------------------------------------------------------- */
-- 'N/A', 'Too Few to Report' and blanks become NULL via TRY_CAST.
-- Dates in the CSV are MM/DD/YYYY -> style 101.
DROP TABLE IF EXISTS dbo.FactReadmission;
CREATE TABLE dbo.FactReadmission (
    FiscalYear               INT           NOT NULL,
    FacilityID               VARCHAR(10)   NOT NULL,
    MeasureKey               INT           NOT NULL,
    Discharges               INT           NULL,
    Readmissions             INT           NULL,
    ExcessReadmissionRatio   DECIMAL(9, 4) NULL,
    PredictedReadmissionRate DECIMAL(9, 4) NULL,
    ExpectedReadmissionRate  DECIMAL(9, 4) NULL,
    PeriodStart              DATE          NULL,
    PeriodEnd                DATE          NULL,
    IsWorseThanExpected      BIT           NULL   -- ratio > 1
);
INSERT INTO dbo.FactReadmission
SELECT r.fiscal_year,
       LTRIM(RTRIM(r.facility_id)),
       m.MeasureKey,
       TRY_CAST(r.number_of_discharges       AS INT),
       TRY_CAST(r.number_of_readmissions     AS INT),
       TRY_CAST(r.excess_readmission_ratio   AS DECIMAL(9, 4)),
       TRY_CAST(r.predicted_readmission_rate AS DECIMAL(9, 4)),
       TRY_CAST(r.expected_readmission_rate  AS DECIMAL(9, 4)),
       TRY_CONVERT(DATE, r.start_date, 101),
       TRY_CONVERT(DATE, r.end_date,   101),
       CASE
           WHEN TRY_CAST(r.excess_readmission_ratio AS DECIMAL(9, 4)) IS NULL THEN NULL
           WHEN TRY_CAST(r.excess_readmission_ratio AS DECIMAL(9, 4)) > 1     THEN 1
           ELSE 0
       END
FROM stg.hrrp_raw AS r
JOIN dbo.DimMeasure AS m
  ON m.MeasureName = LTRIM(RTRIM(r.measure_name))
WHERE r.facility_id IS NOT NULL;

/* ---------- Data-quality checks (expect 0 rows each) ---------------------- */
-- 1. Duplicate grain
SELECT FiscalYear, FacilityID, MeasureKey, COUNT(*) AS n
FROM dbo.FactReadmission
GROUP BY FiscalYear, FacilityID, MeasureKey
HAVING COUNT(*) > 1;

-- 2. Orphan facilities
SELECT DISTINCT f.FacilityID
FROM dbo.FactReadmission AS f
LEFT JOIN dbo.DimHospital AS h ON h.FacilityID = f.FacilityID
WHERE h.FacilityID IS NULL;

-- 3. Implausible ratios (investigate, don't delete)
SELECT * FROM dbo.FactReadmission
WHERE ExcessReadmissionRatio <= 0 OR ExcessReadmissionRatio > 3;
