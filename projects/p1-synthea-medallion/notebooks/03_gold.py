# P1 · Gold: FactEncounter with readmission + LOS flags, conformed dims, date dim.

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append("../src")
from readmission import flag_readmissions, length_of_stay_days  # noqa: E402

spark = SparkSession.builder.getOrCreate()
SCHEMA = os.getenv("P1_SCHEMA", "synthea")

enc = spark.table(f"{SCHEMA}.silver_encounters")

# Flags apply to inpatient rows only; join back so every encounter stays in the fact.
flags = flag_readmissions(enc).select(
    "EncounterId", "DaysToNextAdmit", "IsReadmitIndex", "IsReadmission", "IsOverlap"
)

fact = (
    length_of_stay_days(enc)
    .join(flags, "EncounterId", "left")
    .withColumn("IsInpatient", F.col("EncounterClass") == "inpatient")
    .withColumn("StartDate", F.to_date("StartAt"))
    .withColumn("OutOfPocket", F.col("TotalClaimCost") - F.coalesce(F.col("PayerCoverage"), F.lit(0)))
    .select(
        "EncounterId", "PatientId", "ProviderId", "PayerId", "StartDate", "EncounterClass", "IsInpatient",
        "LengthOfStayDays", "TotalClaimCost", "PayerCoverage", "OutOfPocket",
        F.coalesce("IsReadmitIndex", F.lit(False)).alias("IsReadmitIndex"),
        F.coalesce("IsReadmission", F.lit(False)).alias("IsReadmission"),
        F.coalesce("IsOverlap", F.lit(False)).alias("IsOverlap"),
        "DaysToNextAdmit",
    )
)
fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.fact_encounter")

bounds = fact.select(F.min("StartDate").alias("lo"), F.max("StartDate").alias("hi")).first()
dim_date = (
    spark.sql(f"SELECT explode(sequence(DATE'{bounds.lo}', DATE'{bounds.hi}', INTERVAL 1 DAY)) AS Date")
    .withColumn("Year", F.year("Date"))
    .withColumn("MonthStart", F.trunc("Date", "month"))
)
dim_date.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.dim_date")

for src, dst in [("silver_patients", "dim_patient"), ("silver_payers", "dim_payer"), ("silver_providers", "dim_provider")]:
    spark.table(f"{SCHEMA}.{src}").write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"{SCHEMA}.{dst}")
print("Gold layer written")
