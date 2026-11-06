# P1 · Silver: typed, de-duplicated, validated. Patient dim loaded as SCD Type 1.

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append("../../../shared")
from fabric_utils import load_scd1  # noqa: E402

spark = SparkSession.builder.getOrCreate()
SCHEMA = os.getenv("P1_SCHEMA", "synthea")


def bronze(t):
    return spark.table(f"{SCHEMA}.bronze_{t}")


# ---- encounters -------------------------------------------------------------
encounters = (
    bronze("encounters")
    .select(
        F.col("Id").alias("EncounterId"),
        F.col("PATIENT").alias("PatientId"),
        F.col("PROVIDER").alias("ProviderId"),
        F.col("ORGANIZATION").alias("OrganizationId"),
        F.col("PAYER").alias("PayerId"),
        F.to_timestamp("START").alias("StartAt"),
        F.to_timestamp("STOP").alias("StopAt"),
        F.lower(F.trim("ENCOUNTERCLASS")).alias("EncounterClass"),
        F.col("CODE").alias("EncounterCode"),
        F.col("DESCRIPTION").alias("EncounterDescription"),
        F.col("TOTAL_CLAIM_COST").cast("decimal(18,2)").alias("TotalClaimCost"),
        F.col("PAYER_COVERAGE").cast("decimal(18,2)").alias("PayerCoverage"),
    )
    .dropDuplicates(["EncounterId"])
)

# ---- data-quality gate: fail loudly instead of loading bad data ------------
checks = {
    "null_encounter_id": encounters.filter(F.col("EncounterId").isNull()).count(),
    "null_patient_id": encounters.filter(F.col("PatientId").isNull()).count(),
    "unparsed_start": encounters.filter(F.col("StartAt").isNull()).count(),
    "stop_before_start": encounters.filter(F.col("StopAt") < F.col("StartAt")).count(),
}
print(checks)
hard_failures = {k: v for k, v in checks.items() if k != "stop_before_start" and v > 0}
if hard_failures:
    raise ValueError(f"Data-quality gate failed: {hard_failures}")

encounters.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.silver_encounters")

# ---- patients: SCD1 (address/income corrections overwrite in place) --------
patients = (
    bronze("patients")
    .select(
        F.col("Id").alias("PatientId"),
        F.to_date("BIRTHDATE").alias("BirthDate"),
        F.to_date("DEATHDATE").alias("DeathDate"),
        F.col("GENDER").alias("Gender"),
        F.col("COUNTY").alias("County"),
        F.col("STATE").alias("State"),
    )
    .dropDuplicates(["PatientId"])
)
print(load_scd1(spark, patients, f"{SCHEMA}.silver_patients", key_cols=["PatientId"]))

# ---- payers / providers (small, full overwrite) ----------------------------
bronze("payers").select(F.col("Id").alias("PayerId"), F.col("NAME").alias("PayerName")) \
    .dropDuplicates(["PayerId"]).write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.silver_payers")
bronze("providers").select(F.col("Id").alias("ProviderId"), F.col("SPECIALITY").alias("Specialty")) \
    .dropDuplicates(["ProviderId"]).write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.silver_providers")
