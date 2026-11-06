"""30-day all-cause inpatient readmission flags (Silver -> Gold).

Definition used (simplified; stated in the README):
- Index admission: an inpatient encounter with a discharge time (STOP).
- Readmission: the patient's NEXT inpatient admission starts within
  `window_days` days (inclusive) after the index discharge.
- An admission that starts before the previous one ends (overlap / transfer)
  is NOT counted as a readmission; it is flagged IsOverlap for review.
- Planned readmissions are not excluded (Synthea has no planned flag).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

SECONDS_PER_DAY = 86_400


def flag_readmissions(encounters: DataFrame, window_days: int = 30) -> DataFrame:
    """Expects Silver columns: EncounterId, PatientId, StartAt, StopAt, EncounterClass.

    Returns inpatient encounters with:
      NextAdmitAt, DaysToNextAdmit, IsReadmitIndex (this stay was followed by a
      readmission), IsReadmission (this stay IS a readmission), IsOverlap.
    """
    inpatient = encounters.filter(
        (F.lower(F.col("EncounterClass")) == "inpatient") & F.col("StartAt").isNotNull()
    )

    w = Window.partitionBy("PatientId").orderBy("StartAt", "EncounterId")
    gap_days = (F.unix_timestamp("NextAdmitAt") - F.unix_timestamp("StopAt")) / SECONDS_PER_DAY
    prev_gap_days = (F.unix_timestamp("StartAt") - F.unix_timestamp("PrevStopAt")) / SECONDS_PER_DAY

    return (
        inpatient
        .withColumn("NextAdmitAt", F.lead("StartAt").over(w))
        .withColumn("PrevStopAt", F.lag("StopAt").over(w))
        .withColumn("DaysToNextAdmit", F.round(gap_days, 2))
        .withColumn(
            "IsReadmitIndex",
            F.coalesce(F.col("StopAt").isNotNull() & (gap_days >= 0) & (gap_days <= window_days), F.lit(False)),
        )
        .withColumn(
            "IsReadmission",
            F.coalesce((prev_gap_days >= 0) & (prev_gap_days <= window_days), F.lit(False)),
        )
        .withColumn("IsOverlap", F.coalesce(gap_days < 0, F.lit(False)))
        .drop("PrevStopAt")
    )


def length_of_stay_days(df: DataFrame) -> DataFrame:
    los = (F.unix_timestamp("StopAt") - F.unix_timestamp("StartAt")) / SECONDS_PER_DAY
    return df.withColumn("LengthOfStayDays", F.when(los >= 0, F.round(los, 2)))
