"""Ranked, time-windowed matching: link referrals to appointments.

Generic rewrite of the fuzzy-match branch from a Tableau Prep -> PySpark
migration. Each numbered comment is a bug I found in the first translation.

Inputs (generic columns)
  referrals:    ReferralID, PatientKey, Specialty, StartDate, EndDate,
                ExpirationDate, IsEncountered (bool)
  appointments: AppointmentID, PatientKey, Specialty, AppointmentDate,
                LinkedReferralID (null = not linked by the source system)

Output: one row per matched pair, at most one appointment per referral and
one referral per appointment (greedy: earliest appointment wins).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def match_referrals_to_appointments(
    referrals: DataFrame,
    appointments: DataFrame,
    default_window_days: int = 90,
) -> DataFrame:
    # Bug 1 fixed: precedence. An explicit EndDate wins; ExpirationDate is the
    #   fallback. The first translation had the coalesce arguments reversed.
    # Bug 2 fixed: when both are null, fall back to StartDate + default window
    #   instead of dropping the referral (NULL end -> BETWEEN never true).
    refs = (
        referrals
        .filter(~F.coalesce(F.col("IsEncountered"), F.lit(False)))  # only un-encountered
        .withColumn(
            "WindowEnd",
            F.coalesce(F.col("EndDate"), F.col("ExpirationDate"),
                       F.date_add(F.col("StartDate"), default_window_days)),
        )
        .select("ReferralID", "PatientKey", "Specialty", "StartDate", "WindowEnd")
    )

    # Bug 3 fixed: filter to *unlinked* appointments BEFORE ranking. Ranking
    #   first let already-linked appointments take rank 1, and the later
    #   filter then dropped the referral's only candidate.
    # Bug 4 fixed: de-duplicate appointments on their key before joining. An
    #   un-deduplicated visit/provider join multiplied appointment rows, which
    #   duplicated matches and broke the ranking.
    appts = (
        appointments
        .filter(F.col("LinkedReferralID").isNull())
        .dropDuplicates(["AppointmentID"])
        .select("AppointmentID", "PatientKey", "Specialty", "AppointmentDate")
    )

    candidates = (
        refs.alias("r")
        .join(
            appts.alias("a"),
            (F.col("r.PatientKey") == F.col("a.PatientKey"))
            & (F.col("r.Specialty") == F.col("a.Specialty"))
            & F.col("a.AppointmentDate").between(F.col("r.StartDate"), F.col("r.WindowEnd")),
            "inner",
        )
        .select(
            "r.ReferralID", "r.PatientKey", "r.Specialty", "r.StartDate", "r.WindowEnd",
            "a.AppointmentID", "a.AppointmentDate",
            F.datediff(F.col("a.AppointmentDate"), F.col("r.StartDate")).alias("DaysToAppointment"),
        )
    )

    # Pass 1: each referral keeps its earliest appointment (deterministic tie-break).
    by_ref = Window.partitionBy("ReferralID").orderBy("AppointmentDate", "AppointmentID")
    pass1 = candidates.withColumn("_r1", F.row_number().over(by_ref)).filter("_r1 = 1").drop("_r1")

    # Pass 2: an appointment claimed by several referrals goes to the one
    # that started most recently before it (closest window start).
    by_appt = Window.partitionBy("AppointmentID").orderBy(F.col("DaysToAppointment").asc(), F.col("ReferralID").asc())
    return pass1.withColumn("_r2", F.row_number().over(by_appt)).filter("_r2 = 1").drop("_r2")
