# P2 · Silver: typed, de-duplicated service requests + HourCategory + SLA flag.

import os
import sys
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.append("../../../shared")
from fabric_utils import load_incremental  # noqa: E402

spark = SparkSession.builder.getOrCreate()
SCHEMA = os.getenv("P2_SCHEMA", "nyc311")

# Business hours for 311 agencies (public replay of the CS1 rule): Mon-Fri 08:00-18:00, excl. holidays.
BUSINESS_START, BUSINESS_END = 8, 18      # end exclusive
DEFAULT_SLA_HOURS = 120                   # used when a complaint type has no specific SLA
SLA_HOURS_BY_COMPLAINT = {                # illustrative targets - state them as assumptions in the README
    "Noise - Residential": 12,
    "Illegal Parking": 24,
    "HEAT/HOT WATER": 72,
}


def build_date_dim(start: date, end: date) -> DataFrame:
    """Calendar with weekend/holiday flags (US federal holidays via `holidays`)."""
    import holidays  # pip install holidays

    us = holidays.country_holidays("US", years=range(start.year, end.year + 1))
    hol = spark.createDataFrame([(d,) for d in us.keys()], "Date date").withColumn("IsHoliday", F.lit(True))
    days = spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}', INTERVAL 1 DAY)) AS Date")
    return (
        days.join(hol, "Date", "left")
        .withColumn("IsHoliday", F.coalesce("IsHoliday", F.lit(False)))
        .withColumn("IsWeekend", F.dayofweek("Date").isin(1, 7))         # 1 = Sunday, 7 = Saturday
        .withColumn("IsBusinessDay", ~F.col("IsWeekend") & ~F.col("IsHoliday"))
        .withColumn("WeekStart", F.date_trunc("week", "Date").cast("date"))  # Monday
        .withColumn("MonthStart", F.trunc("Date", "month"))
        .withColumn("Year", F.year("Date"))
        .withColumn("YearQuarter", F.concat(F.year("Date"), F.lit("-Q"), F.quarter("Date")))
    )


def to_silver(bronze: DataFrame, date_dim: DataFrame) -> DataFrame:
    sla_map = F.create_map(*[x for k, v in SLA_HOURS_BY_COMPLAINT.items() for x in (F.lit(k), F.lit(v))])

    df = (
        bronze
        .withColumn("UniqueKey", F.col("unique_key").cast("bigint"))
        .withColumn("CreatedAt", F.to_timestamp("created_date"))
        .withColumn("ClosedAt", F.to_timestamp("closed_date"))
        .withColumn("CreatedDate", F.to_date("CreatedAt"))
        .withColumn("CreatedHour", F.hour("CreatedAt"))
        .withColumn("Agency", F.upper(F.trim("agency")))
        .withColumn("ComplaintType", F.trim("complaint_type"))
        .withColumn("Borough", F.coalesce(F.upper(F.trim("borough")), F.lit("UNSPECIFIED")))
        .withColumn("Status", F.trim("status"))
        .filter(F.col("UniqueKey").isNotNull() & F.col("CreatedAt").isNotNull())
    )

    # Data quality: closed before created happens in the source -> treat duration as unknown.
    hours = (F.unix_timestamp("ClosedAt") - F.unix_timestamp("CreatedAt")) / 3600.0
    df = df.withColumn("HoursToClose", F.when(F.col("ClosedAt") >= F.col("CreatedAt"), F.round(hours, 2)))
    df = df.withColumn("IsClosedTimeValid", F.col("HoursToClose").isNotNull())

    sla = F.coalesce(F.element_at(sla_map, F.col("ComplaintType")), F.lit(DEFAULT_SLA_HOURS))
    df = df.withColumn("SlaHours", sla).withColumn(
        "WithinSLA", F.when(F.col("IsClosedTimeValid"), F.col("HoursToClose") <= F.col("SlaHours"))
    )

    cal = date_dim.select(F.col("Date").alias("_d"), "IsBusinessDay")
    df = (
        df.join(F.broadcast(cal), F.col("CreatedDate") == F.col("_d"), "left")
        .withColumn(
            "HourCategory",
            F.when(F.col("_d").isNull(), "Unclassified")
            .when(F.col("IsBusinessDay") & F.col("CreatedHour").between(BUSINESS_START, BUSINESS_END - 1), "Business")
            .otherwise("OutOfBusiness"),
        )
        .drop("_d", "IsBusinessDay")
    )

    return df.select(
        "UniqueKey", "CreatedAt", "ClosedAt", "CreatedDate", "CreatedHour", "Agency", "ComplaintType",
        "Borough", "Status", "HoursToClose", "IsClosedTimeValid", "SlaHours", "WithinSLA",
        "HourCategory", F.col("_ingested_at").alias("IngestedAt"),
    )


if __name__ == "__main__":
    bronze = spark.table(f"{SCHEMA}.bronze_service_requests")
    bounds = bronze.select(F.min(F.to_date("created_date")), F.max(F.to_date("created_date"))).first()
    date_dim = build_date_dim(bounds[0], date(bounds[1].year, 12, 31))
    date_dim.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.dim_date")

    silver = to_silver(bronze, date_dim)
    print(load_incremental(spark, silver, f"{SCHEMA}.silver_service_requests",
                           merge_keys=["UniqueKey"], watermark_col="IngestedAt", lookback_days=0))
