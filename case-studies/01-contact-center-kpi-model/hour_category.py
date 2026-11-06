"""Classify interval rows as Business / OutOfBusiness / Unclassified in PySpark.

Why it's in Silver instead of DAX: the rule used to run inside a FILTER over
the fact table in several measures. Each measure re-evaluated hour, weekday,
holiday and department per row at query time. Materialising one column
makes every measure a plain filter on a low-cardinality column.

Rules are data, not code: one row per department code in `rules_df`.
    DeptCode | StartHour | EndHour | WeekdaysOnly | ExcludeHolidays
    OPS      | 0         | 24      | false        | false     (24/7 team)
    SCHED    | 8         | 17      | true         | true
    CLINIC   | 7         | 19      | true         | true
EndHour is exclusive: 8-17 means 08:00:00 to 16:59:59.

Edge cases handled
- Department not in rules -> "Unclassified" (a binary DAX IF silently put
  these into out-of-business; adding a new department then skewed KPIs).
- Date not in the date dimension while the rule needs a weekday/holiday
  check -> "Unclassified" rather than guessing.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

BUSINESS, OUT_OF_BUSINESS, UNCLASSIFIED = "Business", "OutOfBusiness", "Unclassified"


def add_hour_category(
    fact_df: DataFrame,
    rules_df: DataFrame,
    date_dim: DataFrame,
    dept_col: str = "DeptCode",
    ts_col: str = "IntervalStart",
    date_col: str = "Date",
    date_key: str = "DateValue",
    out_col: str = "HourCategory",
) -> DataFrame:
    rules = rules_df.select(
        F.col("DeptCode").alias("_rule_dept"),
        F.col("StartHour").cast("int").alias("_start"),
        F.col("EndHour").cast("int").alias("_end"),
        F.col("WeekdaysOnly").cast("boolean").alias("_wk_only"),
        F.col("ExcludeHolidays").cast("boolean").alias("_ex_hol"),
    )
    dates = date_dim.select(
        F.col(date_key).alias("_d"),
        F.col("IsWeekend").cast("boolean").alias("_is_weekend"),
        F.col("IsHoliday").cast("boolean").alias("_is_holiday"),
    )

    df = (
        fact_df
        .withColumn("_hr", F.hour(F.col(ts_col)))
        .join(F.broadcast(rules), F.col(dept_col) == F.col("_rule_dept"), "left")
        .join(F.broadcast(dates), F.col(date_col) == F.col("_d"), "left")
    )

    needs_calendar = F.col("_wk_only") | F.col("_ex_hol")
    in_window = (F.col("_hr") >= F.col("_start")) & (F.col("_hr") < F.col("_end"))
    day_ok = (
        (~F.col("_wk_only") | ~F.col("_is_weekend"))
        & (~F.col("_ex_hol") | ~F.col("_is_holiday"))
    )

    category = (
        F.when(F.col("_rule_dept").isNull(), F.lit(UNCLASSIFIED))
        .when(F.col("_hr").isNull(), F.lit(UNCLASSIFIED))
        .when(needs_calendar & F.col("_d").isNull(), F.lit(UNCLASSIFIED))
        .when(in_window & F.coalesce(day_ok, F.lit(True)), F.lit(BUSINESS))
        .otherwise(F.lit(OUT_OF_BUSINESS))
    )

    helper_cols = ["_hr", "_rule_dept", "_start", "_end", "_wk_only", "_ex_hol", "_d", "_is_weekend", "_is_holiday"]
    return df.withColumn(out_col, category).drop(*helper_cols)
