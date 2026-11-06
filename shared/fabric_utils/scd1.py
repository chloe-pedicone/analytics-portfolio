"""SCD Type 1 dimension load with hash-based change detection.

Only rows whose tracked attributes actually changed are rewritten, so Delta
creates fewer new files and downstream Direct Lake models reframe less data.
A corrected attribute (e.g. a team re-assigned in a later extract) overwrites
the existing row in place - no history is kept (that would be Type 2).
"""
from __future__ import annotations

from typing import Iterable, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .incremental_merge import table_exists

HASH_COL = "RowHash"
LOADED_COL = "LoadedAt"
_NULL_TOKEN = "\u0000NULL\u0000"  # distinguishes NULL from the string 'NULL'


def add_row_hash(df: DataFrame, tracked_cols: Iterable[str]) -> DataFrame:
    # concat_ws silently skips NULLs, so ('a', NULL) and (NULL, 'a') would
    # hash the same without the coalesce.
    parts = [F.coalesce(F.col(c).cast("string"), F.lit(_NULL_TOKEN)) for c in tracked_cols]
    return df.withColumn(HASH_COL, F.sha2(F.concat_ws("||", *parts), 256))


def load_scd1(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    key_cols: Iterable[str],
    tracked_cols: Optional[Iterable[str]] = None,
) -> dict:
    key_cols = list(key_cols)
    tracked = list(tracked_cols) if tracked_cols else [c for c in source_df.columns if c not in key_cols]

    dupes = source_df.groupBy(*key_cols).count().filter("count > 1").limit(1).count()
    if dupes:
        raise ValueError(f"source_df has duplicate rows for keys {key_cols}; de-duplicate first")

    src = add_row_hash(source_df, tracked).withColumn(LOADED_COL, F.current_timestamp())

    if not table_exists(spark, target_table):
        src.write.format("delta").mode("overwrite").saveAsTable(target_table)
        return {"mode": "initial"}

    condition = " AND ".join(f"t.`{k}` = s.`{k}`" for k in key_cols)
    (
        DeltaTable.forName(spark, target_table).alias("t")
        .merge(src.alias("s"), condition)
        .whenMatchedUpdateAll(condition=f"t.`{HASH_COL}` <> s.`{HASH_COL}` OR t.`{HASH_COL}` IS NULL")
        .whenNotMatchedInsertAll()
        .execute()
    )
    return {"mode": "merge"}
