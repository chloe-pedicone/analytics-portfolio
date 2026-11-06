"""Watermark + lookback incremental load into a Delta table (Bronze -> Silver).

Pattern used in CS1 and P2. Generic rewrite: no employer code or object names.

How it works
------------
1. First run (target missing): full write.
2. Later runs: read MAX(watermark) from the target, subtract `lookback_days`,
   and only take source rows at or after that cutoff. The lookback re-reads
   late-arriving / restated rows.
3. De-duplicate the source slice on the merge keys (latest watermark wins),
   because Delta MERGE fails if several source rows match one target row.
4. MERGE: update matched rows, insert new ones.

Requirements: watermark column must be DATE or TIMESTAMP; merge keys must be
non-null (NULL never equals NULL in the merge condition).
"""
from __future__ import annotations

from datetime import timedelta
from typing import Iterable, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

try:  # Spark 3.4+
    from pyspark.errors import AnalysisException
except ImportError:  # pragma: no cover - older runtimes
    from pyspark.sql.utils import AnalysisException


def table_exists(spark: SparkSession, fq_table: str) -> bool:
    """True if the table resolves.

    Uses spark.table() rather than spark.catalog.tableExists(): in Fabric
    notebooks with no default lakehouse attached, catalog calls that rely on
    currentDatabase() can fail, while fully qualified reads still work.
    """
    try:
        spark.table(fq_table)  # analysis is eager -> raises if missing
        return True
    except AnalysisException:
        return False


def load_incremental(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    merge_keys: Iterable[str],
    watermark_col: str,
    lookback_days: int = 3,
    partition_by: Optional[Iterable[str]] = None,
) -> dict:
    merge_keys = list(merge_keys)
    if not merge_keys:
        raise ValueError("merge_keys must not be empty")
    missing = [c for c in [*merge_keys, watermark_col] if c not in source_df.columns]
    if missing:
        raise ValueError(f"Columns not in source_df: {missing}")

    # --- initial load -------------------------------------------------------
    if not table_exists(spark, target_table):
        writer = source_df.write.format("delta").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.saveAsTable(target_table)
        return {"mode": "initial", "cutoff": None}

    # --- incremental slice --------------------------------------------------
    max_wm = spark.table(target_table).agg(F.max(watermark_col)).first()[0]
    if max_wm is None:
        cutoff = None
        src = source_df
    else:
        cutoff = max_wm - timedelta(days=lookback_days)  # works for date and datetime
        src = source_df.filter(F.col(watermark_col) >= F.lit(cutoff))

    # one row per key: latest watermark wins
    w = Window.partitionBy(*merge_keys).orderBy(F.col(watermark_col).desc())
    src = src.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    condition = " AND ".join(f"t.`{k}` = s.`{k}`" for k in merge_keys)
    (
        DeltaTable.forName(spark, target_table).alias("t")
        .merge(src.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    return {"mode": "merge", "cutoff": cutoff}
