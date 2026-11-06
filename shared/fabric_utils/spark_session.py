"""One way to get a Spark session that works both in Fabric and locally.

Fabric: a session already exists (Delta + lakehouse catalog configured) -> reuse it.
Local (Codespaces / laptop): build one with
  * Delta Lake (jars resolved by delta-spark's pip helper),
  * a persistent table catalog (Hive metastore in <repo>/metastore_db) so a table
    written by one script can be read by the next,
  * one warehouse folder (<repo>/spark-warehouse) no matter which folder you run from,
  * UTC session time zone (same as Fabric's default), and
  * the requested schemas created if missing.
Both folders are in .gitignore.
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

from pyspark.sql import SparkSession

_REPO_ROOT = Path(__file__).resolve().parents[2]


def get_spark(app_name: str = "portfolio", schemas: Iterable[str] = ()) -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:  # Fabric notebook (or any pre-configured session)
        return active

    from delta import configure_spark_with_delta_pip

    warehouse = _REPO_ROOT / "spark-warehouse"
    metastore = _REPO_ROOT / "metastore_db"
    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={metastore};create=true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")   # local: default 200 is wasteful
        .config("spark.ui.showConsoleProgress", "false")
        .enableHiveSupport()
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    return spark
