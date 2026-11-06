import shutil
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "shared"))
sys.path.insert(0, str(ROOT / "case-studies" / "01-contact-center-kpi-model"))
sys.path.insert(0, str(ROOT / "case-studies" / "03-tableau-prep-migration"))

_WAREHOUSE = tempfile.mkdtemp(prefix="pf-wh-")
_STATE = {"delta": False}


def _base_builder():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[1]")
        .appName("portfolio-tests")
        .config("spark.sql.warehouse.dir", _WAREHOUSE)
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )


@pytest.fixture(scope="session")
def spark():
    """Delta-enabled session when the Delta jars can be resolved, else plain Spark."""
    try:
        from delta import configure_spark_with_delta_pip

        builder = (
            _base_builder()
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        session = configure_spark_with_delta_pip(builder).getOrCreate()
        _STATE["delta"] = True
    except Exception:  # no network to Maven, or delta-spark not installed
        session = _base_builder().getOrCreate()
    yield session
    session.stop()
    shutil.rmtree(_WAREHOUSE, ignore_errors=True)


@pytest.fixture
def delta_spark(spark):
    if not _STATE["delta"]:
        pytest.skip("Delta Lake jars unavailable in this environment")
    return spark
