"""Repo-wide pytest setup: ONE Spark session shared by every test folder.

Spark allows a single active session per process. If two conftest files each
build their own, whichever runs first wins and the second silently gets the
first one's settings (e.g. Delta configs on a session without the Delta jars).
So the session lives here, at the repo root, and nowhere else.
"""
import shutil
import tempfile

import pytest

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


def _delta_classes_loaded(session) -> bool:
    """True only if the Delta jars are really on the JVM classpath."""
    from py4j.java_gateway import JavaClass

    return isinstance(session._jvm.io.delta.tables.DeltaTable, JavaClass)


@pytest.fixture(scope="session")
def spark():
    """Delta-enabled session when the Delta jars can be resolved, else plain Spark."""
    session = None
    try:
        from delta import configure_spark_with_delta_pip

        builder = (
            _base_builder()
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        session = configure_spark_with_delta_pip(builder).getOrCreate()
        _STATE["delta"] = _delta_classes_loaded(session)
    except Exception:  # no network to Maven, or delta-spark not installed
        _STATE["delta"] = False

    if not _STATE["delta"]:
        # Never keep a session that has Delta configs but no Delta jars.
        if session is not None:
            session.stop()
        session = _base_builder().getOrCreate()

    yield session
    session.stop()
    shutil.rmtree(_WAREHOUSE, ignore_errors=True)


@pytest.fixture
def delta_spark(spark):
    if not _STATE["delta"]:
        pytest.skip("Delta Lake jars unavailable in this environment")
    return spark
