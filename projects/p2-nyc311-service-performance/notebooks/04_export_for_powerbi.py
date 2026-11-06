# P2 · Export: write each Gold table as ONE Parquet file for Power BI Desktop (local Import mode).
#   Not needed in Fabric: there the report reads the Lakehouse tables directly (Direct Lake).
#
#   python projects/p2-nyc311-service-performance/notebooks/04_export_for_powerbi.py
#   -> <repo>/pbi_export/<table>.parquet  (gitignored); zip it, download it, and point the
#      report's DataFolder parameter at the unzipped folder.

import os
import shutil
import sys
from pathlib import Path

try:  # local run: make <repo>/shared importable no matter which folder you run from
    REPO = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(REPO / "shared"))
except NameError:  # Fabric notebook has no __file__
    REPO = Path.cwd()

from fabric_utils import get_spark  # noqa: E402

SCHEMA = os.getenv("P2_SCHEMA", "nyc311")
TABLES = ["dim_agency", "dim_complaint_type", "dim_borough", "dim_date", "fact_service_request"]
OUT = REPO / "pbi_export"

spark = get_spark("p2-export", schemas=[SCHEMA])
OUT.mkdir(exist_ok=True)

for t in TABLES:
    df = spark.table(f"{SCHEMA}.{t}")
    tmp = OUT / f"_{t}_tmp"
    # coalesce(1): one file per table without collecting rows to the driver (unlike toPandas)
    df.coalesce(1).write.mode("overwrite").parquet(str(tmp))
    part = next(tmp.glob("part-*.parquet"))
    target = OUT / f"{t}.parquet"
    target.unlink(missing_ok=True)
    shutil.move(str(part), str(target))
    shutil.rmtree(tmp)
    print(f"{t}: {df.count():,} rows -> {target.relative_to(REPO)}")
