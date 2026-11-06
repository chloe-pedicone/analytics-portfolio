# P2 · Gold: star schema for Power BI (Direct Lake or Import).
#   FactServiceRequest -> DimAgency, DimComplaintType, DimBorough, dim_date

import os
import sys
from pathlib import Path

try:  # local run: make <repo>/shared importable no matter which folder you run from
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "shared"))
except NameError:  # Fabric notebook has no __file__: upload fabric_utils as a library/resource
    pass

from pyspark.sql import functions as F

from fabric_utils import get_spark  # noqa: E402

SCHEMA = os.getenv("P2_SCHEMA", "nyc311")
spark = get_spark("p2-gold", schemas=[SCHEMA])

silver = spark.table(f"{SCHEMA}.silver_service_requests")


def dim(col: str, key: str):
       """Small dimension with a deterministic surrogate key: the same name gets the same key on
       every run, so a new name can't shift existing keys and a half-finished run can't leave
       dims and fact mismatched. xxhash64 -> bigint (Power BI Int64)."""
       return silver.select(col).distinct().withColumn(key, F.xxhash64(col))


dim_agency = dim("Agency", "AgencyKey")
dim_complaint = dim("ComplaintType", "ComplaintTypeKey")
dim_borough = dim("Borough", "BoroughKey")

fact = (
    silver
    .join(F.broadcast(dim_agency), "Agency")
    .join(F.broadcast(dim_complaint), "ComplaintType")
    .join(F.broadcast(dim_borough), "Borough")
    .select(
        "UniqueKey", "CreatedDate", "CreatedHour", "AgencyKey", "ComplaintTypeKey", "BoroughKey",
        "Status", "HoursToClose", "SlaHours", "WithinSLA", "HourCategory",
        F.col("ClosedAt").isNotNull().alias("IsClosed"),
    )
)

# Full rebuild is fine at this size (a few million rows). Keys are hashes of the
# name, so they are stable across rebuilds.

for name, df in [("dim_agency", dim_agency), ("dim_complaint_type", dim_complaint),
                 ("dim_borough", dim_borough), ("fact_service_request", fact)]:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.{name}")
    print(f"wrote {SCHEMA}.{name}")
