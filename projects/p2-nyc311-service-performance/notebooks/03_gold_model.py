# P2 · Gold: star schema for Power BI (Direct Lake or Import).
#   FactServiceRequest -> DimAgency, DimComplaintType, DimBorough, dim_date

import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
SCHEMA = os.getenv("P2_SCHEMA", "nyc311")

silver = spark.table(f"{SCHEMA}.silver_service_requests")


def dim(col: str, key: str):
    """Small dimension with an integer surrogate key (integer keys are cheaper to relate)."""
    w = Window.orderBy(col)
    return silver.select(col).distinct().withColumn(key, F.row_number().over(w).cast("int"))


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

# Full rebuild is fine at this size (a few million rows). Surrogate keys from
# row_number are only stable within one rebuild, so dims and fact are always
# written together.
for name, df in [("dim_agency", dim_agency), ("dim_complaint_type", dim_complaint),
                 ("dim_borough", dim_borough), ("fact_service_request", fact)]:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.{name}")
    print(f"wrote {SCHEMA}.{name}")
