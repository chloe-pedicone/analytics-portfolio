# P3 · Bronze: land Synthea CSV exports as Delta tables, untouched (all strings).
#
# Generate data (Java 17+):
#   git clone https://github.com/synthetichealth/synthea && cd synthea
#   ./run_synthea -p 5000 --exporter.csv.export=true
# then upload output/csv/*.csv to Files/synthea/ (Fabric) or point RAW_DIR at it.

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
RAW_DIR = os.getenv("SYNTHEA_RAW_DIR", "Files/synthea")
SCHEMA = os.getenv("P3_SCHEMA", "synthea")
TABLES = ["patients", "encounters", "conditions", "procedures", "payers", "providers", "organizations"]

for t in TABLES:
    df = (
        spark.read.option("header", True).option("inferSchema", False).csv(f"{RAW_DIR}/{t}.csv")
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
    )
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.bronze_{t}")
    print(f"bronze_{t}: {df.count():,} rows")
