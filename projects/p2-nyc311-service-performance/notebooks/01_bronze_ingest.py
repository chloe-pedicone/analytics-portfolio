# Fabric notebook source / plain PySpark script
# P2 · Bronze: pull NYC 311 service requests incrementally from the Socrata API.
#
# Dataset: 311 Service Requests from 2020 to Present (erm2-nwe9)
#   https://data.cityofnewyork.us/d/erm2-nwe9
# Optional: set env var NYC_APP_TOKEN (free) for higher rate limits.

import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from pyspark.sql import types as T

try:  # local run: make <repo>/shared importable no matter which folder you run from
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "shared"))
except NameError:  # Fabric notebook has no __file__: upload fabric_utils as a library/resource
    pass
from fabric_utils import get_spark, load_incremental, table_exists  # noqa: E402

# ---- config ---------------------------------------------------------------
SCHEMA = os.getenv("P2_SCHEMA", "nyc311")          # Fabric: "<lakehouse>.<schema>" if schemas enabled
spark = get_spark("p2-bronze", schemas=[SCHEMA])   # Fabric: reuses the notebook session
BRONZE_TABLE = f"{SCHEMA}.bronze_service_requests"
ENDPOINT = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
INITIAL_START = os.getenv("P2_INITIAL_START", "2026-10-25T00:00:00")  # small test window; widen later
LOOKBACK_DAYS = 3                                   # re-pull recent rows: status/closed_date change
PAGE_SIZE = 50_000
COLUMNS = [
    "unique_key", "created_date", "closed_date", "agency", "complaint_type",
    "descriptor", "status", "borough", "incident_zip", "resolution_action_updated_date",
]

bronze_schema = T.StructType(
    [T.StructField(c, T.StringType(), True) for c in COLUMNS]
    + [T.StructField("_ingested_at", T.TimestampType(), False)]
)


def start_from() -> str:
    """Incremental start: max(created_date) in Bronze minus lookback, else INITIAL_START."""
    if not table_exists(spark, BRONZE_TABLE):
        return INITIAL_START
    max_created = spark.table(BRONZE_TABLE).selectExpr("max(created_date)").first()[0]
    if max_created is None:
        return INITIAL_START
    ts = datetime.fromisoformat(max_created[:19]) - timedelta(days=LOOKBACK_DAYS)
    return ts.strftime("%Y-%m-%dT%H:%M:%S")


RETRY_STATUS = {429, 500, 502, 503, 504}   # throttled or temporary server errors
MAX_ATTEMPTS = 6


def get_page(params: dict, headers: dict) -> list:
    """GET one page; on throttling/temporary errors wait and retry instead of crashing."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = requests.get(ENDPOINT, params=params, headers=headers, timeout=120)
        except (requests.ConnectionError, requests.Timeout) as exc:
            resp, reason = None, type(exc).__name__
        else:
            if resp.status_code not in RETRY_STATUS:
                resp.raise_for_status()          # other 4xx (bad query, bad token): fail fast
                return resp.json()
            reason = f"HTTP {resp.status_code}"
        if attempt == MAX_ATTEMPTS:
            if resp is not None:
                resp.raise_for_status()
            raise RuntimeError(f"API still failing after {MAX_ATTEMPTS} attempts ({reason})")
        retry_after = resp.headers.get("Retry-After", "") if resp is not None else ""
        wait = int(retry_after) if retry_after.isdigit() else min(5 * 2 ** (attempt - 1), 120)
        print(f"{reason} at offset {params['$offset']:,}; retry {attempt}/{MAX_ATTEMPTS - 1} in {wait}s")
        time.sleep(wait)


def fetch(since: str):
    token = os.getenv("NYC_APP_TOKEN")
    if not token:
        print("warning: NYC_APP_TOKEN not set - shared IP pool, expect 429 throttling")
    headers = {"X-App-Token": token} if token else {}
    offset = 0
    while True:
        params = {
            "$select": ",".join(COLUMNS),
            "$where": f"created_date >= '{since}'",
            "$order": "created_date, unique_key",   # stable order -> safe offset paging
            "$limit": PAGE_SIZE,
            "$offset": offset,
        }
        page = get_page(params, headers)
        if not page:
            return
        yield page
        if len(page) < PAGE_SIZE:
            return
        offset += PAGE_SIZE


since = start_from()
ingested_at = datetime.utcnow()
rows = []
for page in fetch(since):
    rows.extend(tuple(r.get(c) for c in COLUMNS) + (ingested_at,) for r in page)

print(f"Fetched {len(rows):,} rows since {since}")
if rows:
    src = spark.createDataFrame(rows, bronze_schema)
    # Bronze keeps raw strings; latest pull of a unique_key wins.
    result = load_incremental(
        spark, src, BRONZE_TABLE,
        merge_keys=["unique_key"], watermark_col="_ingested_at", lookback_days=0,
    )
    print(result)

# Memory note: for multi-million-row backfills, write each page to Files/ as
# JSON and read with spark.read.json instead of collecting rows in the driver.
