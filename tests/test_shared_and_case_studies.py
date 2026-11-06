from datetime import date, datetime as dt

import pytest

from fabric_utils import build_dag, load_incremental, load_scd1, table_exists
from hour_category import add_hour_category
from time_window_match import match_referrals_to_appointments


# ---------------------------------------------------------------- incremental
def test_incremental_merge_lookback_and_dedupe(delta_spark):
    spark = delta_spark
    tbl = "default.t_incr"
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
    schema = "Id int, Val string, Dt date"
    first = spark.createDataFrame([(1, "a", date(2024, 1, 1)), (2, "b", date(2024, 1, 10))], schema)
    assert load_incremental(spark, first, tbl, ["Id"], "Dt", lookback_days=3)["mode"] == "initial"

    # Id 1 changed but is older than max(Dt)-3 -> ignored; Id 2 restated (dup in source,
    # latest wins); Id 3 new.
    second = spark.createDataFrame(
        [(1, "a2", date(2024, 1, 1)), (2, "b-old", date(2024, 1, 8)), (2, "b2", date(2024, 1, 9)),
         (3, "c", date(2024, 1, 11))],
        schema,
    )
    res = load_incremental(spark, second, tbl, ["Id"], "Dt", lookback_days=3)
    assert res["mode"] == "merge" and res["cutoff"] == date(2024, 1, 7)
    got = {r.Id: r.Val for r in spark.table(tbl).collect()}
    assert got == {1: "a", 2: "b2", 3: "c"}


def test_table_exists(spark):
    assert table_exists(spark, "default.does_not_exist_xyz") is False


# ---------------------------------------------------------------- SCD1
def test_scd1_overwrites_only_changed_rows(delta_spark):
    spark = delta_spark
    tbl = "default.t_scd1"
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
    schema = "AgentId int, Team string, Site string"
    load_scd1(spark, spark.createDataFrame([(1, "A", None), (2, "B", "X")], schema), tbl, ["AgentId"])
    before = {r.AgentId: r.LoadedAt for r in spark.table(tbl).collect()}

    load_scd1(spark, spark.createDataFrame([(1, "A", None), (2, "C", "X"), (3, "D", "Y")], schema), tbl, ["AgentId"])
    rows = {r.AgentId: r for r in spark.table(tbl).collect()}
    assert rows[2].Team == "C" and rows[3].Team == "D"
    assert rows[1].LoadedAt == before[1]          # unchanged row not rewritten
    assert rows[2].LoadedAt != before[2]


def test_scd1_null_vs_value_hash_differs(spark):
    from fabric_utils import add_row_hash
    df = spark.createDataFrame([("a", None), (None, "a")], "c1 string, c2 string")
    h = [r.RowHash for r in add_row_hash(df, ["c1", "c2"]).collect()]
    assert h[0] != h[1]


def test_scd1_rejects_duplicate_keys(spark):
    df = spark.createDataFrame([(1, "A"), (1, "B")], "AgentId int, Team string")
    with pytest.raises(ValueError):
        load_scd1(spark, df, "default.t_scd1_dupe", ["AgentId"])


# ---------------------------------------------------------------- HourCategory
def test_hour_category(spark):
    rules = spark.createDataFrame(
        [("OPS", 0, 24, False, False), ("SCHED", 8, 17, True, True)],
        "DeptCode string, StartHour int, EndHour int, WeekdaysOnly boolean, ExcludeHolidays boolean",
    )
    dates = spark.createDataFrame(
        [(date(2024, 7, 3), False, False), (date(2024, 7, 4), False, True), (date(2024, 7, 6), True, False)],
        "DateValue date, IsWeekend boolean, IsHoliday boolean",
    )
    fact = spark.createDataFrame(
        [
            ("r1", "SCHED", dt(2024, 7, 3, 8, 0)),    # weekday 08:00 -> Business
            ("r2", "SCHED", dt(2024, 7, 3, 17, 0)),   # 17:00, end exclusive -> Out
            ("r3", "SCHED", dt(2024, 7, 4, 10, 0)),   # holiday -> Out
            ("r4", "SCHED", dt(2024, 7, 6, 10, 0)),   # Saturday -> Out
            ("r5", "OPS", dt(2024, 7, 6, 3, 0)),      # 24/7 -> Business
            ("r6", "NEW", dt(2024, 7, 3, 10, 0)),     # no rule -> Unclassified
            ("r7", "SCHED", dt(2024, 8, 1, 10, 0)),   # date missing from dim -> Unclassified
            ("r8", "OPS", dt(2024, 8, 1, 10, 0)),     # 24/7 needs no calendar -> Business
        ],
        "Id string, DeptCode string, IntervalStart timestamp",
    ).withColumn("Date", __import__("pyspark.sql.functions", fromlist=["to_date"]).to_date("IntervalStart"))

    out = {r.Id: r.HourCategory for r in add_hour_category(fact, rules, dates).collect()}
    assert out == {
        "r1": "Business", "r2": "OutOfBusiness", "r3": "OutOfBusiness", "r4": "OutOfBusiness",
        "r5": "Business", "r6": "Unclassified", "r7": "Unclassified", "r8": "Business",
    }


# ---------------------------------------------------------------- time-window match
def test_time_window_match(spark):
    refs = spark.createDataFrame(
        [
            ("R1", "P1", "CARD", date(2024, 1, 1), date(2024, 1, 31), date(2024, 3, 1), False),
            ("R2", "P1", "CARD", date(2024, 1, 10), None, None, False),          # fallback window 90d
            ("R3", "P2", "DERM", date(2024, 1, 1), None, date(2024, 1, 20), False),  # expiration fallback
            ("R4", "P2", "DERM", date(2024, 1, 1), None, None, True),             # encountered -> excluded
        ],
        "ReferralID string, PatientKey string, Specialty string, StartDate date, EndDate date, "
        "ExpirationDate date, IsEncountered boolean",
    )
    appts = spark.createDataFrame(
        [
            ("A1", "P1", "CARD", date(2024, 1, 15), None),
            ("A1", "P1", "CARD", date(2024, 1, 15), None),     # duplicate row -> deduped
            ("A2", "P1", "CARD", date(2024, 2, 15), None),     # outside R1 end (1/31), inside R2 fallback
            ("A0", "P1", "CARD", date(2024, 1, 12), "RX"),     # already linked -> excluded before ranking
            ("A3", "P2", "DERM", date(2024, 1, 25), None),     # after R3 expiration -> no match
        ],
        "AppointmentID string, PatientKey string, Specialty string, AppointmentDate date, LinkedReferralID string",
    )
    out = {r.ReferralID: r.AppointmentID for r in match_referrals_to_appointments(refs, appts).collect()}
    # R1 and R2 both take A1 first; A1 goes to R2 (closer start, 5 days vs 14) in pass 2.
    # Greedy limitation documented in the module: R1 is left unmatched.
    assert out == {"R2": "A1"}


# ---------------------------------------------------------------- DAG
def test_build_dag_and_cycle_detection():
    dag = build_dag([("spine", []), ("a", ["spine"]), ("b", ["spine"])], args={"mode": "inc"})
    assert [a["name"] for a in dag["activities"]] == ["spine", "a", "b"]
    assert dag["activities"][1]["dependencies"] == ["spine"]
    with pytest.raises(ValueError):
        build_dag([("x", ["y"]), ("y", ["x"])])
    with pytest.raises(ValueError):
        build_dag([("x", ["missing"])])
