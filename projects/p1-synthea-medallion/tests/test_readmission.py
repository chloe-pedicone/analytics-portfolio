from datetime import datetime as dt

from readmission import flag_readmissions, length_of_stay_days

COLS = "EncounterId string, PatientId string, StartAt timestamp, StopAt timestamp, EncounterClass string"


def _rows(df):
    return {r["EncounterId"]: r for r in df.collect()}


def test_readmission_within_30_days(spark):
    df = spark.createDataFrame(
        [
            ("e1", "p1", dt(2024, 1, 1), dt(2024, 1, 5), "inpatient"),
            ("e2", "p1", dt(2024, 2, 4), dt(2024, 2, 6), "inpatient"),   # 30 days after e1 stop -> readmit
            ("e3", "p1", dt(2024, 6, 1), dt(2024, 6, 2), "inpatient"),   # 116 days later -> not
            ("o1", "p1", dt(2024, 1, 10), dt(2024, 1, 10), "outpatient"),  # ignored
        ],
        COLS,
    )
    r = _rows(flag_readmissions(df))
    assert set(r) == {"e1", "e2", "e3"}
    assert r["e1"]["IsReadmitIndex"] is True
    assert r["e2"]["IsReadmission"] is True
    assert r["e2"]["IsReadmitIndex"] is False
    assert r["e3"]["IsReadmission"] is False and r["e3"]["IsReadmitIndex"] is False


def test_day_31_is_not_readmission(spark):
    df = spark.createDataFrame(
        [
            ("a", "p2", dt(2024, 3, 1), dt(2024, 3, 1, 12), "inpatient"),
            ("b", "p2", dt(2024, 4, 1, 12, 0, 1), dt(2024, 4, 2), "inpatient"),  # 31 days + 1s
        ],
        COLS,
    )
    r = _rows(flag_readmissions(df))
    assert r["a"]["IsReadmitIndex"] is False
    assert r["b"]["IsReadmission"] is False


def test_overlap_and_missing_stop(spark):
    df = spark.createDataFrame(
        [
            ("x", "p3", dt(2024, 5, 1), dt(2024, 5, 10), "Inpatient"),
            ("y", "p3", dt(2024, 5, 8), dt(2024, 5, 12), "inpatient"),   # starts before x ends
            ("z", "p3", dt(2024, 5, 20), None, "inpatient"),              # still admitted
        ],
        COLS,
    )
    r = _rows(flag_readmissions(df))
    assert r["x"]["IsOverlap"] is True and r["x"]["IsReadmitIndex"] is False
    assert r["y"]["IsReadmission"] is False
    assert r["y"]["IsReadmitIndex"] is True          # z starts 8 days after y stops
    assert r["z"]["IsReadmission"] is True
    assert r["z"]["IsReadmitIndex"] is False          # no stop -> cannot be an index


def test_patients_are_independent(spark):
    df = spark.createDataFrame(
        [
            ("m1", "pA", dt(2024, 1, 1), dt(2024, 1, 2), "inpatient"),
            ("m2", "pB", dt(2024, 1, 5), dt(2024, 1, 6), "inpatient"),
        ],
        COLS,
    )
    r = _rows(flag_readmissions(df))
    assert not r["m1"]["IsReadmitIndex"] and not r["m2"]["IsReadmission"]


def test_length_of_stay(spark):
    df = spark.createDataFrame(
        [("l1", "p", dt(2024, 1, 1), dt(2024, 1, 3, 12), "inpatient"),
         ("l2", "p", dt(2024, 1, 5), dt(2024, 1, 4), "inpatient")],
        COLS,
    )
    r = _rows(length_of_stay_days(df))
    assert r["l1"]["LengthOfStayDays"] == 2.5
    assert r["l2"]["LengthOfStayDays"] is None
