from datetime import date, datetime

from src.gold.ml_training import (add_exclusion_reason, add_target, add_calendar_features,
                                  add_holiday_features, add_history_features, add_split)


def test_target_starts_at_15_minutes(spark):
    df = spark.createDataFrame([(14,), (15,), (-3,)], "arrival_delay int")
    assert [r["is_delayed"] for r in add_target(df).collect()] == [0, 1, 0]


def test_exclusion_reason(spark):
    df = spark.createDataFrame(
        [(True, False, True, None), (False, True, True, 10), (False, False, False, 10), (False, False, True, 10)],
        "is_cancelled boolean, is_diverted boolean, has_weather boolean, arrival_delay int")
    reasons = [r["exclusion_reason"] for r in add_exclusion_reason(df).collect()]
    assert reasons == ["cancelado", "desviado", "sin clima en el origen", None]


def test_calendar_features(spark):
    df = spark.createDataFrame([(date(2015, 7, 4), 6, datetime(2015, 7, 4, 14, 35), datetime(2015, 7, 4, 17, 10))],
                               "flight_date date, day_of_week int, departure_planned_local timestamp_ntz, "
                               "arrival_planned_local timestamp_ntz")
    row = add_calendar_features(df).first()
    assert (row["month"], row["day_of_month"], row["is_weekend"]) == (7, 4, True)
    assert (row["departure_hour_local"], row["arrival_hour_local"]) == (14, 17)


def test_days_to_nearest_national_holiday(spark):
    flights = spark.createDataFrame([(date(2015, 7, 3),), (date(2015, 7, 6),)], "flight_date date")
    holidays = spark.createDataFrame([(date(2015, 7, 3), True), (date(2015, 7, 5), False)],
                                     "holiday_date date, is_national boolean")
    rows = {r["flight_date"]: r for r in add_holiday_features(flights, holidays).collect()}
    assert rows[date(2015, 7, 3)]["is_holiday"] is True
    assert rows[date(2015, 7, 6)]["days_to_nearest_holiday"] == 3     # el regional no cuenta


def test_history_only_looks_at_previous_days(spark):
    # Un vuelo demorado el 1/1 y otro puntual el 10/1; queremos la historia del 10/1
    history = spark.createDataFrame([("AA", date(2015, 1, 1), 60), ("AA", date(2015, 1, 10), 0)],
                                    "airline string, flight_date date, arrival_delay int")
    flights = spark.createDataFrame([("AA", date(2015, 1, 1)), ("AA", date(2015, 1, 10))],
                                    "airline string, flight_date date")
    rows = {r["flight_date"]: r for r in add_history_features(flights, history, ["airline"], "airline").collect()}

    assert rows[date(2015, 1, 1)]["airline_flights_30d"] == 0          # sin días previos
    assert rows[date(2015, 1, 1)]["airline_delay_rate_30d"] is None
    assert rows[date(2015, 1, 10)]["airline_flights_30d"] == 1         # solo el 1/1, no el mismo día
    assert rows[date(2015, 1, 10)]["airline_delay_rate_30d"] == 1.0


def test_split_is_temporal(spark):
    df = spark.createDataFrame([(date(2015, 8, 31),), (date(2015, 9, 1),), (date(2015, 11, 1),)], "flight_date date")
    splits = []
    for row in add_split(df, "2015-08-31", "2015-10-31").collect():
        splits.append(row["split"])
    assert splits == ["train", "validation", "test"]


def test_history_forgets_flights_older_than_30_days(spark):
    history = spark.createDataFrame([("AA", date(2015, 1, 1), 60), ("AA", date(2015, 1, 10), 0),
                                     ("AA", date(2015, 2, 5), 0)],
                                    "airline string, flight_date date, arrival_delay int")
    flights = spark.createDataFrame([("AA", date(2015, 2, 5))], "airline string, flight_date date")
    row = add_history_features(flights, history, ["airline"], "airline").first()
    assert row["airline_flights_30d"] == 1         # el 1/1 quedó fuera de la ventana, el 10/1 no
    assert row["airline_delay_rate_30d"] == 0.0


def test_history_skips_the_previous_day(spark):
    # Un vuelo demorado el 19/8: el vuelo del 20/8 no lo ve, porque podría seguir en el aire
    history = spark.createDataFrame([("AA", date(2015, 8, 19), 60), ("AA", date(2015, 8, 20), 0)],
                                    "airline string, flight_date date, arrival_delay int")
    flights = spark.createDataFrame([("AA", date(2015, 8, 20))], "airline string, flight_date date")
    row = add_history_features(flights, history, ["airline"], "airline").first()
    assert row["airline_flights_30d"] == 0