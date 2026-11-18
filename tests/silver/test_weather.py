import json
from datetime import datetime

from src.silver.weather import (build_weather_schema, explode_hourly_weather,
                                deduplicate_weather, count_repeated_hours)

VARIABLES = ["temperature_2m", "precipitation"]


def make_raw(spark, rows, utc_offset_seconds=-18000):
    """rows: (iata_code, horas, temperaturas, precipitaciones, ingestion_ts)."""
    data = []
    for code, times, temps, precs, ts in rows:
        raw = json.dumps({"timezone": "America/New_York",
                          "utc_offset_seconds": utc_offset_seconds,
                          "hourly": {"time": times, "temperature_2m": temps, "precipitation": precs}})
        data.append((code, raw, ts, "b1"))
    return spark.createDataFrame(
        data, "iata_code string, raw_json string, ingestion_ts timestamp, batch_id string")


def test_schema_includes_every_variable():
    schema = build_weather_schema(VARIABLES)
    assert "temperature_2m: array<double>" in schema
    assert "precipitation: array<double>" in schema
    assert "utc_offset_seconds int" in schema


def test_one_row_per_hour(spark):
    raw = make_raw(spark, [("ATL",
                            ["2015-01-01T00:00", "2015-01-01T01:00", "2015-01-01T02:00"],
                            [3.1, 2.8, 2.5], [0.0, 0.2, 0.0],
                            datetime(2026, 1, 1))])
    result = explode_hourly_weather(raw, VARIABLES).orderBy("weather_hour_utc").collect()

    assert len(result) == 3
    assert result[1]["temperature_2m"] == 2.8
    assert result[1]["precipitation"] == 0.2
    assert result[0]["timezone"] == "America/New_York"


def test_hour_is_converted_to_utc(spark):
    # Open-Meteo usa UTC-5 fijo para Atlanta, también en julio
    raw = make_raw(spark, [("ATL", ["2015-07-01T12:00"], [28.1], [0.0], datetime(2026, 1, 1))])
    row = explode_hourly_weather(raw, VARIABLES).collect()[0]
    assert row["weather_hour_utc"] == datetime(2015, 7, 1, 17, 0)


def test_utc_conversion_crosses_midnight(spark):
    raw = make_raw(spark, [("ATL", ["2015-12-31T22:00"], [5.0], [0.0], datetime(2026, 1, 1))])
    row = explode_hourly_weather(raw, VARIABLES).collect()[0]
    assert row["weather_hour_utc"] == datetime(2016, 1, 1, 3, 0)


def test_null_values_are_kept(spark):
    raw = make_raw(spark, [("ATL", ["2015-01-01T00:00"], [None], [0.0], datetime(2026, 1, 1))])
    row = explode_hourly_weather(raw, VARIABLES).collect()[0]
    assert row["temperature_2m"] is None


def test_deduplicate_keeps_latest_load(spark):
    raw = make_raw(spark, [
        ("ATL", ["2015-01-01T00:00"], [1.0], [0.0], datetime(2026, 1, 1)),
        ("ATL", ["2015-01-01T00:00"], [9.0], [0.0], datetime(2026, 2, 1)),
    ])
    hourly = explode_hourly_weather(raw, VARIABLES)

    assert count_repeated_hours(hourly).count() == 1
    result = deduplicate_weather(hourly).collect()
    assert len(result) == 1
    assert result[0]["temperature_2m"] == 9.0