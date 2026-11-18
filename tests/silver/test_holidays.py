import json
from datetime import date, datetime

from src.silver.holidays import explode_holidays, merge_duplicate_holidays


def holiday(day, name, is_global, counties=None, types=("Public",)):
    return {"date": day, "localName": name, "name": name, "countryCode": "US", "fixed": False,
            "global": is_global, "counties": counties, "launchYear": None, "types": list(types)}


def make_raw(spark, loads):
    """loads: lista de (lista de feriados, ingestion_ts)."""
    data = [(2015, "US", json.dumps(h), ts, f"b{i}") for i, (h, ts) in enumerate(loads)]
    return spark.createDataFrame(
        data, "year int, country_code string, raw_json string, ingestion_ts timestamp, batch_id string")


def silver(spark, loads):
    return merge_duplicate_holidays(explode_holidays(make_raw(spark, loads)))


def by_name(rows):
    return {r["holiday_name"]: r for r in rows}


def test_one_row_per_holiday_with_date_type(spark):
    rows = explode_holidays(make_raw(spark, [([
        holiday("2015-07-03", "Independence Day", True),
        holiday("2015-12-25", "Christmas Day", True),
    ], datetime(2026, 1, 1))])).collect()

    assert len(rows) == 2
    assert by_name(rows)["Independence Day"]["holiday_date"] == date(2015, 7, 3)


def test_state_prefix_is_removed(spark):
    rows = explode_holidays(make_raw(spark, [([
        holiday("2015-05-08", "Truman Day", False, ["US-MO"], ["School"]),
    ], datetime(2026, 1, 1))])).collect()
    assert rows[0]["states"] == ["MO"]


def test_regional_duplicates_are_merged(spark):
    # Good Friday viene dos veces: Public en unos estados, Optional en Texas
    rows = silver(spark, [([
        holiday("2015-04-03", "Good Friday", False, ["US-CT", "US-DE"], ["Public"]),
        holiday("2015-04-03", "Good Friday", False, ["US-TX"], ["Optional"]),
    ], datetime(2026, 1, 1))]).collect()

    assert len(rows) == 1
    assert rows[0]["is_national"] is False
    assert rows[0]["states"] == ["CT", "DE", "TX"]
    assert rows[0]["types"] == ["Optional", "Public"]


def test_national_wins_and_has_no_states(spark):
    # Columbus Day viene regional (Public) y nacional (Bank)
    rows = silver(spark, [([
        holiday("2015-10-12", "Columbus Day", False, ["US-AL", "US-NY"], ["Public"]),
        holiday("2015-10-12", "Columbus Day", True, None, ["Bank"]),
        holiday("2015-10-12", "Indigenous Peoples' Day", False, ["US-AK"], ["Public"]),
    ], datetime(2026, 1, 1))]).collect()

    rows = by_name(rows)
    assert len(rows) == 2
    assert rows["Columbus Day"]["is_national"] is True
    assert rows["Columbus Day"]["states"] == []
    assert rows["Indigenous Peoples' Day"]["states"] == ["AK"]


def test_only_latest_load_is_kept(spark):
    rows = silver(spark, [
        ([holiday("2015-12-25", "Christmas Day", True, types=["Public"])], datetime(2026, 1, 1)),
        ([holiday("2015-12-25", "Christmas Day", True, types=["Public", "Bank"])], datetime(2026, 2, 1)),
    ]).collect()

    assert len(rows) == 1
    assert rows[0]["types"] == ["Bank", "Public"]
    assert rows[0]["batch_id"] == "b1"
