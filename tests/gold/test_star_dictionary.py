from datetime import datetime

from src.gold.star_dictionary import STAR_DICTIONARY
from src.gold.fact_flights import FACT_COLUMNS
from src.gold.dimensions import build_dim_date, build_dim_airline, build_dim_airport, build_dim_weather


def described_columns(table_name):
    """Los nombres de las columnas descriptas en el diccionario, en orden."""
    names = []
    for column, description in STAR_DICTIONARY[table_name][1]:
        names.append(column)
    return names


def test_fact_columns_are_all_described():
    assert described_columns("fact_flights") == FACT_COLUMNS


def test_dimension_columns_are_all_described(spark):
    holidays = spark.createDataFrame([], "holiday_date date, holiday_name string, is_national boolean")
    airlines = spark.createDataFrame([("AA", "American Airlines Inc.")], "iata_code string, airline_name string")
    airports = spark.createDataFrame([("ATL", "Atlanta Intl", "Atlanta", "GA", 33.64, -84.43, "America/New_York")],
                                     "iata_code string, airport_name string, city string, state string, "
                                     "latitude double, longitude double, timezone string")
    weather = spark.createDataFrame([("ATL", datetime(2015, 1, 15, 19, 0), 5.0, 0.0, 0.0, 10.0, 15.0, 20.0)],
                                    "iata_code string, weather_hour_utc timestamp_ntz, temperature_2m double, "
                                    "precipitation double, snowfall double, wind_speed_10m double, "
                                    "wind_gusts_10m double, cloud_cover double")

    assert build_dim_date(spark, holidays, "2015-01-01", "2015-12-31").columns == described_columns("dim_date")
    assert build_dim_airline(airlines).columns == described_columns("dim_airline")
    assert build_dim_airport(airports, weather).columns == described_columns("dim_airport")
    assert build_dim_weather(spark, weather).columns == described_columns("dim_weather")