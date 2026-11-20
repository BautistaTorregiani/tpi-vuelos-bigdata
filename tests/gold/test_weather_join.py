from datetime import datetime

from src.gold.weather_join import build_weather_key, add_departure_weather, rename_weather_columns


def make_weather(spark, rows):
    """Clima de prueba: (aeropuerto, hora UTC, temperatura)."""
    return spark.createDataFrame(rows, "iata_code string, weather_hour_utc timestamp_ntz, temperature_2m double")


def make_flights(spark, rows):
    """Vuelos de prueba: (id, aeropuerto de origen, hora de salida en punto UTC)."""
    return spark.createDataFrame(rows, "flight_id int, origin_airport string, departure_hour_utc timestamp_ntz")


# La clave calculada desde el clima y desde el vuelo tiene que ser la misma.
def test_weather_key_is_the_same_for_weather_and_flight(spark):
    hour = datetime(2015, 1, 15, 19, 0)
    weather = make_weather(spark, [("ATL", hour, 5.0)])
    flights = make_flights(spark, [(1, "ATL", hour)])

    weather_key = weather.select(build_weather_key("iata_code", "weather_hour_utc")).first()[0]
    flight_key = flights.select(build_weather_key("origin_airport", "departure_hour_utc")).first()[0]
    assert weather_key == flight_key


def test_weather_key_changes_with_airport_or_hour(spark):
    weather = make_weather(spark, [("ATL", datetime(2015, 1, 15, 19, 0), 5.0),
                                   ("ORD", datetime(2015, 1, 15, 19, 0), 5.0),
                                   ("ATL", datetime(2015, 1, 15, 20, 0), 5.0)])
    keys = weather.select(build_weather_key("iata_code", "weather_hour_utc")).distinct().count()
    assert keys == 3


def test_flight_gets_weather_of_its_departure_hour(spark):
    weather = make_weather(spark, [("ATL", datetime(2015, 1, 15, 19, 0), 5.0),
                                   ("ATL", datetime(2015, 1, 15, 20, 0), 3.0)])
    flights = make_flights(spark, [(1, "ATL", datetime(2015, 1, 15, 19, 0))])

    row = add_departure_weather(flights, weather, ["temperature_2m"]).first()
    assert row["temperature_2m"] == 5.0     # el de las 19:00, no el de las 20:00


def test_inner_join_drops_flights_without_weather(spark):
    weather = make_weather(spark, [("ATL", datetime(2015, 1, 15, 19, 0), 5.0)])
    flights = make_flights(spark, [(1, "ATL", datetime(2015, 1, 15, 19, 0)),
                                   (2, "ABQ", datetime(2015, 1, 15, 19, 0))])   # ABQ no tiene clima

    result = add_departure_weather(flights, weather, ["temperature_2m"], how="inner")
    assert result.count() == 1
    assert result.first()["flight_id"] == 1


def test_left_join_keeps_flights_without_weather(spark):
    weather = make_weather(spark, [("ATL", datetime(2015, 1, 15, 19, 0), 5.0)])
    flights = make_flights(spark, [(2, "ABQ", datetime(2015, 1, 15, 19, 0))])

    row = add_departure_weather(flights, weather, ["temperature_2m"], how="left").first()
    assert row["flight_id"] == 2
    assert row["temperature_2m"] is None


def test_has_weather_marks_flights_without_weather(spark):
    weather = make_weather(spark, [("ATL", datetime(2015, 1, 15, 19, 0), 5.0)])
    flights = make_flights(spark, [(1, "ATL", datetime(2015, 1, 15, 19, 0)),
                                   (2, "ABQ", datetime(2015, 1, 15, 19, 0))])

    rows = {r["flight_id"]: r for r in add_departure_weather(flights, weather, ["temperature_2m"]).collect()}
    assert len(rows) == 2                     # por defecto no se pierde ningún vuelo
    assert rows[1]["has_weather"] is True
    assert rows[2]["has_weather"] is False


def test_weather_columns_get_gold_names(spark):
    df = spark.createDataFrame([(5.0, 1.2)], "temperature_2m double, precipitation double")
    row = rename_weather_columns(df).first()
    assert row["temperature_c"] == 5.0
    assert row["precipitation_mm"] == 1.2