from datetime import date, datetime

from src.gold.fact_flights import add_fact_keys, add_delay_indicators, count_orphans
from src.gold.weather_join import build_weather_key


def make_flights(spark, rows):
    """Vuelos de prueba: (id, fecha, aerolínea, origen, destino, hora de salida en punto UTC)."""
    return spark.createDataFrame(rows, "flight_id int, flight_date date, airline string, origin_airport string, "
                                       "destination_airport string, departure_hour_utc timestamp_ntz")


def make_delays(spark, rows):
    """Vuelos de prueba: (id, retraso de salida, retraso de llegada, cancelado, desviado)."""
    return spark.createDataFrame(rows, "flight_id int, departure_delay int, arrival_delay int, "
                                       "is_cancelled boolean, is_diverted boolean")


def test_fact_keys(spark):
    hour = datetime(2015, 1, 15, 19, 0)
    weather = spark.createDataFrame([("ATL", hour)], "iata_code string, weather_hour_utc timestamp_ntz")
    flights = make_flights(spark, [(1, date(2015, 1, 15), "AA", "ATL", "LAX", hour)])

    row = add_fact_keys(flights, weather).first()
    expected_key = weather.select(build_weather_key("iata_code", "weather_hour_utc")).first()[0]
    assert row["date_key"] == 20150115
    assert row["airline_key"] == "AA"
    assert row["origin_airport_key"] == "ATL"
    assert row["destination_airport_key"] == "LAX"
    assert row["weather_key"] == expected_key


def test_flight_without_weather_points_to_unknown_row(spark):
    hour = datetime(2015, 1, 15, 19, 0)
    weather = spark.createDataFrame([("ATL", hour)], "iata_code string, weather_hour_utc timestamp_ntz")
    flights = make_flights(spark, [(1, date(2015, 1, 15), "AA", "ABQ", "LAX", hour)])   # ABQ no tiene clima

    row = add_fact_keys(flights, weather).first()
    assert row["weather_key"] == -1


def test_delay_indicators(spark):
    flights = make_delays(spark, [(1, 20, 30, False, False),     # salió y llegó demorado
                                  (2, 5, 10, False, False),      # a tiempo
                                  (3, None, None, True, False),  # cancelado
                                  (4, 25, None, False, True)])   # desviado: salió, pero no llegó
    rows = add_delay_indicators(flights).orderBy("flight_id").collect()

    assert rows[0]["is_departure_delayed_15"] is True
    assert rows[0]["is_arrival_delayed_15"] is True
    assert rows[1]["is_departure_delayed_15"] is False
    assert rows[1]["is_arrival_delayed_15"] is False
    assert rows[2]["is_departure_delayed_15"] is None
    assert rows[2]["is_arrival_delayed_15"] is None
    assert rows[3]["is_departure_delayed_15"] is True
    assert rows[3]["is_arrival_delayed_15"] is None


def test_count_orphans(spark):
    fact = spark.createDataFrame([("AA",), ("DL",), ("ZZ",)], "airline_key string")
    dim = spark.createDataFrame([("AA",), ("DL",)], "airline_key string")
    assert count_orphans(fact, "airline_key", dim, "airline_key") == 1     # ZZ no existe en la dimensión