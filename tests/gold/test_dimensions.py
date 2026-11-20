from datetime import date, datetime

from src.gold.dimensions import build_dim_date, build_dim_airline, build_dim_airport, build_dim_weather

# Dimensión de fecha

def make_holidays(spark, rows):
    """Feriados de prueba: (fecha, nombre, es nacional)."""
    return spark.createDataFrame(rows, "holiday_date date, holiday_name string, is_national boolean")


def test_dim_date_has_one_row_per_day(spark):
    dim = build_dim_date(spark, make_holidays(spark, []), "2015-01-01", "2015-12-31")
    assert dim.count() == 365
    assert dim.select("date_key").distinct().count() == 365


def test_dim_date_calendar_columns(spark):
    dim = build_dim_date(spark, make_holidays(spark, []), "2015-01-01", "2015-12-31")
    row = dim.filter("date = '2015-08-29'").first()     # un sábado
    assert row["date_key"] == 20150829
    assert row["quarter"] == 3
    assert row["day_of_week"] == 6
    assert row["is_weekend"] is True


def test_monday_is_day_one(spark):
    dim = build_dim_date(spark, make_holidays(spark, []), "2015-01-01", "2015-12-31")
    row = dim.filter("date = '2015-01-05'").first()     # un lunes
    assert row["day_of_week"] == 1
    assert row["is_weekend"] is False


def test_only_national_holidays_are_marked(spark):
    holidays = make_holidays(spark, [(date(2015, 7, 4), "Independence Day", True),
                                     (date(2015, 3, 2), "Texas Independence Day", False)])
    dim = build_dim_date(spark, holidays, "2015-01-01", "2015-12-31")

    july_4 = dim.filter("date = '2015-07-04'").first()
    march_2 = dim.filter("date = '2015-03-02'").first()
    assert july_4["is_holiday"] is True
    assert july_4["holiday_name"] == "Independence Day"
    assert march_2["is_holiday"] is False


# Dimensiones de aerolínea y aeropuerto

def test_dim_airline_uses_code_as_key(spark):
    airlines = spark.createDataFrame([("AA", "American Airlines Inc.")],
                                     "iata_code string, airline_name string")
    row = build_dim_airline(airlines).first()
    assert row["airline_key"] == "AA"
    assert row["airline_name"] == "American Airlines Inc."


def test_dim_airport_marks_airports_with_weather(spark):
    airports = spark.createDataFrame(
        [("ATL", "Atlanta Intl", "Atlanta", "GA", 33.64, -84.43, "America/New_York"),
         ("ABQ", "Albuquerque Intl", "Albuquerque", "NM", 35.04, -106.61, "America/Denver")],
        "iata_code string, airport_name string, city string, state string, "
        "latitude double, longitude double, timezone string")
    weather = spark.createDataFrame([("ATL",), ("ATL",)], "iata_code string")   # varias horas de ATL

    rows = build_dim_airport(airports, weather).orderBy("airport_key").collect()
    assert len(rows) == 2                      # ATL no se duplica por tener varias horas
    assert rows[0]["airport_key"] == "ABQ"
    assert rows[0]["has_weather"] is False
    assert rows[1]["has_weather"] is True


# Dimensión de clima

def make_weather_hours(spark, rows):
    """Clima de prueba: (aeropuerto, hora, temperatura, precipitación, nieve, viento, ráfagas, nubes)."""
    return spark.createDataFrame(rows, "iata_code string, weather_hour_utc timestamp_ntz, "
                                       "temperature_2m double, precipitation double, snowfall double, "
                                       "wind_speed_10m double, wind_gusts_10m double, cloud_cover double")


def test_dim_weather_categories(spark):
    hour = datetime(2015, 1, 15, 19, 0)
    weather = make_weather_hours(spark, [
        ("ATL", hour, 5.0, 0.0, 0.0, 10.0, 15.0, 20.0),     # sin precipitación, calmo
        ("ORD", hour, -2.0, 1.0, 0.5, 45.0, 60.0, 90.0),    # nieve, fuerte
        ("MIA", hour, 25.0, 9.0, 0.0, 70.0, 90.0, 100.0),   # lluvia fuerte, temporal
    ])
    rows = build_dim_weather(spark, weather).filter("weather_key != -1").orderBy("iata_code").collect()

    assert rows[0]["iata_code"] == "ATL"
    assert rows[0]["precipitation_category"] == "Sin precipitación"
    assert rows[0]["wind_category"] == "Calmo o suave"
    assert rows[1]["iata_code"] == "MIA"
    assert rows[1]["precipitation_category"] == "Fuerte"
    assert rows[1]["wind_category"] == "Temporal"
    assert rows[2]["iata_code"] == "ORD"
    assert rows[2]["precipitation_category"] == "Nieve"
    assert rows[2]["wind_category"] == "Fuerte"


def test_dim_weather_keeps_values_and_renames_columns(spark):
    weather = make_weather_hours(spark, [("ATL", datetime(2015, 1, 15, 19, 0), 5.0, 1.2, 0.0, 22.0, 35.0, 80.0)])
    row = build_dim_weather(spark, weather).filter("weather_key != -1").first()

    assert row["temperature_c"] == 5.0
    assert row["precipitation_mm"] == 1.2
    assert row["wind_speed_kmh"] == 22.0
    assert row["precipitation_category"] == "Leve"
    assert row["wind_category"] == "Moderado"


def test_missing_measurement_is_sin_dato_but_keeps_its_key(spark):
    weather = make_weather_hours(spark, [("ATL", datetime(2015, 1, 15, 19, 0), 5.0, None, None, None, None, 80.0)])
    row = build_dim_weather(spark, weather).filter("weather_key != -1").first()

    assert row["iata_code"] == "ATL"
    assert row["precipitation_category"] == "Sin dato"
    assert row["wind_category"] == "Sin dato"


def test_dim_weather_has_unknown_row(spark):
    weather = make_weather_hours(spark, [("ATL", datetime(2015, 1, 15, 19, 0), 5.0, 0.0, 0.0, 10.0, 15.0, 20.0)])
    dim = build_dim_weather(spark, weather)

    unknown = dim.filter("weather_key = -1").collect()
    assert dim.count() == 2
    assert len(unknown) == 1
    assert unknown[0]["iata_code"] is None
    assert unknown[0]["temperature_c"] is None
    assert unknown[0]["precipitation_category"] == "Sin dato"
    assert unknown[0]["wind_category"] == "Sin dato"