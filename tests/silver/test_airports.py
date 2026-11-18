from src.silver.airports import find_timezones, add_timezone


def make_airports(spark, rows):
    """Crea una tabla de aeropuertos de prueba ya limpia: código, latitud, longitud."""
    return spark.createDataFrame(rows, "iata_code string, latitude double, longitude double")


def test_timezone_from_coordinates(spark):
    airports = make_airports(spark, [("ATL", 33.64, -84.43)])
    assert find_timezones(airports) == {"ATL": "America/New_York"}


def test_airport_without_coordinates_uses_manual_timezone(spark):
    airports = make_airports(spark, [("ECP", None, None)])
    assert find_timezones(airports) == {"ECP": "America/Chicago"}


def test_add_timezone_adds_column(spark):
    airports = make_airports(spark, [("LAX", 33.94, -118.41)])
    row = add_timezone(spark, airports).first()
    assert row["timezone"] == "America/Los_Angeles"