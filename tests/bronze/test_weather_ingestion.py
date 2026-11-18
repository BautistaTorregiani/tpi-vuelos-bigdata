from src.bronze.weather_ingestion import (
    build_weather_url,
    select_top_airports,
    fetch_weather_for_airports,
    estimate_api_cost
)


def no_wait(seconds):
    """Reemplaza la pausa real para que el test sea rápido."""
    pass


def fake_fetch(url):
    """API falsa: falla si le piden el clima de ORD y responde bien para cualquier otro."""
    if "41.98" in url:                  # 41.98 es la latitud de ORD
        raise RuntimeError("API caída")
    return "datos del clima"


def test_build_weather_url_has_required_params():
    url = build_weather_url(33.64, -84.43, "2015-01-01", "2015-12-31",
                            ["temperature_2m", "precipitation"])

    assert "latitude=33.64" in url
    assert "longitude=-84.43" in url
    assert "start_date=2015-01-01" in url
    assert "end_date=2015-12-31" in url
    assert "hourly=temperature_2m%2Cprecipitation" in url
    assert "timezone=auto" in url    # sin esto, las horas vendrían en UTC


def test_select_top_airports_excludes_numeric_codes(spark):
    flights = spark.createDataFrame(
        [("ATL",), ("ATL",), ("ORD",),
         ("10397",), ("10397",), ("10397",)],   # código numérico de octubre
        ["ORIGIN_AIRPORT"])
    airports = spark.createDataFrame(
        [("ATL", "33.64", "-84.43"), ("ORD", "41.98", "-87.90")],
        ["IATA_CODE", "LATITUDE", "LONGITUDE"])

    top = select_top_airports(flights, airports, n=2).collect()

    assert [row["IATA_CODE"] for row in top] == ["ATL", "ORD"]
    assert top[0]["n_flights"] == 2


def test_fetch_weather_separates_results_and_errors():
    airports = [
        {"IATA_CODE": "ATL", "LATITUDE": "33.64", "LONGITUDE": "-84.43"},
        {"IATA_CODE": "ORD", "LATITUDE": "41.98", "LONGITUDE": "-87.90"},
    ]

    results, errors = fetch_weather_for_airports(
        airports, "2015-01-01", "2015-01-31", ["temperature_2m"],
        fetch=fake_fetch, sleep=no_wait)

    # ATL salió bien
    assert len(results) == 1
    assert results[0]["iata_code"] == "ATL"
    assert results[0]["raw_json"] == "datos del clima"

    # ORD falló y quedó registrado con su motivo
    assert len(errors) == 1
    assert errors[0]["entity"] == "ORD"
    assert errors[0]["error_message"] == "API caída"



def test_estimate_api_cost_matches_example_from_consigna():
    # La consigna dice: 1 aeropuerto, 1 año y 6 variables pesa unas 15,6 llamadas
    cost = estimate_api_cost(1, "2015-01-01", "2015-12-31", 6)
    assert round(cost, 1) == 15.6