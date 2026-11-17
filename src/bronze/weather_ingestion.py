import time
from urllib.parse import urlencode
from datetime import date

from pyspark.sql import DataFrame, functions as F
from src.common.api_requests import fetch_json

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"


def build_weather_url(lat: float, lon: float, start_date: str, end_date: str,
                      variables: list[str]) -> str:
    """Arma la URL de la API de Open-Meteo para un aeropuerto."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(variables),
        "timezone": "auto",
    }
    return f"{BASE_URL}?{urlencode(params)}"


def select_top_airports(flights: DataFrame, airports: DataFrame, n: int = 50) -> DataFrame:
    """Los n aeropuertos de origen con más vuelos, con sus coordenadas.
    Solo cuenta códigos IATA de 3 letras (excluye los numéricos de octubre)."""
    counts = (flights
              .filter(F.col("ORIGIN_AIRPORT").rlike("^[A-Z]{3}$"))
              .groupBy(F.col("ORIGIN_AIRPORT").alias("IATA_CODE"))
              .agg(F.count("*").alias("n_flights")))
    coords = airports.select("IATA_CODE", "LATITUDE", "LONGITUDE")
    return (counts
            .join(coords, "IATA_CODE")
            .filter(F.col("LATITUDE").isNotNull() & F.col("LONGITUDE").isNotNull())
            .orderBy(F.desc("n_flights"), "IATA_CODE")
            .limit(n))


def fetch_weather_for_airports(airports, start_date: str, end_date: str,
                               variables: list[str], fetch=fetch_json,
                               sleep=time.sleep, pause_seconds: float = 2):
    """Pide el clima de cada aeropuerto a la API.
    Devuelve dos listas: las respuestas que salieron bien y los errores."""
    results = []
    errors = []

    for airport in airports:
        url = build_weather_url(float(airport["LATITUDE"]), float(airport["LONGITUDE"]),
                                start_date, end_date, variables)
        try:
            results.append({
                "iata_code": airport["IATA_CODE"],
                "start_date": start_date,
                "end_date": end_date,
                "source_url": url,
                "raw_json": fetch(url),
            })
        except Exception as error:
            errors.append({
                "source": "open-meteo",
                "entity": airport["IATA_CODE"],
                "source_url": url,
                "error_message": str(error),
            })
        sleep(pause_seconds)   # pausa entre llamadas para no superar el límite por minuto

    return results, errors


def estimate_api_cost(n_airports: int, start_date: str, end_date: str, n_variables: int) -> float:
    """Peso estimado de la consulta en la API de Open-Meteo (fórmula del blog, 2023)."""
    n_days = (date.fromisoformat(end_date) - date.fromisoformat(start_date)).days + 1
    return n_airports * (n_days / 14) * (n_variables / 10)