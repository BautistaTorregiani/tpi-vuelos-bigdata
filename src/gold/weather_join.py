from pyspark.sql import functions as F


def build_weather_key(airport_column, hour_column):
    """Clave del clima de un aeropuerto en una hora UTC. La usan dim_weather y fact_flights."""
    return F.xxhash64(F.col(airport_column), F.col(hour_column))


def add_departure_weather(flights, weather, variables, how="left"):
    """Agrega a cada vuelo el clima de su aeropuerto de origen en la hora de salida.
    has_weather indica si se encontró clima para ese vuelo."""
    columns = ["iata_code", "weather_hour_utc"]
    for variable in variables:
        columns.append(variable)
    weather_at_departure = weather.select(columns).withColumn("has_weather", F.lit(True))

    same_airport = flights["origin_airport"] == weather_at_departure["iata_code"]
    same_hour = flights["departure_hour_utc"] == weather_at_departure["weather_hour_utc"]
    joined = flights.join(weather_at_departure, same_airport & same_hour, how)

    joined = joined.withColumn("has_weather", F.coalesce(F.col("has_weather"), F.lit(False)))
    return joined.drop("iata_code", "weather_hour_utc")

WEATHER_COLUMN_NAMES = {
    "temperature_2m": "temperature_c",
    "precipitation": "precipitation_mm",
    "snowfall": "snowfall_cm",
    "wind_speed_10m": "wind_speed_kmh",
    "wind_gusts_10m": "wind_gusts_kmh",
    "cloud_cover": "cloud_cover_pct",
}


def rename_weather_columns(df):
    """Renombra las variables de clima de Silver con la unidad en el nombre, como en dim_weather."""
    for silver_name, gold_name in WEATHER_COLUMN_NAMES.items():
        df = df.withColumnRenamed(silver_name, gold_name)
    return df