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