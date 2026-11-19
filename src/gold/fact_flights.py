from pyspark.sql import functions as F

from src.gold.weather_join import build_weather_key, add_departure_weather


def add_fact_keys(flights, weather):
    """Agrega las claves hacia cada dimensión. Los vuelos sin clima apuntan a la fila -1 de dim_weather."""
    flights = add_departure_weather(flights, weather, [])
    flights = flights.withColumn("weather_key",
                                 F.when(F.col("has_weather"),
                                        build_weather_key("origin_airport", "departure_hour_utc"))
                                  .otherwise(F.lit(-1)))
    flights = flights.withColumn("date_key", F.date_format("flight_date", "yyyyMMdd").cast("int"))
    flights = flights.withColumnRenamed("airline", "airline_key")
    flights = flights.withColumnRenamed("origin_airport", "origin_airport_key")
    return flights.withColumnRenamed("destination_airport", "destination_airport_key")


def add_delay_indicators(flights):
    """Indicadores de retraso de 15 minutos o más. Quedan vacíos en los vuelos cancelados o desviados."""
    not_completed = F.col("is_cancelled") | F.col("is_diverted")
    flights = flights.withColumn("is_departure_delayed_15",
                                 F.when(F.col("is_cancelled"), None)
                                  .otherwise(F.col("departure_delay") >= 15))
    return flights.withColumn("is_arrival_delayed_15",
                              F.when(not_completed, None)
                               .otherwise(F.col("arrival_delay") >= 15))


FACT_COLUMNS = [
    "flight_id", "date_key", "airline_key", "origin_airport_key", "destination_airport_key", "weather_key",
    "flight_number", "tail_number",
    "departure_planned_local", "departure_planned_utc", "departure_real_local", "departure_real_utc",
    "arrival_planned_local", "arrival_planned_utc", "arrival_real_local", "arrival_real_utc",
    "distance", "planned_duration", "real_duration", "air_time", "taxi_out", "taxi_in",
    "departure_delay", "arrival_delay",
    "delay_air_system", "delay_security", "delay_airline", "delay_late_aircraft", "delay_weather",
    "is_cancelled", "is_diverted", "cancellation_reason",
    "is_departure_delayed_15", "is_arrival_delayed_15",
]


def build_fact_flights(flights, weather):
    """Una fila por vuelo programado, incluidos los cancelados y desviados."""
    fact = add_fact_keys(flights, weather)
    fact = add_delay_indicators(fact)
    return fact.select(FACT_COLUMNS)


def count_orphans(fact, fact_column, dim, dim_column):
    """Cantidad de filas de la fact cuya clave no existe en la dimensión. Tiene que dar 0."""
    orphans = fact.join(dim, fact[fact_column] == dim[dim_column], "left_anti")
    return orphans.count()