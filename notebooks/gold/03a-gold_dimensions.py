# Databricks notebook source

dbutils.widgets.text("catalog", "vuelos_dev")
dbutils.widgets.text("batch_id", "manual")
catalog = dbutils.widgets.get("catalog")
batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

from src.gold.dimensions import build_dim_date, build_dim_airline, build_dim_airport, build_dim_weather
from src.common.quality import build_check, save_checks, fail_if_critical

holidays = spark.table(f"{catalog}.silver.holidays")
airlines = spark.table(f"{catalog}.silver.airlines")
airports = spark.table(f"{catalog}.silver.airports")
weather = spark.table(f"{catalog}.silver.weather_hourly")

dim_date = build_dim_date(spark, holidays)
dim_airline = build_dim_airline(airlines)
dim_airport = build_dim_airport(airports, weather)
dim_weather = build_dim_weather(spark, weather)

# COMMAND ----------

# Controles de calidad, antes de guardar
total_dates = dim_date.count()
total_airlines = dim_airline.count()
total_airports = dim_airport.count()
airports_with_weather = dim_airport.filter("has_weather").count()
total_weather = dim_weather.count()
unknown_rows = dim_weather.filter("weather_key = -1").count()

checks = [
    build_check("dim_date", "dias_faltantes", 365 - total_dates, 365, True),
    build_check("dim_date", "claves_repetidas",
                total_dates - dim_date.select("date_key").distinct().count(), total_dates, True),
    build_check("dim_airline", "claves_repetidas",
                total_airlines - dim_airline.select("airline_key").distinct().count(), total_airlines, True),
    build_check("dim_airport", "claves_repetidas",
                total_airports - dim_airport.select("airport_key").distinct().count(), total_airports, True),
    build_check("dim_airport", "aeropuertos_con_clima_distinto_de_50",
                abs(50 - airports_with_weather), total_airports, True),
    build_check("dim_weather", "claves_repetidas",
                total_weather - dim_weather.select("weather_key").distinct().count(), total_weather, True),
    build_check("dim_weather", "fila_sin_dato_distinta_de_1", abs(1 - unknown_rows), total_weather, True),
]
save_checks(spark, catalog, checks, batch_id)
fail_if_critical(checks)

dim_date.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.dim_date")
dim_airline.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.dim_airline")
dim_airport.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.dim_airport")
dim_weather.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.dim_weather")