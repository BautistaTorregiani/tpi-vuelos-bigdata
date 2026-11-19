# Databricks notebook source

dbutils.widgets.text("catalog", "vuelos_dev")
dbutils.widgets.text("weather_variables",
    "temperature_2m,precipitation,snowfall,wind_speed_10m,wind_gusts_10m,cloud_cover")

dbutils.widgets.text("batch_id", "manual")
catalog = dbutils.widgets.get("catalog")
batch_id = dbutils.widgets.get("batch_id")
variables = dbutils.widgets.get("weather_variables").split(",")

# COMMAND ----------

from src.silver.weather import explode_hourly_weather, deduplicate_weather, count_repeated_hours
from src.common.quality import save_check

raw = spark.table(f"{catalog}.bronze.weather_raw")
hourly = explode_hourly_weather(raw, variables)

repeated = count_repeated_hours(hourly)
print(f"Horas repetidas por aeropuerto: {repeated.count()}")
save_check(spark, catalog, "weather_hourly", "horas_repetidas", repeated.count(), False, batch_id)
display(repeated.orderBy("iata_code", "weather_hour_utc").limit(20))

weather = deduplicate_weather(hourly)

# COMMAND ----------

# Se reconstruye entera desde bronze: re-ejecutar no duplica
weather.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.silver.weather_hourly")

result = spark.table(f"{catalog}.silver.weather_hourly")
print(f"weather_hourly: {result.count():,} filas")
display(result.groupBy("iata_code").count().orderBy("count"))  # ~8.760 por aeropuerto

if result.count() == 0:
    raise RuntimeError("silver.weather_hourly quedó vacía")

# Si a un aeropuerto le falta alguna variable en el JSON, explode no genera filas y desaparece sin aviso
airports_bronze = raw.select("iata_code").distinct().count()
airports_silver = result.select("iata_code").distinct().count()
missing_airports = airports_bronze - airports_silver
save_check(spark, catalog, "weather_hourly", "aeropuertos_faltantes", missing_airports, True, batch_id)
if missing_airports > 0:
    raise RuntimeError(f"Faltan aeropuertos: {airports_bronze} en Bronze y {airports_silver} en Silver")
