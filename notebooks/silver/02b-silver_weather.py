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
from src.common.quality import build_check, save_checks, fail_if_critical

raw = spark.table(f"{catalog}.bronze.weather_raw")
hourly = explode_hourly_weather(raw, variables)
weather = deduplicate_weather(hourly)

# COMMAND ----------

# Controles de calidad, antes de guardar
total = weather.count()
repeated_hours = count_repeated_hours(hourly).count()

# Si a un aeropuerto le falta alguna variable en el JSON, explode no genera filas y desaparece sin aviso
airports_bronze = raw.select("iata_code").distinct().count()
airports_silver = weather.select("iata_code").distinct().count()

checks = [
    build_check("weather_hourly", "tabla_vacia", 1 if total == 0 else 0, total, True),
    build_check("weather_hourly", "horas_repetidas", repeated_hours, total, False),
    build_check("weather_hourly", "aeropuertos_faltantes", airports_bronze - airports_silver,
                airports_bronze, True),
]
save_checks(spark, catalog, checks, batch_id)
fail_if_critical(checks)

# Se reconstruye entera desde bronze: re-ejecutar no duplica
weather.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.weather_hourly")
