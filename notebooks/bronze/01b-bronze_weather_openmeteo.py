# Databricks notebook source

# COMMAND ----------

dbutils.widgets.text("catalog", "vuelos_dev")
dbutils.widgets.text("start_date", "2015-01-01")
dbutils.widgets.text("end_date", "2015-12-31")
dbutils.widgets.text("top_n", "50")
dbutils.widgets.text("weather_variables",
    "temperature_2m,precipitation,snowfall,wind_speed_10m,wind_gusts_10m,cloud_cover")
dbutils.widgets.text("batch_id", "manual")

catalog = dbutils.widgets.get("catalog")
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")
top_n = int(dbutils.widgets.get("top_n"))
variables = dbutils.widgets.get("weather_variables").split(",")
batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

from pyspark.sql import functions as F
from src.bronze.weather_ingestion import select_top_airports, fetch_weather_for_airports, estimate_api_cost
from src.common.save_tables import save_without_duplicates, save_errors

# Elegimos los aeropuertos
flights = spark.table(f"{catalog}.bronze.flights_raw")
airports = spark.table(f"{catalog}.bronze.airports_raw")
top_airports = select_top_airports(flights, airports, top_n).collect()

# Calculamos el costo en la API antes de llamarla (fórmula del blog de Open-Meteo)
cost = estimate_api_cost(len(top_airports), start_date, end_date, len(variables))
print(f"Costo estimado: {cost:.0f} llamadas de las 10.000 diarias")
if cost > 5000:
    raise RuntimeError("El costo estimado supera la mitad del límite diario de la API")

# Pedimos el clima de cada aeropuerto
results, errors = fetch_weather_for_airports(top_airports, start_date, end_date, variables)
print(f"Aeropuertos OK: {len(results)} | Con error: {len(errors)}")

# COMMAND ----------

# Guardamos el clima sin duplicar y los errores en el historial
new_weather = spark.createDataFrame(
    results, "iata_code string, start_date string, end_date string, source_url string, raw_json string")
new_weather = new_weather.withColumn("ingestion_ts", F.current_timestamp())
new_weather = new_weather.withColumn("batch_id", F.lit(batch_id))

save_without_duplicates(spark, new_weather, f"{catalog}.bronze.weather_raw",
                        keys=["iata_code", "start_date", "end_date"])
save_errors(spark, errors, f"{catalog}.bronze.ingestion_errors", batch_id)

# Si no se pudo traer el clima de ningún aeropuerto, algo grave pasó: la tarea falla
if len(results) == 0:
    raise RuntimeError("No se pudo obtener el clima de ningún aeropuerto")