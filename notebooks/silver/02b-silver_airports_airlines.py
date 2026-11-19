# Databricks notebook source
# MAGIC %pip install timezonefinder

# COMMAND ----------

dbutils.widgets.text("catalog", "vuelos_dev")
dbutils.widgets.text("batch_id", "manual")
catalog = dbutils.widgets.get("catalog")
batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

from pyspark.sql import functions as F
from src.silver.airports import clean_airports, add_timezone
from src.silver.airlines import clean_airlines
from src.common.quality import save_check

# Aeropuertos: limpiamos y agregamos la zona horaria de cada uno
airports = spark.table(f"{catalog}.bronze.airports_raw")
airports = clean_airports(airports)
airports = add_timezone(spark, airports)

# Control de calidad: sin zona horaria no se pueden pasar los vuelos a UTC
without_timezone = airports.filter(F.col("timezone").isNull()).count()
save_check(spark, catalog, "airports", "sin_zona_horaria", without_timezone, True, batch_id)

repeated_airports = airports.count() - airports.select("iata_code").distinct().count()
save_check(spark, catalog, "airports", "codigos_repetidos", repeated_airports, True, batch_id)

if without_timezone > 0 or repeated_airports > 0:
    raise RuntimeError("Aeropuertos sin zona horaria o con códigos repetidos")

airports.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.airports")

# COMMAND ----------

# Aerolíneas
airlines = spark.table(f"{catalog}.bronze.airlines_raw")
airlines = clean_airlines(airlines)

repeated_airlines = airlines.count() - airlines.select("iata_code").distinct().count()
save_check(spark, catalog, "airlines", "codigos_repetidos", repeated_airlines, True, batch_id)
if repeated_airlines > 0:
    raise RuntimeError("Aerolíneas con códigos repetidos")

airlines.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.airlines")
