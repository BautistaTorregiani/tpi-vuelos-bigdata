# Databricks notebook source
# MAGIC %pip install timezonefinder

# COMMAND ----------

dbutils.widgets.text("catalog", "vuelos_dev")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

from pyspark.sql import functions as F
from src.silver.airports import clean_airports, add_timezone
from src.silver.airlines import clean_airlines

# Aeropuertos: limpiamos y agregamos la zona horaria de cada uno
airports = spark.table(f"{catalog}.bronze.airports_raw")
airports = clean_airports(airports)
airports = add_timezone(spark, airports)

# Control de calidad: sin zona horaria no se pueden pasar los vuelos a UTC
without_timezone = airports.filter(F.col("timezone").isNull()).count()
if without_timezone > 0:
    raise RuntimeError(f"Hay {without_timezone} aeropuertos sin zona horaria")

airports.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.airports")

# COMMAND ----------

# Aerolíneas
airlines = spark.table(f"{catalog}.bronze.airlines_raw")
airlines = clean_airlines(airlines)

airlines.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.airlines")
