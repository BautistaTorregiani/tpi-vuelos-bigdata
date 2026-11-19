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
from src.common.quality import build_check, save_checks, fail_if_critical

# Aeropuertos: limpiamos y agregamos la zona horaria de cada uno
airports = spark.table(f"{catalog}.bronze.airports_raw")
airports = clean_airports(airports)
airports = add_timezone(spark, airports)

# Aerolíneas
airlines = spark.table(f"{catalog}.bronze.airlines_raw")
airlines = clean_airlines(airlines)

# COMMAND ----------

# Controles de calidad, antes de guardar
total_airports = airports.count()
without_timezone = airports.filter(F.col("timezone").isNull()).count()
repeated_airports = total_airports - airports.select("iata_code").distinct().count()

total_airlines = airlines.count()
repeated_airlines = total_airlines - airlines.select("iata_code").distinct().count()

checks = [
    build_check("airports", "sin_zona_horaria", without_timezone, total_airports, True),
    build_check("airports", "codigos_repetidos", repeated_airports, total_airports, True),
    build_check("airlines", "codigos_repetidos", repeated_airlines, total_airlines, True),
]
save_checks(spark, catalog, checks, batch_id)
fail_if_critical(checks)

airports.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.airports")
airlines.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.airlines")
