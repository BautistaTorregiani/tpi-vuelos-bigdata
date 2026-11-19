# Databricks notebook source

dbutils.widgets.text("catalog", "vuelos_dev")
dbutils.widgets.text("batch_id", "manual")
catalog = dbutils.widgets.get("catalog")
batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

from pyspark.sql import functions as F
from src.silver.flights import (cast_flight_columns, add_flight_date, add_departure_times,
                                build_airport_code_mapping, translate_airport_codes,
                                add_airport_timezones, add_departure_utc,
                                add_arrival_utc, add_arrival_local)
from src.common.quality import save_check

flights = spark.table(f"{catalog}.bronze.flights_raw")
flights = cast_flight_columns(flights)
flights = add_flight_date(flights)
flights = add_departure_times(flights)

# Octubre trae códigos numéricos: los pasamos a 3 letras
codes = spark.table(f"{catalog}.bronze.airport_codes_raw")
mapping = build_airport_code_mapping(codes)
flights = translate_airport_codes(flights, mapping)

# Con la zona horaria de cada aeropuerto pasamos los horarios a UTC
airports = spark.table(f"{catalog}.silver.airports")
flights = add_airport_timezones(flights, airports)
flights = add_departure_utc(flights)
flights = add_arrival_utc(flights)
flights = add_arrival_local(flights)

# COMMAND ----------

# Se reconstruye entera desde bronze: re-ejecutar no duplica
flights.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.silver.flights")

result = spark.table(f"{catalog}.silver.flights")
print(f"flights: {result.count():,} filas")

# COMMAND ----------

# Controles de calidad
numeric_codes = result.filter(F.col("origin_airport").rlike("^[0-9]+$")
                              | F.col("destination_airport").rlike("^[0-9]+$")).count()
print(f"Vuelos con códigos sin traducir: {numeric_codes}")

without_timezone = result.filter(F.col("origin_timezone").isNull()
                                 | F.col("destination_timezone").isNull()).count()
print(f"Vuelos sin zona horaria: {without_timezone}")

save_check(spark, catalog, "flights", "codigos_sin_traducir", numeric_codes, True, batch_id)
save_check(spark, catalog, "flights", "sin_zona_horaria", without_timezone, True, batch_id)

if numeric_codes > 0 or without_timezone > 0:
    raise RuntimeError("Hay vuelos sin traducir o sin zona horaria: no se pueden unir con el clima")
