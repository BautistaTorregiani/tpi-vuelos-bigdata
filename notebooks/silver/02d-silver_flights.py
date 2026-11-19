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
                                add_arrival_utc, add_arrival_local, add_flight_id)
from src.common.quality import build_check, save_checks, fail_if_critical

flights = spark.table(f"{catalog}.bronze.flights_raw")
flights = cast_flight_columns(flights)
flights = add_flight_date(flights)
flights = add_departure_times(flights)

# Octubre trae códigos numéricos: los pasamos a 3 letras
codes = spark.table(f"{catalog}.bronze.airport_codes_raw")
mapping = build_airport_code_mapping(codes)
flights = translate_airport_codes(flights, mapping)
flights = add_flight_id(flights)

# Con la zona horaria de cada aeropuerto pasamos los horarios a UTC
airports = spark.table(f"{catalog}.silver.airports")
flights = add_airport_timezones(flights, airports)
flights = add_departure_utc(flights)
flights = add_arrival_utc(flights)
flights = add_arrival_local(flights)

# COMMAND ----------

# Controles de calidad, antes de guardar: una sola pasada sobre los datos
counts = flights.select(
    F.count("*").alias("total"),
    F.count_distinct("flight_id").alias("distinct_ids"),
    F.sum(F.when(F.col("origin_airport").rlike("^[0-9]+$")
                 | F.col("destination_airport").rlike("^[0-9]+$"), 1).otherwise(0)).alias("numeric_codes"),
    F.sum(F.when(F.col("origin_timezone").isNull()
                 | F.col("destination_timezone").isNull(), 1).otherwise(0)).alias("without_timezone"),
    F.sum(F.when(F.col("flight_date").isNull() | F.col("airline").isNull()
                 | F.col("origin_airport").isNull()
                 | F.col("departure_planned_local").isNull(), 1).otherwise(0)).alias("null_keys"),
).first()

total = counts["total"]
bronze_total = spark.table(f"{catalog}.bronze.flights_raw").count()

checks = [
    build_check("flights", "filas_distintas_a_bronze", abs(bronze_total - total), bronze_total, True),
    build_check("flights", "id_repetido", total - counts["distinct_ids"], total, True),
    build_check("flights", "codigos_sin_traducir", counts["numeric_codes"], total, True),
    build_check("flights", "sin_zona_horaria", counts["without_timezone"], total, True),
    build_check("flights", "claves_nulas", counts["null_keys"], total, True),
]
save_checks(spark, catalog, checks, batch_id)
fail_if_critical(checks)

# Solo si pasaron todos los controles críticos, guardamos (re-ejecutar no duplica)
flights.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.flights")
