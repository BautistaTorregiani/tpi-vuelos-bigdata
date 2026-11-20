# Databricks notebook source

dbutils.widgets.text("catalog", "vuelos_dev")
dbutils.widgets.text("batch_id", "manual")
catalog = dbutils.widgets.get("catalog")
batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

from src.gold.fact_flights import build_fact_flights, count_orphans
from src.common.quality import build_check, save_checks, fail_if_critical
from src.common.catalog_comments import apply_comments
from src.gold.star_dictionary import STAR_DICTIONARY

flights = spark.table(f"{catalog}.silver.flights")
weather = spark.table(f"{catalog}.silver.weather_hourly")
fact = build_fact_flights(flights, weather)

dim_date = spark.table(f"{catalog}.gold.dim_date")
dim_airline = spark.table(f"{catalog}.gold.dim_airline")
dim_airport = spark.table(f"{catalog}.gold.dim_airport")
dim_weather = spark.table(f"{catalog}.gold.dim_weather")

# COMMAND ----------

# Controles de calidad, antes de guardar
total = fact.count()
silver_total = flights.count()

checks = [
    build_check("fact_flights", "filas_distintas_a_silver", abs(silver_total - total), silver_total, True),
    build_check("fact_flights", "id_repetido", total - fact.select("flight_id").distinct().count(), total, True),
    build_check("fact_flights", "huerfanos_fecha", count_orphans(fact, "date_key", dim_date, "date_key"), total, True),
    build_check("fact_flights", "huerfanos_aerolinea",
                count_orphans(fact, "airline_key", dim_airline, "airline_key"), total, True),
    build_check("fact_flights", "huerfanos_origen",
                count_orphans(fact, "origin_airport_key", dim_airport, "airport_key"), total, True),
    build_check("fact_flights", "huerfanos_destino",
                count_orphans(fact, "destination_airport_key", dim_airport, "airport_key"), total, True),
    build_check("fact_flights", "huerfanos_clima",
                count_orphans(fact, "weather_key", dim_weather, "weather_key"), total, True),
]
save_checks(spark, catalog, checks, batch_id)
fail_if_critical(checks)

fact.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.fact_flights")

# COMMAND ----------

# Comentarios de Unity Catalog en la fact y sus columnas
table_comment, column_comments = STAR_DICTIONARY["fact_flights"]
apply_comments(spark, f"{catalog}.gold.fact_flights", table_comment, column_comments)