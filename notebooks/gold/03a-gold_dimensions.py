# Databricks notebook source

dbutils.widgets.text("catalog", "vuelos_dev")
dbutils.widgets.text("batch_id", "manual")
dbutils.widgets.text("start_date", "2015-01-01")
dbutils.widgets.text("end_date", "2015-12-31")
dbutils.widgets.text("force_fail", "false")

catalog = dbutils.widgets.get("catalog")
batch_id = dbutils.widgets.get("batch_id")
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")
force_fail = dbutils.widgets.get("force_fail")

# Fallo provocado a propósito, solo para demostrar el repair run
if force_fail == "true":
    raise RuntimeError("Fallo provocado a propósito para probar el repair run")

# COMMAND ----------

from datetime import date
from src.gold.dimensions import build_dim_date, build_dim_airline, build_dim_airport, build_dim_weather
from src.common.quality import build_check, save_checks, fail_if_critical
from src.common.catalog_comments import apply_comments
from src.gold.star_dictionary import STAR_DICTIONARY

holidays = spark.table(f"{catalog}.silver.holidays")
airlines = spark.table(f"{catalog}.silver.airlines")
airports = spark.table(f"{catalog}.silver.airports")
weather = spark.table(f"{catalog}.silver.weather_hourly")

dim_date = build_dim_date(spark, holidays, start_date, end_date)
dim_airline = build_dim_airline(airlines)
dim_airport = build_dim_airport(airports, weather)
dim_weather = build_dim_weather(spark, weather)

# COMMAND ----------

# Controles de calidad, antes de guardar
expected_days = (date.fromisoformat(end_date) - date.fromisoformat(start_date)).days + 1
weather_airports = weather.select("iata_code").distinct().count()

total_dates = dim_date.count()
total_airlines = dim_airline.count()
total_airports = dim_airport.count()
airports_with_weather = dim_airport.filter("has_weather").count()
total_weather = dim_weather.count()
unknown_rows = dim_weather.filter("weather_key = -1").count()

checks = [
    build_check("dim_date", "dias_faltantes", expected_days - total_dates, expected_days, True),
    build_check("dim_date", "claves_repetidas",
                total_dates - dim_date.select("date_key").distinct().count(), total_dates, True),
    build_check("dim_airline", "claves_repetidas",
                total_airlines - dim_airline.select("airline_key").distinct().count(), total_airlines, True),
    build_check("dim_airport", "claves_repetidas",
                total_airports - dim_airport.select("airport_key").distinct().count(), total_airports, True),
    build_check("dim_airport", "aeropuertos_con_clima_distintos_del_clima",
                abs(weather_airports - airports_with_weather), total_airports, True),
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

# COMMAND ----------

# Comentarios de Unity Catalog en cada dimensión y sus columnas
for table_name in ["dim_date", "dim_airline", "dim_airport", "dim_weather"]:
    table_comment, column_comments = STAR_DICTIONARY[table_name]
    apply_comments(spark, f"{catalog}.gold.{table_name}", table_comment, column_comments)