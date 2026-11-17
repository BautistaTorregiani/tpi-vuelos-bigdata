# Databricks notebook source

# COMMAND ----------

dbutils.widgets.text("catalog", "vuelos_dev")
dbutils.widgets.text("year", "2015")
dbutils.widgets.text("country_code", "US")
dbutils.widgets.text("batch_id", "manual")

catalog = dbutils.widgets.get("catalog")
year = int(dbutils.widgets.get("year"))
country_code = dbutils.widgets.get("country_code")
batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

from pyspark.sql import functions as F
from src.bronze.holidays_ingestion import build_holidays_url
from src.common.api_requests import fetch_json
from src.common.save_tables import save_without_duplicates, save_errors

# Pedimos los feriados a la API
url = build_holidays_url(year, country_code)

try:
    raw_json = fetch_json(url)
    error = None
except Exception as e:
    raw_json = None
    error = str(e)

# COMMAND ----------

# Guardamos los feriados sin duplicar, o registramos el error y la tarea falla
if raw_json is not None:
    new_holidays = spark.createDataFrame(
        [(year, country_code, url, raw_json)],
        "year int, country_code string, source_url string, raw_json string")
    new_holidays = new_holidays.withColumn("ingestion_ts", F.current_timestamp())
    new_holidays = new_holidays.withColumn("batch_id", F.lit(batch_id))

    save_without_duplicates(spark, new_holidays, f"{catalog}.bronze.holidays_raw",
                            keys=["year", "country_code"])
    print(f"Feriados {year} {country_code}: OK")
else:
    save_errors(spark, [{"source": "nager-date", "entity": f"{year}-{country_code}",
                         "source_url": url, "error_message": error}],
                f"{catalog}.bronze.ingestion_errors", batch_id)
    raise RuntimeError(f"No se pudieron obtener los feriados: {error}")