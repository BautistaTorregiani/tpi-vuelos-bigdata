# Databricks notebook source

dbutils.widgets.text("catalog", "vuelos_dev")
dbutils.widgets.text("landing_path", "abfss://landing@tpivuelos.dfs.core.windows.net")
dbutils.widgets.text("batch_id", "manual")

catalog = dbutils.widgets.get("catalog")
landing_path = dbutils.widgets.get("landing_path")
batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

from src.bronze.csv_ingestion import read_raw_csv, add_ingestion_metadata

files = {
    "airlines_raw": "kaggle/flight_delays_2015/airlines.csv",
    "airports_raw": "kaggle/flight_delays_2015/airports.csv",
    "flights_raw": "kaggle/flight_delays_2015/flights.csv",
    "airport_codes_raw": "airport_codes/master_coordinate.csv",
}

for table_name, file in files.items():
    df = read_raw_csv(spark, f"{landing_path}/{file}")
    df = add_ingestion_metadata(df, batch_id)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.bronze.{table_name}")
    print(f"{table_name}: {spark.table(f'{catalog}.bronze.{table_name}').count():,} filas")