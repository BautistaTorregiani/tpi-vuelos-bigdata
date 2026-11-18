# Databricks notebook source

dbutils.widgets.text("catalog", "vuelos_dev")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

from src.silver.holidays import explode_holidays, merge_duplicate_holidays

raw = spark.table(f"{catalog}.bronze.holidays_raw")
holidays = merge_duplicate_holidays(explode_holidays(raw))

# Se reconstruye entera desde bronze: re-ejecutar no duplica
holidays.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.silver.holidays")

result = spark.table(f"{catalog}.silver.holidays")
print(f"holidays: {result.count()} filas")
display(result.orderBy("holiday_date"))

if result.count() == 0:
    raise RuntimeError("silver.holidays quedó vacía")
