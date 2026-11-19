# Databricks notebook source

dbutils.widgets.text("catalog", "vuelos_dev")
dbutils.widgets.text("batch_id", "manual")
catalog = dbutils.widgets.get("catalog")
batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

from src.silver.holidays import explode_holidays, merge_duplicate_holidays
from src.common.quality import save_check

raw = spark.table(f"{catalog}.bronze.holidays_raw")
holidays = merge_duplicate_holidays(explode_holidays(raw))

# Se reconstruye entera desde bronze: re-ejecutar no duplica
holidays.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.silver.holidays")

result = spark.table(f"{catalog}.silver.holidays")
print(f"holidays: {result.count()} filas")
display(result.orderBy("holiday_date"))

empty = 1 if result.count() == 0 else 0
save_check(spark, catalog, "holidays", "tabla_vacia", empty, True, batch_id)
if empty:
    raise RuntimeError("silver.holidays quedó vacía")
