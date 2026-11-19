# Databricks notebook source

dbutils.widgets.text("catalog", "vuelos_dev")
dbutils.widgets.text("batch_id", "manual")
catalog = dbutils.widgets.get("catalog")
batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

from src.silver.holidays import explode_holidays, merge_duplicate_holidays
from src.common.quality import build_check, save_checks, fail_if_critical

raw = spark.table(f"{catalog}.bronze.holidays_raw")
holidays = merge_duplicate_holidays(explode_holidays(raw))

# Controles de calidad, antes de guardar
total = holidays.count()
checks = [build_check("holidays", "tabla_vacia", 1 if total == 0 else 0, total, True)]
save_checks(spark, catalog, checks, batch_id)
fail_if_critical(checks)

# Se reconstruye entera desde bronze: re-ejecutar no duplica
holidays.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.holidays")
