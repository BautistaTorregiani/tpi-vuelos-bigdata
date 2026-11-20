# Databricks notebook source

dbutils.widgets.text("catalog", "vuelos_dev")
dbutils.widgets.text("batch_id", "manual")
dbutils.widgets.text("train_end", "2015-08-31")
dbutils.widgets.text("validation_end", "2015-10-31")
dbutils.widgets.text("weather_variables",
    "temperature_2m,precipitation,snowfall,wind_speed_10m,wind_gusts_10m,cloud_cover")
catalog = dbutils.widgets.get("catalog")
batch_id = dbutils.widgets.get("batch_id")
train_end = dbutils.widgets.get("train_end")
validation_end = dbutils.widgets.get("validation_end")
variables = dbutils.widgets.get("weather_variables").split(",")

# COMMAND ----------

from pyspark.sql import functions as F
from src.gold.weather_join import add_departure_weather, rename_weather_columns
from src.gold.ml_training import (add_exclusion_reason, add_target, add_calendar_features,
                                  add_holiday_features, add_history_features, add_split)
from src.gold.ml_dictionary import DATA_DICTIONARY, COLUMNS, TABLE_COMMENT, column_comments, build_dictionary_table
from src.common.quality import build_check, save_checks, fail_if_critical
from src.common.catalog_comments import apply_comments

flights = spark.table(f"{catalog}.silver.flights")
weather = spark.table(f"{catalog}.silver.weather_hourly")
holidays = spark.table(f"{catalog}.silver.holidays")

flights = add_departure_weather(flights, weather, variables)
flights = rename_weather_columns(flights)
flights = add_exclusion_reason(flights)

# Registro de población: cuántos vuelos entran y cuántos se excluyen y por qué
log = flights.groupBy("exclusion_reason").count()
log = log.select(F.coalesce("exclusion_reason", F.lit("incluido")).alias("step"),
                 F.col("count").alias("rows"))
log = log.withColumn("batch_id", F.lit(batch_id)).withColumn("logged_at", F.current_timestamp())

# COMMAND ----------

# La historia se aprende de todos los vuelos completados (tengan clima o no)
history = flights.filter(~F.col("is_cancelled") & ~F.col("is_diverted") & F.col("arrival_delay").isNotNull())

training = flights.filter(F.col("exclusion_reason").isNull())
training = add_target(training)
training = add_calendar_features(training)
training = add_holiday_features(training, holidays)
training = add_history_features(training, history, ["airline"], "airline")
training = add_history_features(training, history, ["origin_airport"], "origin")
training = add_history_features(training, history, ["origin_airport", "destination_airport"], "route")
training = add_split(training, train_end, validation_end)
training = training.select(COLUMNS)

# COMMAND ----------

# Controles de calidad, antes de guardar
counts = training.select(
    F.count("*").alias("total"),
    F.count_distinct("flight_id").alias("distinct_ids"),
    F.sum(F.col("is_delayed").isNull().cast("int")).alias("null_target"),
).first()
total = counts["total"]

checks = [
    build_check("ml_flight_delay_training", "tabla_vacia", 1 if total == 0 else 0, total, True),
    build_check("ml_flight_delay_training", "id_repetido", total - counts["distinct_ids"], total, True),
    build_check("ml_flight_delay_training", "target_nulo", counts["null_target"], total, True),
]
save_checks(spark, catalog, checks, batch_id)
fail_if_critical(checks)

# Se reconstruye entera desde silver: re-ejecutar no duplica
training.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.gold.ml_flight_delay_training")
log.write.mode("overwrite").saveAsTable(f"{catalog}.gold.ml_population_log")

# COMMAND ----------

# Comentarios de Unity Catalog y diccionario de datos como tabla, para el informe
table = f"{catalog}.gold.ml_flight_delay_training"
apply_comments(spark, table, TABLE_COMMENT, column_comments())
build_dictionary_table(spark, table).write.mode("overwrite").saveAsTable(f"{catalog}.gold.ml_data_dictionary")

# Resumen por partición: cantidad de filas y tasa de demora (desbalance)
display(spark.table(table).groupBy("split").agg(F.count("*").alias("rows"),
                                               F.avg("is_delayed").alias("delay_rate")))
