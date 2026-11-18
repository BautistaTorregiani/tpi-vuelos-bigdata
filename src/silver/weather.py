from pyspark.sql import DataFrame, Window, functions as F


def build_weather_schema(variables: list[str]) -> str:
    """Esquema del JSON de Open-Meteo para las variables pedidas."""
    arrays = []
    for variable in variables:
        arrays.append(f"{variable}: array<double>")
    arrays_text = ", ".join(arrays)
    return f"timezone string, utc_offset_seconds int, hourly struct<time: array<string>, {arrays_text}>"


def explode_hourly_weather(df: DataFrame, variables: list[str]) -> DataFrame:
    """Una fila por aeropuerto y hora, con la hora en UTC.
    Open-Meteo usa un desfase fijo sin horario de verano, por eso pasamos a UTC.
    Ojo: precipitation y snowfall de las 14:00 son lo acumulado entre las 13:00 y las 14:00."""
    parsed = df.withColumn("json", F.from_json("raw_json", build_weather_schema(variables)))
    hourly = parsed.select("iata_code", "ingestion_ts", "batch_id",
                           F.col("json.timezone").alias("timezone"),
                           F.col("json.utc_offset_seconds").alias("utc_offset_seconds"),
                           "json.hourly.*")

    # Juntamos las listas posición por posición y abrimos una fila por hora
    rows = hourly.withColumn("hour_data", F.explode(F.arrays_zip("time", *variables)))
    rows = rows.withColumn("api_hour",
                           F.to_timestamp_ntz(F.col("hour_data.time"), F.lit("yyyy-MM-dd'T'HH:mm")))
    hour_utc = F.expr("api_hour - utc_offset_seconds * INTERVAL 1 SECOND")  # 12:00 con UTC-5 -> 17:00

    columns = ["iata_code", hour_utc.alias("weather_hour_utc")]
    for variable in variables:
        columns.append(F.col(f"hour_data.{variable}").alias(variable))
    columns += ["timezone", "utc_offset_seconds", "ingestion_ts", "batch_id"]
    return rows.select(columns)


def deduplicate_weather(df: DataFrame) -> DataFrame:
    """Una fila por aeropuerto y hora UTC; se queda con la carga más reciente."""
    latest_first = Window.partitionBy("iata_code", "weather_hour_utc").orderBy(F.desc("ingestion_ts"))
    df = df.withColumn("rank", F.row_number().over(latest_first))
    df = df.filter("rank = 1")
    return df.drop("rank")


def count_repeated_hours(df: DataFrame) -> DataFrame:
    """Horas repetidas por aeropuerto antes de deduplicar (cargas que se pisan)."""
    counts = df.groupBy("iata_code", "weather_hour_utc").count()
    return counts.filter("count > 1")
