from pyspark.sql import DataFrame, Window, functions as F


def build_weather_schema(variables: list[str]) -> str:
    """Esquema del JSON de Open-Meteo para las variables pedidas."""
    arrays = ", ".join(f"{v}: array<double>" for v in variables)
    return f"timezone string, utc_offset_seconds int, hourly struct<time: array<string>, {arrays}>"


def explode_hourly_weather(df: DataFrame, variables: list[str]) -> DataFrame:
    """Una fila por aeropuerto y hora, con la hora en UTC.
    Open-Meteo usa un desfase fijo sin horario de verano, por eso pasamos a UTC."""
    parsed = df.withColumn("json", F.from_json("raw_json", build_weather_schema(variables)))
    hourly = parsed.select("iata_code", "ingestion_ts", "batch_id",
                           F.col("json.timezone").alias("timezone"),
                           F.col("json.utc_offset_seconds").alias("utc_offset_seconds"),
                           "json.hourly.*")

    rows = hourly.withColumn("h", F.explode(F.arrays_zip("time", *variables)))
    rows = rows.withColumn("api_hour", F.to_timestamp_ntz(F.col("h.time"), F.lit("yyyy-MM-dd'T'HH:mm")))
    hour_utc = F.expr("timestampadd(SECOND, -utc_offset_seconds, api_hour)")  # 12:00 con UTC-5 -> 17:00

    return rows.select(
        "iata_code",
        hour_utc.alias("weather_hour_utc"),
        *[F.col(f"h.{v}").alias(v) for v in variables],
        "timezone",
        "utc_offset_seconds",
        "ingestion_ts",
        "batch_id",
    )


def deduplicate_weather(df: DataFrame) -> DataFrame:
    """Una fila por aeropuerto y hora UTC; se queda con la carga más reciente."""
    latest_first = Window.partitionBy("iata_code", "weather_hour_utc").orderBy(F.desc("ingestion_ts"))
    return (df.withColumn("rank", F.row_number().over(latest_first))
              .filter("rank = 1")
              .drop("rank"))


def count_repeated_hours(df: DataFrame) -> DataFrame:
    """Horas repetidas por aeropuerto antes de deduplicar (cargas que se pisan)."""
    return (df.groupBy("iata_code", "weather_hour_utc").count()
              .filter("count > 1"))