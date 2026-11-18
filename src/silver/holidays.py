from pyspark.sql import DataFrame, Window, functions as F

HOLIDAYS_SCHEMA = ("array<struct<date: string, localName: string, countryCode: string, "
                   "global: boolean, counties: array<string>, types: array<string>>>")

def empty_list():
    return F.array().cast("array<string>")


def explode_holidays(df: DataFrame) -> DataFrame:
    """Una fila por feriado. Los estados quedan sin el prefijo del país ("US-CA" -> "CA")."""
    parsed = df.withColumn("h", F.explode(F.from_json("raw_json", HOLIDAYS_SCHEMA)))
    states = F.transform(F.coalesce("h.counties", empty_list()),
                         lambda s: F.regexp_replace(s, "^[A-Z]{2}-", ""))
    return parsed.select(
        F.col("h.countryCode").alias("country_code"),
        F.to_date("h.date").alias("holiday_date"),
        F.col("h.localName").alias("holiday_name"),
        F.col("h.global").alias("is_global"),
        states.alias("states"),
        F.coalesce("h.types", empty_list()).alias("types"),
        "ingestion_ts",
        "batch_id",
    )


def merge_duplicate_holidays(df: DataFrame) -> DataFrame:
    """Una fila por país, fecha y nombre. Nager repite feriados (uno nacional y otro
    regional, o con distinto tipo): se juntan sus estados y tipos.
    Si hubo varias cargas, se usa solo la más reciente."""
    key = ["country_code", "holiday_date", "holiday_name"]
    df = df.withColumn("latest_ts", F.max("ingestion_ts").over(Window.partitionBy(*key)))
    df = df.filter(F.col("ingestion_ts") == F.col("latest_ts")).drop("latest_ts")

    merged = df.groupBy(*key).agg(
        F.max("is_global").alias("is_national"),
        F.array_sort(F.array_distinct(F.flatten(F.collect_list("states")))).alias("states"),
        F.array_sort(F.array_distinct(F.flatten(F.collect_list("types")))).alias("types"),
        F.max("ingestion_ts").alias("ingestion_ts"),
        F.max("batch_id").alias("batch_id"),
    )
    # Si es nacional, la lista de estados no aplica
    return merged.withColumn("states", F.when(F.col("is_national"), empty_list())
                                         .otherwise(F.col("states")))
