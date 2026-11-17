from pyspark.sql import DataFrame, SparkSession, functions as F


def save_without_duplicates(spark: SparkSession, new_data: DataFrame,
                            table_name: str, keys: list[str]):
    """Guarda new_data sin duplicar: si ya existe una fila con las mismas claves
    la reemplaza, si no la agrega. Si la tabla no existe, la crea."""
    if not spark.catalog.tableExists(table_name):
        new_data.write.saveAsTable(table_name)
        return

    # Armamos la condición del MERGE a partir de las claves. Por ejemplo, con
    # ["year", "country_code"] queda: "old.year = new.year AND old.country_code = new.country_code"
    comparisons = []
    for key in keys:
        comparisons.append(f"old.{key} = new.{key}")
    condition = " AND ".join(comparisons)

    new_data.createOrReplaceTempView("new_data")
    spark.sql(f"""
        MERGE INTO {table_name} AS old
        USING new_data AS new
        ON {condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def save_errors(spark: SparkSession, errors: list[dict], table_name: str, batch_id: str):
    """Agrega los errores de ingesta a la tabla de errores (se acumulan: es un historial)."""
    if len(errors) == 0:
        return

    errors_df = spark.createDataFrame(
        errors, "source string, entity string, source_url string, error_message string")
    errors_df = errors_df.withColumn("ingestion_ts", F.current_timestamp())
    errors_df = errors_df.withColumn("batch_id", F.lit(batch_id))
    errors_df.write.mode("append").saveAsTable(table_name)