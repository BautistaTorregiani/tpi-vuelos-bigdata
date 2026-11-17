from pyspark.sql import DataFrame, SparkSession, functions as F


def read_raw_csv(spark: SparkSession, path: str) -> DataFrame:
    """Lee un CSV con encabezado, dejando todas las columnas como texto."""
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", False)   # todo string: copia fiel de la fuente
        .csv(path)
    )


def add_ingestion_metadata(df: DataFrame, batch_id: str) -> DataFrame:
    """Agrega de dónde vino cada fila, cuándo se cargó y en qué lote."""
    return (
        df
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("batch_id", F.lit(batch_id))
    )