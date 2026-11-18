from pyspark.sql import functions as F


def clean_airlines(df):
    """Renombra las columnas de la tabla de aerolíneas."""
    return df.select(
        F.col("IATA_CODE").alias("iata_code"),
        F.col("AIRLINE").alias("airline_name"),
        F.col("batch_id"),
           )