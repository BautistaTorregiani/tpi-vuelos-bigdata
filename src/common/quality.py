from pyspark.sql import functions as F


def build_check(spark, table_name, check_name, failed_rows, is_critical, batch_id):
    """Arma la fila con el resultado de un control de calidad."""
    row = [(table_name, check_name, failed_rows, is_critical, failed_rows == 0, batch_id)]
    df = spark.createDataFrame(row, "table_name string, check_name string, failed_rows long, "
                                    "is_critical boolean, passed boolean, batch_id string")
    return df.withColumn("checked_at", F.current_timestamp())


def save_check(spark, catalog, table_name, check_name, failed_rows, is_critical, batch_id):
    """Guarda el resultado en silver.quality_checks. Se acumula: es un historial."""
    df = build_check(spark, table_name, check_name, failed_rows, is_critical, batch_id)
    df.write.mode("append").saveAsTable(f"{catalog}.silver.quality_checks")
