from pyspark.sql import functions as F


def build_check(table_name, check_name, failed_rows, total_rows, is_critical):
    """Arma el resultado de un control de calidad como un diccionario."""
    return {
        "table_name": table_name,
        "check_name": check_name,
        "failed_rows": failed_rows,
        "total_rows": total_rows,
        "is_critical": is_critical,
        "passed": failed_rows == 0,
    }


def save_checks(spark, catalog, checks, batch_id):
    """Guarda todos los controles de una corrida en silver.quality_checks, de una sola vez."""
    schema = ("table_name string, check_name string, failed_rows long, total_rows long, "
              "is_critical boolean, passed boolean")
    df = spark.createDataFrame(checks, schema)
    df = df.withColumn("batch_id", F.lit(batch_id))
    df = df.withColumn("checked_at", F.current_timestamp())
    df.write.mode("append").saveAsTable(f"{catalog}.silver.quality_checks")


def fail_if_critical(checks):
    """Hace fallar la tarea si algún control crítico no pasó."""
    failed = []
    for check in checks:
        if check["is_critical"] and not check["passed"]:
            failed.append(f"{check['table_name']}.{check['check_name']} ({check['failed_rows']} filas)")
    if len(failed) > 0:
        raise RuntimeError("Fallaron controles críticos: " + ", ".join(failed))
