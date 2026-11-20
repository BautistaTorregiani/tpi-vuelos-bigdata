from pyspark.sql import Window, functions as F

HISTORY_DAYS = 30


def add_exclusion_reason(df):
    """Motivo por el que el vuelo queda fuera de la tabla de ML (vacío si entra)."""
    reason = (F.when(F.col("is_cancelled"), "cancelado")
               .when(F.col("is_diverted"), "desviado")
               .when(~F.col("has_weather"), "sin clima en el origen")
               .when(F.col("arrival_delay").isNull(), "sin retraso de llegada"))
    return df.withColumn("exclusion_reason", reason)


def add_target(df):
    """1 si llegó con 15 minutos o más de retraso, 0 si no."""
    return df.withColumn("is_delayed", (F.col("arrival_delay") >= 15).cast("int"))


def add_calendar_features(df):
    """Variables de calendario y horario, todas conocidas antes de salir."""
    df = df.withColumn("month", F.month("flight_date"))
    df = df.withColumn("day_of_month", F.dayofmonth("flight_date"))
    df = df.withColumn("is_weekend", F.col("day_of_week").isin(6, 7))   # 1 = lunes ... 7 = domingo
    df = df.withColumn("departure_hour_local", F.hour("departure_planned_local"))
    df = df.withColumn("arrival_hour_local", F.hour("arrival_planned_local"))
    return df


def add_holiday_features(df, holidays):
    """Si el día es feriado nacional y a cuántos días está el feriado nacional más cercano."""
    national = holidays.filter("is_national").select("holiday_date")
    dates = df.select("flight_date").distinct()
    distances = dates.crossJoin(national)
    distances = distances.withColumn("days", F.abs(F.datediff("flight_date", "holiday_date")))
    distances = distances.groupBy("flight_date").agg(F.min("days").alias("days_to_nearest_holiday"))
    distances = distances.withColumn("is_holiday", F.col("days_to_nearest_holiday") == 0)
    return df.join(distances, "flight_date", "left")


def add_history_features(df, history, keys, prefix, days=HISTORY_DAYS):
    """Cantidad de vuelos y tasa de demora de los días ANTERIORES (nunca el mismo día),
    agrupando por keys. history son los vuelos completados de los que se aprende;"""
    group_columns = keys + ["flight_date"]

    daily = history.groupBy(group_columns).agg(
        F.count("*").alias("flights"),
        F.sum((F.col("arrival_delay") >= 15).cast("int")).alias("delayed"))
    daily = daily.withColumn("day_number", F.expr("unix_date(flight_date)"))

    # Sumamos los días de los vuelos a predecir, aunque ese día no haya historia (con 0 vuelos)
    days_to_predict = df.select(group_columns).distinct()
    daily = days_to_predict.join(daily, group_columns, "full")
    daily = daily.fillna(0, subset=["flights", "delayed"])

    # Desde "days" días antes hasta el día anterior: el mismo día no se mira
    previous_days = Window.partitionBy(keys).orderBy("day_number").rangeBetween(-days - 1, -2)
    daily = daily.withColumn("previous_flights",
                             F.coalesce(F.sum("flights").over(previous_days), F.lit(0)))
    daily = daily.withColumn("previous_delayed", F.sum("delayed").over(previous_days))

    flights_name = f"{prefix}_flights_{days}d"
    rate_name = f"{prefix}_delay_rate_{days}d"
    delay_rate = F.when(F.col("previous_flights") > 0,
                        F.col("previous_delayed") / F.col("previous_flights"))
    daily = daily.select(*group_columns,
                         F.col("previous_flights").alias(flights_name),
                         delay_rate.alias(rate_name))
    return df.join(daily, group_columns, "left")


def add_split(df, train_end, validation_end):
    """Partición temporal: train hasta train_end, validation hasta validation_end, test el resto."""
    split = (F.when(F.col("flight_date") <= F.lit(train_end).cast("date"), "train")
              .when(F.col("flight_date") <= F.lit(validation_end).cast("date"), "validation")
              .otherwise("test"))
    return df.withColumn("split", split)
