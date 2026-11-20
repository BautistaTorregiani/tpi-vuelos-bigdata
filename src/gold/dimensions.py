from pyspark.sql import functions as F

from src.gold.weather_join import build_weather_key

# Dimensión de fecha
def build_dim_date(spark, holidays, start_date, end_date):
    """Una fila por día entre start_date y end_date, con sus datos de calendario y los feriados nacionales."""
    days = F.sequence(F.to_date(F.lit(start_date)), F.to_date(F.lit(end_date)))
    dates = spark.range(1).select(F.explode(days).alias("date"))

    dates = dates.withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    dates = dates.withColumn("year", F.year("date"))
    dates = dates.withColumn("quarter", F.quarter("date"))
    dates = dates.withColumn("month", F.month("date"))
    dates = dates.withColumn("month_name", F.date_format("date", "MMMM"))
    dates = dates.withColumn("day_of_month", F.dayofmonth("date"))
    dates = dates.withColumn("day_of_week", F.expr("weekday(date) + 1"))   # 1 = lunes, como en los vuelos
    dates = dates.withColumn("day_name", F.date_format("date", "EEEE"))
    dates = dates.withColumn("is_weekend", F.col("day_of_week") >= 6)

    national = holidays.filter(F.col("is_national"))
    national = national.select(F.col("holiday_date").alias("date"), "holiday_name")
    national = national.dropDuplicates(["date"])

    dates = dates.join(national, "date", "left")
    dates = dates.withColumn("is_holiday", F.col("holiday_name").isNotNull())

    return dates.select("date_key", "date", "year", "quarter", "month", "month_name",
                        "day_of_month", "day_of_week", "day_name", "is_weekend",
                        "is_holiday", "holiday_name")

# Dimensiones de aerolínea
def build_dim_airline(airlines):
    """Una fila por aerolínea. La clave es su código."""
    return airlines.select(F.col("iata_code").alias("airline_key"), "airline_name")

# Dimensiones de aeropuerto
def build_dim_airport(airports, weather):
    """Una fila por aeropuerto. La clave es su código IATA.
    has_weather indica si es uno de los aeropuertos con datos de clima."""
    with_weather = weather.select(F.col("iata_code").alias("airport_key")).distinct()
    with_weather = with_weather.withColumn("has_weather", F.lit(True))

    dim = airports.select(F.col("iata_code").alias("airport_key"), "airport_name", "city", "state",
                          "latitude", "longitude", "timezone")
    dim = dim.join(with_weather, "airport_key", "left")
    return dim.withColumn("has_weather", F.coalesce(F.col("has_weather"), F.lit(False)))


# Dimensión de clima
def precipitation_category(precipitation_column, snowfall_column):
    """Categoría de precipitación por hora. La nieve tiene prioridad sobre la lluvia."""
    precipitation = F.col(precipitation_column)
    return (F.when(F.col(snowfall_column) > 0, "Nieve")
             .when(precipitation.isNull(), "Sin dato")
             .when(precipitation == 0, "Sin precipitación")
             .when(precipitation < 2.5, "Leve")
             .when(precipitation <= 7.6, "Moderada")
             .otherwise("Fuerte"))


def wind_category(wind_column):
    """Categoría de viento, según una escala de Beaufort simplificada (km/h)."""
    wind = F.col(wind_column)
    return (F.when(wind.isNull(), "Sin dato")
             .when(wind < 20, "Calmo o suave")
             .when(wind < 40, "Moderado")
             .when(wind < 62, "Fuerte")
             .otherwise("Temporal"))


def build_dim_weather(spark, weather):
    """Una fila por aeropuerto y hora, con las mediciones y sus categorías,
    más la fila -1 ("Sin dato") para los vuelos sin clima."""
    dim = weather.select(
        build_weather_key("iata_code", "weather_hour_utc").alias("weather_key"),
        "iata_code",
        "weather_hour_utc",
        F.col("temperature_2m").alias("temperature_c"),
        F.col("precipitation").alias("precipitation_mm"),
        F.col("snowfall").alias("snowfall_cm"),
        F.col("wind_speed_10m").alias("wind_speed_kmh"),
        F.col("wind_gusts_10m").alias("wind_gusts_kmh"),
        F.col("cloud_cover").alias("cloud_cover_pct"),
    )
    dim = dim.withColumn("precipitation_category", precipitation_category("precipitation_mm", "snowfall_cm"))
    dim = dim.withColumn("wind_category", wind_category("wind_speed_kmh"))

    unknown_row = spark.createDataFrame([(-1, "Sin dato", "Sin dato")],
                                        "weather_key bigint, precipitation_category string, wind_category string")
    return dim.unionByName(unknown_row, allowMissingColumns=True)