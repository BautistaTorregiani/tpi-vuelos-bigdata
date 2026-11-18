from pyspark.sql import functions as F
from timezonefinder import TimezoneFinder

# Aeropuertos que en airports.csv no tienen coordenadas: les ponemos la zona a mano
MANUAL_TIMEZONES = {
    "ECP": "America/Chicago",   # Panama City, Florida (usa hora central)
    "PBG": "America/New_York",  # Plattsburgh, Nueva York
    "UST": "America/New_York",  # St. Augustine, Florida
}


def clean_airports(df):
    """Renombra las columnas y pasa las coordenadas a número."""
    return df.select(
        F.col("IATA_CODE").alias("iata_code"),
        F.col("AIRPORT").alias("airport_name"),
        F.col("CITY").alias("city"),
        F.col("STATE").alias("state"),
        F.expr("try_cast(LATITUDE AS double)").alias("latitude"),
        F.expr("try_cast(LONGITUDE AS double)").alias("longitude"),
        F.col("batch_id"),
    )


def find_timezones(airports):
    """Arma un diccionario {código: zona horaria}, por ejemplo {"ATL": "America/New_York"}."""
    finder = TimezoneFinder()
    timezones = {}
    for row in airports.collect():
        code = row["iata_code"]
        if code in MANUAL_TIMEZONES:
            timezones[code] = MANUAL_TIMEZONES[code]
        elif row["latitude"] is not None:
            timezones[code] = finder.timezone_at(lat=row["latitude"], lng=row["longitude"])
    return timezones


def add_timezone(spark, airports):
    """Agrega la columna timezone a la tabla de aeropuertos."""
    timezones = find_timezones(airports)
    timezone_table = spark.createDataFrame(list(timezones.items()),
                                           "iata_code string, timezone string")
    return airports.join(timezone_table, "iata_code", "left")