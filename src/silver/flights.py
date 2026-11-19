from pyspark.sql import DataFrame, functions as F

FLIGHT_COLUMNS = {
    "AIRLINE": ("airline", "string"),
    "FLIGHT_NUMBER": ("flight_number", "int"),
    "TAIL_NUMBER": ("tail_number", "string"),
    "ORIGIN_AIRPORT": ("origin_airport", "string"),
    "DESTINATION_AIRPORT": ("destination_airport", "string"),
    "YEAR": ("year", "int"),
    "MONTH": ("month", "int"),
    "DAY": ("day", "int"),
    "DAY_OF_WEEK": ("day_of_week", "int"),
    "SCHEDULED_DEPARTURE": ("departure_planned_hhmm", "string"),
    "DISTANCE": ("distance", "int"),
    "SCHEDULED_TIME": ("planned_duration", "int"),
    "ELAPSED_TIME": ("real_duration", "int"),
    "AIR_TIME": ("air_time", "int"),
    "TAXI_OUT": ("taxi_out", "int"),
    "TAXI_IN": ("taxi_in", "int"),
    "DEPARTURE_DELAY": ("departure_delay", "int"),
    "ARRIVAL_DELAY": ("arrival_delay", "int"),
    "CANCELLED": ("is_cancelled", "boolean"),
    "DIVERTED": ("is_diverted", "boolean"),
    "CANCELLATION_REASON": ("cancellation_reason", "string"),
    "AIR_SYSTEM_DELAY": ("delay_air_system", "int"),
    "SECURITY_DELAY": ("delay_security", "int"),
    "AIRLINE_DELAY": ("delay_airline", "int"),
    "LATE_AIRCRAFT_DELAY": ("delay_late_aircraft", "int"),
    "WEATHER_DELAY": ("delay_weather", "int"),
    "batch_id": ("batch_id", "string"),
}


def cast_flight_columns(df: DataFrame) -> DataFrame:
    new_columns = []
    for bronze_name, (silver_name, new_type) in FLIGHT_COLUMNS.items():
        converted = F.expr(f"try_cast({bronze_name} AS {new_type})").alias(silver_name)
        new_columns.append(converted)
    return df.select(new_columns)


def add_flight_date(df: DataFrame) -> DataFrame:
    """Arma la fecha del vuelo a partir de año, mes y día, y descarta esas tres columnas."""
    df = df.withColumn("flight_date", F.make_date("year", "month", "day"))
    return df.drop("year", "month", "day")


def add_departure_times(df: DataFrame) -> DataFrame:
    """Arma la salida planeada y la real, en hora local.
    La planeada es la fecha del vuelo pegada a la hora HHMM.
    La real es la planeada más el retraso de salida."""

    hhmm = F.lpad("departure_planned_hhmm", 4, "0")      # "5" → "0005", por las dudas
    flight_date = F.col("flight_date")

    # Caso especial: "2400" es la medianoche, o sea las 00:00 del día siguiente
    is_midnight = hhmm == "2400"
    hhmm = F.when(is_midnight, "0000").otherwise(hhmm)
    flight_date = F.when(is_midnight, F.date_add(flight_date, 1)).otherwise(flight_date)

    # "0005" → "00:05", y lo pegamos a la fecha: "2015-01-01 00:05"
    time_text = F.concat(F.substring(hhmm, 1, 2), F.lit(":"), F.substring(hhmm, 3, 2))
    datetime_text = F.concat(flight_date.cast("string"), F.lit(" "), time_text)
    df = df.withColumn("departure_planned_local", F.to_timestamp_ntz(datetime_text, F.lit("yyyy-MM-dd HH:mm")))

    # Salida real = salida planeada + retraso (si el vuelo se canceló, queda vacía)
    df = df.withColumn("departure_real_local",
                       F.expr("departure_planned_local + departure_delay * INTERVAL 1 MINUTE"))

    return df.drop("departure_planned_hhmm")




# Códigos de aeropuerto de octubre
# En octubre de 2015 los vuelos traen códigos numéricos (ej. 10397)
# en lugar de los de 3 letras (ej. ATL). Acá los traducimos.

def build_airport_code_mapping(codes, reference_date="2015-10-01"):
    """Arma la tabla código numérico -> código de 3 letras, vigente en la fecha dada."""
    # master_coordinate tiene varias filas por aeropuerto, una por período de vigencia.
    # Nos quedamos con la que estaba vigente en la fecha (si THRU_DATE está vacía, sigue vigente).
    date = F.lit(reference_date).cast("date")
    start = F.to_date("AIRPORT_START_DATE", "M/d/yyyy h:mm:ss a")
    thru = F.to_date("AIRPORT_THRU_DATE", "M/d/yyyy h:mm:ss a")
    valid = codes.filter((start <= date) & (thru.isNull() | (thru >= date)))
    return valid.select(F.col("AIRPORT_ID").alias("code"),
                        F.col("AIRPORT").alias("iata_code"))


def translate_airport_codes(df, mapping):
    """Reemplaza los códigos numéricos de origen y destino por el de 3 letras."""
    was_numeric = (F.col("origin_airport").rlike("^[0-9]+$")
                   | F.col("destination_airport").rlike("^[0-9]+$"))
    df = df.withColumn("airport_code_was_numeric", was_numeric)

    # Pasamos la tabla a un diccionario: {"10397": "ATL", "12478": "JFK", ...}
    translations = {}
    for row in mapping.collect():
        translations[row["code"]] = row["iata_code"]

    # Buscar y reemplazar en las dos columnas
    return df.replace(translations, subset=["origin_airport", "destination_airport"])


def add_airport_timezones(df, airports):
    """Agrega la zona horaria del aeropuerto de origen y la del de destino."""
    origin = airports.select(F.col("iata_code").alias("origin_airport"),
                             F.col("timezone").alias("origin_timezone"))
    destination = airports.select(F.col("iata_code").alias("destination_airport"),
                                  F.col("timezone").alias("destination_timezone"))
    df = df.join(origin, "origin_airport", "left")
    return df.join(destination, "destination_airport", "left")


def local_to_utc(column, timezone):
    """Convierte una hora local a UTC, según la zona horaria indicada."""
    return F.convert_timezone(F.col(timezone), F.lit("UTC"), F.col(column))


def add_departure_utc(df):
    """Agrega la salida planeada y la real en UTC, usando la zona del origen."""
    df = df.withColumn("departure_planned_utc",
                       local_to_utc("departure_planned_local", "origin_timezone"))
    df = df.withColumn("departure_real_utc",
                       local_to_utc("departure_real_local", "origin_timezone"))

    # Para unir con clima
    df = df.withColumn("departure_hour_utc",
                       F.expr("departure_planned_utc - minute(departure_planned_utc) * INTERVAL 1 MINUTE"))
    return df


# Calcula la llegada planeada y la real en UTC, sumando minutos a la salida.
def add_arrival_utc(df):
    """Calcula la llegada planeada y la real en UTC, sumando minutos a la salida."""
    df = df.withColumn("arrival_planned_utc",
                       F.expr("departure_planned_utc + planned_duration * INTERVAL 1 MINUTE"))
    df = df.withColumn("arrival_real_utc",
                       F.expr("arrival_planned_utc + arrival_delay * INTERVAL 1 MINUTE"))
    return df



def utc_to_local(column, timezone):
    """Convierte una hora UTC a la hora local de la zona indicada."""
    return F.convert_timezone(F.lit("UTC"), F.col(timezone), F.col(column))


# Agrega la llegada planeada y la real en la hora local del aeropuerto de destino.
def add_arrival_local(df):
    """Agrega la llegada planeada y la real en la hora local del destino."""
    df = df.withColumn("arrival_planned_local",
                       utc_to_local("arrival_planned_utc", "destination_timezone"))
    df = df.withColumn("arrival_real_local",
                       utc_to_local("arrival_real_utc", "destination_timezone"))
    return df


def add_flight_id(df):
    """Identificador del vuelo: siempre da el mismo valor para el mismo vuelo."""
    return df.withColumn("flight_id", F.xxhash64("flight_date", "airline", "flight_number",
                                                 "origin_airport", "destination_airport",
                                                 "departure_planned_local"))
