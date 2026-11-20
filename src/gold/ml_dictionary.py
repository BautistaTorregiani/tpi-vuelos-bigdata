# Diccionario de datos de gold.ml_flight_delay_training: (columna, descripción, origen, disponibilidad)
BEFORE = "antes de la salida programada"

TABLE_COMMENT = ("Una fila por vuelo completado con clima en el origen. "
                 "Target: is_delayed. Predictoras conocidas antes de la salida programada.")

DATA_DICTIONARY = [
    ("flight_id", "Identificador del vuelo (hash de fecha, aerolínea, número, origen, destino y salida)", "silver.flights", "identificador"),
    ("flight_date", "Fecha del vuelo", "silver.flights", BEFORE),
    ("split", "Partición temporal: train, validation o test", "calculada", "identificador"),
    ("is_delayed", "TARGET: 1 si llegó con 15 minutos o más de retraso", "silver.flights (arrival_delay)", "después de la llegada (solo target)"),
    ("airline", "Código IATA de la aerolínea", "silver.flights", BEFORE),
    ("origin_airport", "Código IATA del aeropuerto de origen", "silver.flights", BEFORE),
    ("destination_airport", "Código IATA del aeropuerto de destino", "silver.flights", BEFORE),
    ("distance", "Distancia de la ruta en millas", "silver.flights", BEFORE),
    ("planned_duration", "Duración programada en minutos", "silver.flights", BEFORE),
    ("month", "Mes del vuelo", "calculada de flight_date", BEFORE),
    ("day_of_month", "Día del mes", "calculada de flight_date", BEFORE),
    ("day_of_week", "Día de la semana (1 = lunes ... 7 = domingo)", "silver.flights", BEFORE),
    ("is_weekend", "Si es sábado o domingo", "calculada de day_of_week", BEFORE),
    ("departure_hour_local", "Hora local de salida programada", "calculada de silver.flights", BEFORE),
    ("arrival_hour_local", "Hora local de llegada programada en el destino", "calculada de silver.flights", BEFORE),
    ("is_holiday", "Si el día es feriado nacional", "silver.holidays", BEFORE),
    ("days_to_nearest_holiday", "Días al feriado nacional más cercano", "silver.holidays", BEFORE),
    ("temperature_c", "Temperatura en el origen a la hora de salida (°C)", "silver.weather_hourly", BEFORE + " (pronóstico perfecto)"),
    ("precipitation_mm", "Lluvia en el origen en la hora previa a la salida (mm)", "silver.weather_hourly", BEFORE + " (pronóstico perfecto)"),
    ("snowfall_cm", "Nieve en el origen en la hora previa a la salida (cm)", "silver.weather_hourly", BEFORE + " (pronóstico perfecto)"),
    ("wind_speed_kmh", "Viento en el origen a la hora de salida, a 10 m del suelo (km/h)", "silver.weather_hourly", BEFORE + " (pronóstico perfecto)"),
    ("wind_gusts_kmh", "Ráfagas en el origen a la hora de salida, a 10 m del suelo (km/h)", "silver.weather_hourly", BEFORE + " (pronóstico perfecto)"),
    ("cloud_cover_pct", "Nubosidad en el origen a la hora de salida (%)", "silver.weather_hourly", BEFORE + " (pronóstico perfecto)"),
    ("airline_flights_30d", "Vuelos completados de la aerolínea en 30 días, terminando 2 días antes del vuelo", "calculada de silver.flights", BEFORE),
    ("airline_delay_rate_30d", "Tasa de demora de la aerolínea en 30 días, terminando 2 días antes del vuelo", "calculada de silver.flights", BEFORE),
    ("origin_flights_30d", "Vuelos completados desde el origen en 30 días, terminando 2 días antes del vuelo", "calculada de silver.flights", BEFORE),
    ("origin_delay_rate_30d", "Tasa de demora desde el origen en 30 días, terminando 2 días antes del vuelo", "calculada de silver.flights", BEFORE),
    ("route_flights_30d", "Vuelos completados de la ruta en 30 días, terminando 2 días antes del vuelo", "calculada de silver.flights", BEFORE),
    ("route_delay_rate_30d", "Tasa de demora de la ruta en 30 días, terminando 2 días antes del vuelo", "calculada de silver.flights", BEFORE),
]

COLUMNS = [row[0] for row in DATA_DICTIONARY]

def column_comments():
    """El comentario de cada columna: descripción, origen y disponibilidad."""
    comments = []
    for column, description, origin, availability in DATA_DICTIONARY:
        comments.append((column, f"{description}. Origen: {origin}. Disponible: {availability}"))
    return comments


def build_dictionary_table(spark, table):
    """El diccionario como tabla, con el tipo de dato real de cada columna, para anexar al informe."""
    types = {}
    for field in spark.table(table).schema.fields:
        types[field.name] = field.dataType.simpleString()

    rows = []
    for column, description, origin, availability in DATA_DICTIONARY:
        rows.append((column, types[column], description, origin, availability))
    return spark.createDataFrame(rows, "column_name string, data_type string, description string, "
                                       "origin string, availability string")