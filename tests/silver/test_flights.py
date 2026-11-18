from datetime import date, datetime

from src.silver.flights import (
    FLIGHT_COLUMNS,
    cast_flight_columns,
    add_flight_date,
    add_departure_times,
)


def make_bronze_flight(spark, **values):
    """Crea una fila de Bronze de prueba: todas las columnas vacías salvo las que indiquemos."""
    row = dict.fromkeys(FLIGHT_COLUMNS)   # todas las columnas, vacías
    row.update(values)                     # completamos las que pasamos
    schema = ", ".join(f"{name} string" for name in FLIGHT_COLUMNS)
    return spark.createDataFrame([row], schema)


def test_converts_types_and_renames(spark):
    df = make_bronze_flight(spark, DEPARTURE_DELAY="-11", DISTANCE="1448",
                            CANCELLED="0", AIRLINE="AS")

    row = cast_flight_columns(df).collect()[0]

    assert row["departure_delay"] == -11        # texto "-11" → número -11
    assert row["distance"] == 1448
    assert row["is_cancelled"] is False         
    assert row["airline"] == "AS"               


def test_invalid_value_becomes_empty_instead_of_failing(spark):
    df = make_bronze_flight(spark, DEPARTURE_DELAY="abc")

    row = cast_flight_columns(df).collect()[0]

    assert row["departure_delay"] is None       # no se pudo convertir → vacío, sin error

from datetime import date, datetime

from src.silver.flights import (
    FLIGHT_COLUMNS,
    cast_flight_columns,
    add_flight_date,
    add_departure_times,
)


# ---------- Ayudas para armar datos de prueba ----------

def make_bronze_flight(spark, **values):
    """Crea una fila de Bronze de prueba: todas las columnas vacías salvo las que indiquemos."""
    row = dict.fromkeys(FLIGHT_COLUMNS)   # todas las columnas, vacías
    row.update(values)                     # completamos las que pasamos
    schema = ", ".join(f"{name} string" for name in FLIGHT_COLUMNS)
    return spark.createDataFrame([row], schema)


def make_departure(spark, hhmm, delay):
    """Un vuelo de prueba del 1 de enero de 2015, con la hora planeada y el retraso indicados."""
    return spark.createDataFrame(
        [(date(2015, 1, 1), hhmm, delay)],
        "flight_date date, departure_planned_hhmm string, departure_delay int")

# ---------- Fecha y horarios ----------

def test_flight_date_is_built_from_year_month_day(spark):
    df = spark.createDataFrame([(2015, 1, 1)], "year int, month int, day int")

    row = add_flight_date(df).collect()[0]

    assert row["flight_date"] == date(2015, 1, 1)


def test_planned_departure_from_hhmm(spark):
    row = add_departure_times(make_departure(spark, "1430", 0)).collect()[0]
    assert row["departure_planned_local"] == datetime(2015, 1, 1, 14, 30)


def test_2400_is_midnight_of_next_day(spark):
    row = add_departure_times(make_departure(spark, "2400", 0)).collect()[0]
    assert row["departure_planned_local"] == datetime(2015, 1, 2, 0, 0)


def test_real_departure_can_be_on_previous_day(spark):
    # El vuelo de Alaska: planeado a las 00:05, salió 11 minutos antes
    row = add_departure_times(make_departure(spark, "0005", -11)).collect()[0]
    assert row["departure_real_local"] == datetime(2014, 12, 31, 23, 54)


def test_cancelled_flight_has_no_real_departure(spark):
    row = add_departure_times(make_departure(spark, "0800", None)).collect()[0]
    assert row["departure_real_local"] is None