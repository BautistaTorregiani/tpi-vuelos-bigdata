import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# Spark lanza procesos de Python aparte; en Windows hay que indicarle cuál usar
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Permite importar "src" desde los tests
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture(scope="session")
def spark():
    spark = (SparkSession.builder.master("local[1]").appName("tests")
             .config("spark.sql.session.timeZone", "UTC")   # sin horario de verano: las horas no se corren
             .getOrCreate())
    yield spark
    spark.stop()