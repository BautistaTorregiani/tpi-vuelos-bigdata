"""
Consulta las tablas de gold desde afuera del workspace de Databricks.

Se conecta al SQL warehouse con un access token y muestra las primeras 5 filas
de cada tabla del modelo estrella (Gold A) y de la tabla de ML (Gold B).

Uso:
    1. pip install databricks-sql-connector
    2. Reemplazar PEGAR_ACA_EL_TOKEN por el token recibido por mail.
    3. python consulta_gold.py
"""

from databricks import sql

# Pegá acá el token
TOKEN = "PEGAR_ACA_EL_TOKEN"

# Tablas de gold que se muestran: el modelo estrella (Gold A) y la tabla de ML (Gold B)
TABLES = [
    "dim_date",
    "dim_airline",
    "dim_airport",
    "dim_weather",
    "fact_flights",
    "ml_flight_delay_training",
]

connection = sql.connect(
    server_hostname="adb-7405607763151600.0.azuredatabricks.net",
    http_path="/sql/1.0/warehouses/59f89abd5a23dccd",
    access_token=TOKEN,
)
cursor = connection.cursor()

for table in TABLES:
    full_name = f"vuelos_dev.gold.{table}"
    print(f"\n=== {full_name} ===")

    cursor.execute(f"SELECT * FROM {full_name} LIMIT 5")
    for row in cursor.fetchall():
        print(row)

cursor.close()
connection.close()