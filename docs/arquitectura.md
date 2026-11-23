# Job de Lakeflow: `tpi_vuelos_pipeline`

El Job ejecuta el pipeline completo, de las fuentes a las dos tablas Gold, con 10 tareas de tipo notebook.
Cada tarea espera solo a las que producen los datos que necesita; las que no dependen entre sí corren en paralelo.
Colores: naranja = Bronze, gris = Silver, amarillo = Gold.

![Job tpi_vuelos_pipeline](job_pipeline.png)

<details>
<summary>Código Mermaid del diagrama</summary>

```mermaid
---
config:
  layout: elk
---
flowchart LR
    bronze_csv["<b>bronze_csv</b><br/>CSV de vuelos, aeropuertos<br/>y aerolíneas"]
    bronze_holidays["<b>bronze_holidays</b><br/>feriados<br/>(API Nager.Date)"]
    bronze_weather["<b>bronze_weather</b><br/>clima de los 50 aeropuertos<br/>(API Open-Meteo)"]
    silver_airports_airlines["<b>silver_airports_airlines</b><br/>zona horaria<br/>de cada aeropuerto"]
    silver_holidays["<b>silver_holidays</b><br/>un feriado por fila"]
    silver_weather["<b>silver_weather</b><br/>aeropuerto y hora,<br/>en UTC"]
    silver_flights["<b>silver_flights</b><br/>tipos, horarios en UTC,<br/>flight_id"]
    gold_dimensions["<b>gold_dimensions</b><br/>dim_date, dim_airline,<br/>dim_airport, dim_weather"]
    gold_fact_flights["<b>gold_fact_flights</b><br/>fact_flights:<br/>un vuelo por fila"]
    gold_ml_training["<b>gold_ml_training</b><br/>tabla para entrenar<br/>el modelo de demoras"]
    bronze_csv --> silver_airports_airlines
    bronze_holidays --> silver_holidays
    bronze_weather --> silver_weather
    silver_airports_airlines --> silver_flights
    silver_airports_airlines --> gold_dimensions
    silver_holidays --> gold_dimensions
    silver_weather --> gold_dimensions
    silver_flights --> gold_fact_flights
    gold_dimensions --> gold_fact_flights
    silver_flights --> gold_ml_training
    silver_holidays --> gold_ml_training
    silver_weather --> gold_ml_training
    classDef bronze fill:#f6e0cc,stroke:#a0522d,stroke-width:2px,color:#3b1f0b
    classDef silver fill:#eceff1,stroke:#607d8b,stroke-width:2px,color:#1c2a30
    classDef gold fill:#fff3c4,stroke:#b8860b,stroke-width:2px,color:#3d2c00
    class bronze_csv,bronze_holidays,bronze_weather bronze
    class silver_airports_airlines,silver_flights,silver_holidays,silver_weather silver
    class gold_dimensions,gold_fact_flights,gold_ml_training gold
```

</details>

## Por qué cada dependencia

| Tarea | Espera a | Porque necesita |
|---|---|---|
| `bronze_csv`, `bronze_holidays`, `bronze_weather` | nada | Cada ingesta depende solo de su fuente. `bronze_weather` calcula los 50 aeropuertos con más vuelos leyendo los CSV directo del contenedor `landing`. |
| `silver_airports_airlines` | `bronze_csv` | Los aeropuertos y las aerolíneas crudos. |
| `silver_holidays` | `bronze_holidays` | Los feriados crudos. |
| `silver_weather` | `bronze_weather` | El clima crudo (JSON por aeropuerto). |
| `silver_flights` | `silver_airports_airlines` | La zona horaria de cada aeropuerto, para pasar los horarios a UTC. Los vuelos crudos ya están cargados, porque `silver_airports_airlines` espera a `bronze_csv`. |
| `gold_dimensions` | `silver_airports_airlines`, `silver_holidays`, `silver_weather` | Aeropuertos y aerolíneas, feriados (para `dim_date`) y clima (para `dim_weather`). |
| `gold_fact_flights` | `gold_dimensions`, `silver_flights` | Los vuelos y las dimensiones ya creadas, para controlar que no queden claves huérfanas. |
| `gold_ml_training` | `silver_flights`, `silver_holidays`, `silver_weather` | Vuelos, feriados y clima. No depende de Gold A, así que corre en paralelo con el modelo estrella. |

## Niveles de dependencia

Cada nivel agrupa las tareas que solo esperan a las de niveles anteriores. No son horarios exactos: cada tarea arranca apenas terminan las suyas.

| Nivel | Tareas |
|---|---|
| 1 | `bronze_csv`, `bronze_holidays`, `bronze_weather` |
| 2 | `silver_airports_airlines`, `silver_holidays`, `silver_weather`  |
| 3 | `silver_flights`, `gold_dimensions` |
| 4 | `gold_fact_flights`, `gold_ml_training` |

## Configuración

- **Código:** desde el repositorio de GitHub, rama `main`.
- **Cómputo:** serverless.
- **Identidad:** corre como el service principal `tpi-vuelos-job-sp`.
- **Parámetros:** `catalog`, `start_date`, `end_date`, `landing_path`, `top_n`, `weather_variables`, `year`, `country_code`, `train_end`, `validation_end`, `batch_id = {{job.run_id}}` y `force_fail` (solo para probar el repair run).
- **Si un control de calidad crítico falla,** la tarea se corta antes de escribir y las tareas que dependen de ella no corren.
