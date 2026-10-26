"""DAG: OpenWeatherMap -> raw -> staging -> star schema (dim + fact) -> mart.

Urutan task:
  create_raw_table -> load_raw -> build_staging
  -> [dim_city, dim_date, dim_time, dim_condition] -> fact_weather
  -> [weather_daily, weather_latest]
"""
import json
from datetime import datetime, timedelta

import requests
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

try:  # Airflow 3
    from airflow.sdk import DAG, Variable, task
except ImportError:  # Airflow 2
    from airflow import DAG
    from airflow.decorators import task
    from airflow.models import Variable

PG_CONN_ID = "warehouse_pg"
API_URL = "https://api.openweathermap.org/data/2.5/weather"

# Satu-satunya tempat daftar kota: nama kota -> (provinsi, pulau).
# Menambah kota? Tambahkan di sini, provinsi dan pulau ikut dicatat di dim_city.
CITIES = {
    "Jakarta": ("DKI Jakarta", "Jawa"),
    "Bandung": ("Jawa Barat", "Jawa"),
    "Surabaya": ("Jawa Timur", "Jawa"),
    "Medan": ("Sumatera Utara", "Sumatera"),
}
CITY_VALUES = ", ".join(
    f"('{city}', '{prov}', '{island}')" for city, (prov, island) in CITIES.items()
)

CREATE_RAW = """
CREATE TABLE IF NOT EXISTS raw.weather_current (
    id          BIGSERIAL PRIMARY KEY,
    city        TEXT NOT NULL,
    payload     JSONB NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

STAGING_SQL = [
    "DROP TABLE IF EXISTS staging.weather_current;",
    """
    CREATE TABLE staging.weather_current AS
    WITH parsed AS (
        SELECT
            city,
            payload->'sys'->>'country'                   AS country,
            (payload->'coord'->>'lat')::numeric          AS lat,
            (payload->'coord'->>'lon')::numeric          AS lon,
            to_timestamp((payload->>'dt')::bigint)       AS observed_at,
            (payload->'main'->>'temp')::numeric          AS temp_c,
            (payload->'main'->>'feels_like')::numeric    AS feels_like_c,
            (payload->'main'->>'humidity')::int          AS humidity_pct,
            (payload->'main'->>'pressure')::int          AS pressure_hpa,
            (payload->'wind'->>'speed')::numeric         AS wind_ms,
            (payload->'weather'->0->>'id')::int          AS condition_id,
            payload->'weather'->0->>'main'               AS weather_main,
            payload->'weather'->0->>'description'        AS weather_desc,
            ingested_at
        FROM raw.weather_current
    )
    SELECT DISTINCT ON (city, observed_at)
        city, country, lat, lon, observed_at, temp_c, feels_like_c,
        humidity_pct, pressure_hpa, wind_ms,
        condition_id, weather_main, weather_desc
    FROM parsed
    ORDER BY city, observed_at, ingested_at DESC;
    """,
]

DIM_CITY_SQL = [
    "DROP TABLE IF EXISTS mart.dim_city;",
    f"""
    CREATE TABLE mart.dim_city AS
    WITH geo AS (
        SELECT city, MAX(lat) AS lat, MAX(lon) AS lon
        FROM staging.weather_current
        GROUP BY city
    ),
    ref AS (
        SELECT * FROM (VALUES {CITY_VALUES}) AS t(city_name, province, island)
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY geo.city)::int AS city_key,
        geo.city      AS city_name,
        ref.province,
        ref.island,
        geo.lat,
        geo.lon
    FROM geo
    LEFT JOIN ref ON ref.city_name = geo.city;
    """,
]

DIM_DATE_SQL = [
    "DROP TABLE IF EXISTS mart.dim_date;",
    """
    CREATE TABLE mart.dim_date AS
    WITH bounds AS (
        SELECT
            MIN((observed_at AT TIME ZONE 'Asia/Jakarta')::date) AS d_min,
            MAX((observed_at AT TIME ZONE 'Asia/Jakarta')::date) AS d_max
        FROM staging.weather_current
    ),
    days AS (
        SELECT g::date AS full_date
        FROM bounds,
             generate_series(d_min::timestamp, d_max::timestamp, interval '1 day') AS g
    )
    SELECT
        to_char(full_date, 'YYYYMMDD')::int AS date_key,
        full_date,
        CASE EXTRACT(ISODOW FROM full_date)::int
            WHEN 1 THEN 'Senin' WHEN 2 THEN 'Selasa' WHEN 3 THEN 'Rabu'
            WHEN 4 THEN 'Kamis' WHEN 5 THEN 'Jumat' WHEN 6 THEN 'Sabtu'
            ELSE 'Minggu'
        END AS day_name,
        EXTRACT(MONTH FROM full_date)::int AS month_num,
        (ARRAY['Januari','Februari','Maret','April','Mei','Juni','Juli',
               'Agustus','September','Oktober','November','Desember']
        )[EXTRACT(MONTH FROM full_date)::int] AS month_name,
        EXTRACT(YEAR FROM full_date)::int AS year,
        EXTRACT(ISODOW FROM full_date)::int IN (6, 7) AS is_weekend
    FROM days;
    """,
]

DIM_TIME_SQL = [
    "DROP TABLE IF EXISTS mart.dim_time;",
    """
    CREATE TABLE mart.dim_time AS
    SELECT
        h AS hour_key,
        to_char(h, 'FM00') || ':00' AS hour_label,
        CASE
            WHEN h BETWEEN 0 AND 3   THEN 'Dini hari'
            WHEN h BETWEEN 4 AND 10  THEN 'Pagi'
            WHEN h BETWEEN 11 AND 14 THEN 'Siang'
            WHEN h BETWEEN 15 AND 17 THEN 'Sore'
            ELSE 'Malam'
        END AS part_of_day,
        CASE
            WHEN h BETWEEN 0 AND 3   THEN 1
            WHEN h BETWEEN 4 AND 10  THEN 2
            WHEN h BETWEEN 11 AND 14 THEN 3
            WHEN h BETWEEN 15 AND 17 THEN 4
            ELSE 5
        END AS part_order
    FROM generate_series(0, 23) AS h;
    """,
]

DIM_CONDITION_SQL = [
    "DROP TABLE IF EXISTS mart.dim_condition;",
    """
    CREATE TABLE mart.dim_condition AS
    SELECT DISTINCT ON (condition_id)
        condition_id  AS condition_key,
        weather_main  AS main,
        weather_desc  AS description
    FROM staging.weather_current
    WHERE condition_id IS NOT NULL
    ORDER BY condition_id, observed_at DESC;
    """,
]

FACT_SQL = [
    "DROP TABLE IF EXISTS mart.fact_weather;",
    """
    CREATE TABLE mart.fact_weather AS
    SELECT
        c.city_key,
        to_char(s.observed_at AT TIME ZONE 'Asia/Jakarta', 'YYYYMMDD')::int AS date_key,
        EXTRACT(HOUR FROM s.observed_at AT TIME ZONE 'Asia/Jakarta')::int   AS hour_key,
        s.condition_id AS condition_key,
        s.observed_at,
        s.temp_c,
        s.feels_like_c,
        s.humidity_pct,
        s.pressure_hpa,
        s.wind_ms
    FROM staging.weather_current s
    JOIN mart.dim_city c ON c.city_name = s.city;
    """,
]

MART_DAILY_SQL = [
    "DROP TABLE IF EXISTS mart.weather_daily;",
    """
    CREATE TABLE mart.weather_daily AS
    SELECT
        c.city_name                     AS city,
        d.full_date                     AS obs_date,
        ROUND(AVG(f.temp_c), 1)         AS avg_temp_c,
        MIN(f.temp_c)                   AS min_temp_c,
        MAX(f.temp_c)                   AS max_temp_c,
        ROUND(AVG(f.humidity_pct), 0)   AS avg_humidity_pct,
        MAX(f.wind_ms)                  AS max_wind_ms,
        COUNT(*)                        AS observations
    FROM mart.fact_weather f
    JOIN mart.dim_city c USING (city_key)
    JOIN mart.dim_date d USING (date_key)
    GROUP BY c.city_name, d.full_date;
    """,
]

MART_LATEST_SQL = [
    "DROP TABLE IF EXISTS mart.weather_latest;",
    """
    CREATE TABLE mart.weather_latest AS
    SELECT DISTINCT ON (c.city_name)
        c.city_name   AS city,
        f.observed_at,
        f.temp_c,
        f.feels_like_c,
        f.humidity_pct,
        k.main        AS weather_main,
        k.description AS weather_desc
    FROM mart.fact_weather f
    JOIN mart.dim_city c USING (city_key)
    LEFT JOIN mart.dim_condition k ON k.condition_key = f.condition_key
    ORDER BY c.city_name, f.observed_at DESC;
    """,
]


def sql_task(task_id, sql):
    return SQLExecuteQueryOperator(task_id=task_id, conn_id=PG_CONN_ID, sql=sql)


with DAG(
    dag_id="weather_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["weather", "etl", "star-schema"],
):
    create_raw = sql_task("create_raw_table", CREATE_RAW)

    @task
    def load_raw():
        api_key = Variable.get("owm_api_key")
        rows = []
        for city in CITIES:
            resp = requests.get(
                API_URL,
                params={"q": city, "appid": api_key, "units": "metric"},
                timeout=30,
            )
            resp.raise_for_status()
            rows.append((city, json.dumps(resp.json())))
        PostgresHook(postgres_conn_id=PG_CONN_ID).insert_rows(
            table="raw.weather_current",
            rows=rows,
            target_fields=["city", "payload"],
        )

    build_staging = sql_task("build_staging", STAGING_SQL)

    dims = [
        sql_task("build_dim_city", DIM_CITY_SQL),
        sql_task("build_dim_date", DIM_DATE_SQL),
        sql_task("build_dim_time", DIM_TIME_SQL),
        sql_task("build_dim_condition", DIM_CONDITION_SQL),
    ]
    build_fact = sql_task("build_fact_weather", FACT_SQL)
    marts = [
        sql_task("build_mart_daily", MART_DAILY_SQL),
        sql_task("build_mart_latest", MART_LATEST_SQL),
    ]

    create_raw >> load_raw() >> build_staging
    build_staging >> dims
    dims >> build_fact
    build_fact >> marts
