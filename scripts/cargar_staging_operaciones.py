import os
import json
import time
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    user=os.getenv("DB_USER", "amt_admin"),
    password=os.getenv("DB_PASSWORD"),
    dbname=os.getenv("DB_NAME", "amt_dw"),
)
cursor = conn.cursor()
cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")
conn.commit()

BATCH = 5000  # filas por lote

def cargar_tabla(nombre, df, columnas, insert_sql):
    """Carga un DataFrame en staging.<nombre> en lotes con logs de progreso."""

    t0 = time.time()
    total = len(df)
    print(
        f"{nombre}: {total:,} filas a insertar (lotes de {BATCH:,})",
        flush=True
    )

    insertados = 0
    for i in range(0, total, BATCH):

        lote_df = df.iloc[i:i+BATCH]
        lote = list(
            lote_df.itertuples(
                index=False,
                name=None
            )
        )

        execute_values(
            cursor,
            insert_sql,
            lote,
            page_size=BATCH
        )

        conn.commit()

        insertados += len(lote)

        pct = 100 * insertados / total

        print(
            f"   [{nombre}] {insertados:,}/{total:,} ({pct:5.1f}%) — {time.time()-t0:.1f}s",
            flush=True
        )

    print(
        f"{nombre} completado en {time.time()-t0:.1f}s",
        flush=True
    )


# ------------------------------------------------------------------
# 0. Catálogos de Estaciones y Paradas (desde Bronze Parquet)
# ------------------------------------------------------------------
from bronze_reader import leer_parquet_bronze

print("Cargando staging.tm_estaciones...", flush=True)
df_cat_tm = leer_parquet_bronze("tm_estaciones")
cursor.execute("""
    DROP TABLE IF EXISTS staging.tm_estaciones CASCADE;
    CREATE TABLE staging.tm_estaciones (
    estacion_id VARCHAR(20),
    nombre VARCHAR(100),
    linea VARCHAR(10),
    zona VARCHAR(20),
    latitud NUMERIC(10,6),
    longitud NUMERIC(10,6)
);
""")
conn.commit()
cargar_tabla("tm_estaciones", df_cat_tm, df_cat_tm.columns, "INSERT INTO staging.tm_estaciones VALUES %s")

print("Cargando staging.tu_paradas...", flush=True)
df_cat_tu = leer_parquet_bronze("tu_paradas")
cursor.execute("""
    DROP TABLE IF EXISTS staging.tu_paradas CASCADE;
    CREATE TABLE staging.tu_paradas (
    cod_parada VARCHAR(20),
    descripcion VARCHAR(100),
    ruta VARCHAR(20),
    sector VARCHAR(20)
);
""")
conn.commit()
cargar_tabla("tu_paradas", df_cat_tu, df_cat_tu.columns, "INSERT INTO staging.tu_paradas VALUES %s")

print("Cargando staging.mr_estaciones...", flush=True)
df_cat_mr = leer_parquet_bronze("mr_estaciones")
cursor.execute("""
    DROP TABLE IF EXISTS staging.mr_estaciones CASCADE;
    CREATE TABLE staging.mr_estaciones (
    id_estacion INTEGER,
    nombre_estacion VARCHAR(100),
    zona_nombre VARCHAR(20),
    kilometro NUMERIC(10,2)
);
""")
conn.commit()
cargar_tabla("mr_estaciones", df_cat_mr, df_cat_mr.columns, "INSERT INTO staging.mr_estaciones VALUES %s")

print("Cargando staging.am_estaciones...", flush=True)
df_cat_am = leer_parquet_bronze("am_estaciones")
cursor.execute("""
    DROP TABLE IF EXISTS staging.am_estaciones CASCADE;
    CREATE TABLE staging.am_estaciones (
    station_code VARCHAR(20),
    station_name VARCHAR(100),
    axis VARCHAR(20),
    district VARCHAR(50)
);
""")
conn.commit()
cargar_tabla("am_estaciones", df_cat_am, df_cat_am.columns, "INSERT INTO staging.am_estaciones VALUES %s")

# ------------------------------------------------------------------
# 1. Aerómetro Boardings
# ------------------------------------------------------------------
print("Cargando staging.aerometro_boardings...", flush=True)
from bronze_reader import leer_parquet_bronze
df_am = leer_parquet_bronze("aerometro_boardings")
df_am = df_am[["boarding_id", "user_hash", "station_code", "axis", "timestamp_utc", "cabin_number", "fare"]]
cursor.execute("""
    DROP TABLE IF EXISTS staging.aerometro_boardings;
    CREATE TABLE staging.aerometro_boardings (
        boarding_id INT,
        user_hash VARCHAR(50),
        station_code VARCHAR(50),
        axis VARCHAR(50),
        timestamp_utc VARCHAR(50),
        cabin_number INT,
        fare NUMERIC(10,2)
    );
""")
conn.commit()

cargar_tabla(
    "aerometro_boardings", df_am, df_am.columns,
    "INSERT INTO staging.aerometro_boardings VALUES %s"
)

# ------------------------------------------------------------------
# 2. Transmetro Validaciones
# ------------------------------------------------------------------
print("Cargando staging.transmetro_validaciones...", flush=True)
df_tm = leer_parquet_bronze("transmetro_validaciones")

# Renombrar columnas originales al esquema de staging si vienen con nombres de archivo crudo
mapeo_cols = {
    "validacion_id": "id_validacion",
    "estacion_id": "estacion",
    "fecha_hora": "timestamp_validacion",
    "tarifa": "monto_gtq"
}
df_tm = df_tm.rename(columns=mapeo_cols)

cursor.execute("""
    DROP TABLE IF EXISTS staging.transmetro_validaciones;
    CREATE TABLE staging.transmetro_validaciones (
        id_validacion VARCHAR(50),
        tarjeta VARCHAR(50),
        estacion VARCHAR(100),
        linea VARCHAR(50),
        timestamp_validacion VARCHAR(50),
        monto_gtq NUMERIC(10,2)
    );
""")
conn.commit()

# Seleccionar en el orden exacto del DDL
df_tm_6 = df_tm[["id_validacion", "tarjeta", "estacion", "linea", "timestamp_validacion", "monto_gtq"]]
cargar_tabla(
    "transmetro_validaciones", df_tm_6, df_tm_6.columns,
    "INSERT INTO staging.transmetro_validaciones VALUES %s"
)

# ------------------------------------------------------------------
# 3. Transurbano Transacciones
# ------------------------------------------------------------------
print("Cargando staging.transurbano_transacciones...", flush=True)
df_tu = leer_parquet_bronze("transurbano_transacciones")
df_tu_7 = df_tu[["fecha", "hora", "num_tarjeta", "cod_parada", "ruta", "monto_centavos", "cod_estado"]]
cursor.execute("""
    DROP TABLE IF EXISTS staging.transurbano_transacciones;
    CREATE TABLE staging.transurbano_transacciones (
        fecha VARCHAR(20),
        hora VARCHAR(20),
        num_tarjeta VARCHAR(50),
        cod_parada VARCHAR(50),
        ruta VARCHAR(50),
        monto_centavos INT,
        cod_estado INT
    );
""")
conn.commit()

df_tu_7 = df_tu.iloc[:, :7]
cargar_tabla(
    "transurbano_transacciones", df_tu_7, df_tu_7.columns,
    "INSERT INTO staging.transurbano_transacciones VALUES %s"
)

# ------------------------------------------------------------------
# 4. MetroRiel Viajes (JSONL — se procesa en streaming)
# ------------------------------------------------------------------
print("Cargando staging.metroriel_viajes...", flush=True)
cursor.execute("""
    DROP TABLE IF EXISTS staging.metroriel_viajes;
    CREATE TABLE staging.metroriel_viajes (
        trip_id INT,
        card VARCHAR(50),
        entry JSONB,
        exit JSONB,
        fare_gtq NUMERIC(10,2),
        duration_s INT
    );
""")
conn.commit()


df_mr = leer_parquet_bronze("metroriel_viajes")

t0 = time.time()
lote = []
insertados = 0

# Verificar si viene con prefijo 'entry.' por pd.json_normalize de Bronze
tiene_entry_station = "entry.station" in df_mr.columns

for _, row in df_mr.iterrows():
    if tiene_entry_station:
        entry_obj = {
            "station": row.get("entry.station"),
            "ts": str(row.get("entry.ts"))
        }
        exit_obj = {
            "station": row.get("exit.station"),
            "ts": str(row.get("exit.ts"))
        } if pd.notna(row.get("exit.station")) else None
    else:
        # Si guardó el objeto dict original
        entry_obj = row.get("entry")
        exit_obj = row.get("exit")

    lote.append((
        int(row.get("trip_id")),
        str(row.get("card")),
        json.dumps(entry_obj) if entry_obj is not None else None,
        json.dumps(exit_obj) if exit_obj is not None else None,
        float(row.get("fare_gtq")),
        int(row.get("duration_s")) if pd.notna(row.get("duration_s")) else None,
    ))

    if len(lote) >= BATCH:
        execute_values(
            cursor,
            "INSERT INTO staging.metroriel_viajes (trip_id, card, entry, exit, fare_gtq, duration_s) VALUES %s",
            lote, page_size=BATCH
        )
        conn.commit()
        insertados += len(lote)
        print(f"   [metroriel_viajes] {insertados:,} filas — {time.time()-t0:.1f}s", flush=True)
        lote = []

if lote:
    execute_values(
        cursor,
        "INSERT INTO staging.metroriel_viajes (trip_id, card, entry, exit, fare_gtq, duration_s) VALUES %s",
        lote, page_size=BATCH
    )
    conn.commit()
    insertados += len(lote)

print(f"metroriel_viajes completado: {insertados:,} filas en {time.time()-t0:.1f}s", flush=True)

cursor.close()
conn.close()
print("Todas las tablas operativas de staging han sido pobladas exitosamente.", flush=True)