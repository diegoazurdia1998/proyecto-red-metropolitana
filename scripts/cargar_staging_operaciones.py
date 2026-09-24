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
    password=os.getenv("DB_PASSWORD", "amt_secure_pass"),
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
    print(f"{nombre}: {total:,} filas a insertar (lotes de {BATCH:,})", flush=True)

    filas = list(df.itertuples(index=False, name=None))
    insertados = 0
    for i in range(0, total, BATCH):
        lote = filas[i:i+BATCH]
        execute_values(cursor, insert_sql, lote, page_size=BATCH)
        conn.commit()
        insertados += len(lote)
        pct = 100 * insertados / total
        print(f"   [{nombre}] {insertados:,}/{total:,} ({pct:5.1f}%) — {time.time()-t0:.1f}s", flush=True)

    print(f"{nombre} completado en {time.time()-t0:.1f}s", flush=True)


# ------------------------------------------------------------------
# 1. Aerómetro Boardings
# ------------------------------------------------------------------
print("Cargando staging.aerometro_boardings...", flush=True)
df_am = pd.read_csv("./scripts/datos_red/aerometro_boardings.csv")
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
df_tm = pd.read_csv("./scripts/datos_red/transmetro_validaciones.csv")
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

df_tm_6 = df_tm.iloc[:, :6]  # solo las 6 primeras columnas que usa el INSERT
cargar_tabla(
    "transmetro_validaciones", df_tm_6, df_tm_6.columns,
    "INSERT INTO staging.transmetro_validaciones VALUES %s"
)

# ------------------------------------------------------------------
# 3. Transurbano Transacciones
# ------------------------------------------------------------------
print("Cargando staging.transurbano_transacciones...", flush=True)
df_tu = pd.read_csv("./scripts/datos_red/transurbano_transacciones.csv")
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

t0 = time.time()
lote = []
insertados = 0
with open("./scripts/datos_red/metroriel_viajes.jsonl", "r", encoding="utf-8") as f:
    for line in f:
        if not line.strip():
            continue
        row = json.loads(line)
        lote.append((
            row.get("trip_id"),
            row.get("card"),
            json.dumps(row.get("entry")),
            json.dumps(row.get("exit")),
            row.get("fare_gtq"),
            row.get("duration_s"),
        ))
        if len(lote) >= BATCH:
            execute_values(cursor,
                "INSERT INTO staging.metroriel_viajes (trip_id, card, entry, exit, fare_gtq, duration_s) VALUES %s",
                lote, page_size=BATCH)
            conn.commit()
            insertados += len(lote)
            print(f"   [metroriel_viajes] {insertados:,} filas — {time.time()-t0:.1f}s", flush=True)
            lote = []

    if lote:
        execute_values(cursor,
            "INSERT INTO staging.metroriel_viajes (trip_id, card, entry, exit, fare_gtq, duration_s) VALUES %s",
            lote, page_size=BATCH)
        conn.commit()
        insertados += len(lote)

print(f"metroriel_viajes completado: {insertados:,} filas en {time.time()-t0:.1f}s", flush=True)

cursor.close()
conn.close()
print("Todas las tablas operativas de staging han sido pobladas exitosamente.", flush=True)