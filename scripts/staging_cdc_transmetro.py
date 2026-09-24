import os
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from io import StringIO

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    user=os.getenv("DB_USER", "amt_admin"),
    password=os.getenv("DB_PASSWORD", "amt_secure_pass"),
    dbname=os.getenv("DB_NAME", "amt_dw"),
)
cursor = conn.cursor()

# -------
# 1. DDL
# -------
cursor.execute("""
    CREATE SCHEMA IF NOT EXISTS staging;

    CREATE TABLE IF NOT EXISTS staging.stg_cdc_padron_raw (
        seq INT PRIMARY KEY,
        commit_ts TIMESTAMP,
        op VARCHAR(10),
        tarjeta VARCHAR(50),
        perfil VARCHAR(50),
        zona_residencia VARCHAR(50),
        estado VARCHAR(20)
    );
    TRUNCATE staging.stg_cdc_padron_raw;

    CREATE TABLE IF NOT EXISTS staging.padron_transmetro_actual (
        tarjeta VARCHAR(50) PRIMARY KEY,
        perfil VARCHAR(50),
        zona_residencia VARCHAR(50),
        estado VARCHAR(20),
        es_activa BOOLEAN,
        fecha_actualizacion TIMESTAMP
    );
    TRUNCATE staging.padron_transmetro_actual;

    CREATE INDEX IF NOT EXISTS ix_cdc_raw_op_seq
        ON staging.stg_cdc_padron_raw (op, seq);
""")
conn.commit()

# ---------------
# 2. Carga masiva
# ---------------
df_cdc = pd.read_csv(
    "./scripts/datos_red/cdc_padron_usuarios.csv",
    dtype={"tarjeta": "string", "perfil": "string",
           "zona_residencia": "string", "estado": "string",
           "op": "string"},
    parse_dates=["commit_ts"],
)
df_cdc = df_cdc.sort_values("seq").reset_index(drop=True)
df_cdc["op"] = df_cdc["op"].str.strip().str.upper()

# COPY desde un buffer en memoria
buf = StringIO()
df_cdc.to_csv(buf, index=False, header=False, na_rep="\\N")
buf.seek(0)
cursor.copy_expert(
    """
    COPY staging.stg_cdc_padron_raw
        (seq, commit_ts, op, tarjeta, perfil, zona_residencia, estado)
    FROM STDIN WITH (FORMAT csv, NULL '\\N')
    """,
    buf,
)
conn.commit()

# ---------------------------------------------------------------
# 3. Procesamiento CDC en bloque con SQL set-based
#    Aplica INSERT/UPDATE/DELETE respetando el orden de seq
# ---------------------------------------------------------------

# 3a) UPSERT de todos los INSERT/UPDATE (el último gana por seq)
cursor.execute("""
    WITH ultimo_evento AS (
        SELECT DISTINCT ON (tarjeta)
               seq, commit_ts, op, tarjeta, perfil, zona_residencia, estado
        FROM staging.stg_cdc_padron_raw
        WHERE op IN ('INSERT','UPDATE')
        ORDER BY tarjeta, seq DESC
    )
    INSERT INTO staging.padron_transmetro_actual
        (tarjeta, perfil, zona_residencia, estado, es_activa, fecha_actualizacion)
    SELECT tarjeta, perfil, zona_residencia, estado,
           TRUE, commit_ts
    FROM ultimo_evento
    ON CONFLICT (tarjeta) DO UPDATE
    SET perfil            = COALESCE(EXCLUDED.perfil, staging.padron_transmetro_actual.perfil),
        zona_residencia   = COALESCE(EXCLUDED.zona_residencia, staging.padron_transmetro_actual.zona_residencia),
        estado            = COALESCE(EXCLUDED.estado, staging.padron_transmetro_actual.estado),
        es_activa         = TRUE,
        fecha_actualizacion = EXCLUDED.fecha_actualizacion
    WHERE staging.padron_transmetro_actual.fecha_actualizacion IS NULL
       OR EXCLUDED.fecha_actualizacion >= staging.padron_transmetro_actual.fecha_actualizacion;
""")

# 3b) Soft-delete: solo si el DELETE es posterior al último INSERT/UPDATE
cursor.execute("""
    WITH ultimo_delete AS (
        SELECT DISTINCT ON (tarjeta)
               tarjeta, commit_ts
        FROM staging.stg_cdc_padron_raw
        WHERE op = 'DELETE'
        ORDER BY tarjeta, seq DESC
    )
    UPDATE staging.padron_transmetro_actual p
    SET es_activa = FALSE,
        estado = 'BAJA_CDC',
        fecha_actualizacion = d.commit_ts
    FROM ultimo_delete d
    WHERE p.tarjeta = d.tarjeta
      AND d.commit_ts >= p.fecha_actualizacion;
""")
conn.commit()

# --------------------------
# 4. Métricas una sola query
# --------------------------
cursor.execute("""
    SELECT
        COUNT(*) FILTER (WHERE op = 'INSERT') AS total_inserts,
        COUNT(*) FILTER (WHERE op = 'UPDATE') AS total_updates,
        COUNT(*) FILTER (WHERE op = 'DELETE') AS total_deletes
    FROM staging.stg_cdc_padron_raw;
""")
total_inserts, total_updates, total_deletes = cursor.fetchone()

cursor.execute("""
    SELECT
        COUNT(*) FILTER (WHERE es_activa)      AS activas,
        COUNT(*) FILTER (WHERE NOT es_activa)  AS bajas
    FROM staging.padron_transmetro_actual;
""")
activas, bajas = cursor.fetchone()

os.makedirs("results", exist_ok=True)

# Export
with open("results/fase1_1.2_padron_vigente_transmetro.csv", "w", encoding="utf-8", newline="") as f:
    cursor.copy_expert(
        "COPY (SELECT * FROM staging.padron_transmetro_actual) TO STDOUT WITH (FORMAT csv, HEADER)",
        f,
    )

with open("results/fase1_1.2_resumen_cdc_conteos.md", "w", encoding="utf-8") as f:
    f.write("# Resumen CDC y Catálogos Mínimos (Subfase 1.2)\n\n")
    f.write("## Balance CDC Transmetro\n")
    f.write(f"- **Eventos INSERT:** {total_inserts}\n")
    f.write(f"- **Eventos DELETE:** {total_deletes}\n")
    f.write(f"- **Eventos UPDATE:** {total_updates}\n")
    f.write(f"- **Tarjetas activas:** {activas}\n")
    f.write(f"- **Tarjetas dadas de baja:** {bajas}\n")

cursor.close()
conn.close()