import os
import json
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    user=os.getenv("DB_USER", "amt_admin"),
    password=os.getenv("DB_PASSWORD"),
    dbname=os.getenv("DB_NAME", "amt_dw")
)
cursor = conn.cursor()

# Tablas exclusivas para identificadores únicos
cursor.execute("""
    CREATE SCHEMA IF NOT EXISTS staging;

    CREATE TABLE IF NOT EXISTS staging.catalogo_minimo_transurbano (
        num_tarjeta VARCHAR(50) PRIMARY KEY
    );
    CREATE TABLE IF NOT EXISTS staging.catalogo_minimo_metroriel (
        card VARCHAR(50) PRIMARY KEY
    );
    CREATE TABLE IF NOT EXISTS staging.catalogo_minimo_aerometro (
        user_hash VARCHAR(50) PRIMARY KEY
    );

    TRUNCATE staging.catalogo_minimo_transurbano;
    TRUNCATE staging.catalogo_minimo_metroriel;
    TRUNCATE staging.catalogo_minimo_aerometro;
""")
conn.commit()

from bronze_reader import leer_parquet_bronze

# 1. Transurbano (desde Bronze Parquet)
print("Cargando transurbano_transacciones...", flush=True)

df_tu = leer_parquet_bronze("transurbano_transacciones")
print(f"   {len(df_tu):,} registros leídos", flush=True)

tu_keys = (
    df_tu["num_tarjeta"]
    .dropna()
    .astype(str)
    .str.strip()
    .unique()
)

print(f"   {len(tu_keys):,} usuarios únicos encontrados", flush=True)

for i, k in enumerate(tu_keys, start=1):
    cursor.execute(
        """
        INSERT INTO staging.catalogo_minimo_transurbano (num_tarjeta)
        VALUES (%s)
        ON CONFLICT DO NOTHING;
        """,
        (k,)
    )

    if i % 10000 == 0:
        print(
            f"   [Transurbano] {i:,}/{len(tu_keys):,} usuarios",
            flush=True
        )

print("   Transurbano completado", flush=True)

# 2. MetroRiel (desde Bronze Parquet)
print("Cargando metroriel_viajes...", flush=True)

df_mr = leer_parquet_bronze("metroriel_viajes")
print(f"   {len(df_mr):,} registros leídos", flush=True)

mr_keys = (
    df_mr["card"]
    .dropna()
    .astype(str)
    .str.strip()
    .unique()
)

print(f"   {len(mr_keys):,} usuarios únicos encontrados", flush=True)

for i, k in enumerate(mr_keys, start=1):
    cursor.execute(
        """
        INSERT INTO staging.catalogo_minimo_metroriel (card)
        VALUES (%s)
        ON CONFLICT DO NOTHING;
        """,
        (k,)
    )

    if i % 10000 == 0:
        print(
            f"   [MetroRiel] {i:,}/{len(mr_keys):,} usuarios",
            flush=True
        )

print("   MetroRiel completado", flush=True)

# 3. Aerómetro (desde Bronze Parquet)
print("Cargando aerometro_boardings...", flush=True)

df_am = leer_parquet_bronze("aerometro_boardings")
print(f"   {len(df_am):,} registros leídos", flush=True)

am_keys = (
    df_am["user_hash"]
    .dropna()
    .astype(str)
    .str.strip()
    .unique()
)

print(f"   {len(am_keys):,} usuarios únicos encontrados", flush=True)

for i, k in enumerate(am_keys, start=1):
    cursor.execute(
        """
        INSERT INTO staging.catalogo_minimo_aerometro (user_hash)
        VALUES (%s)
        ON CONFLICT DO NOTHING;
        """,
        (k,)
    )

    if i % 10000 == 0:
        print(
            f"   [Aerómetro] {i:,}/{len(am_keys):,} usuarios",
            flush=True
        )

print("   Aerómetro completado", flush=True)

print("Confirmando cambios...", flush=True)
conn.commit()
print("Commit completado", flush=True)


os.makedirs("results", exist_ok=True)
# Exportar consolidado de catálogos mínimos a CSV
query_cat = """
    SELECT 'Transurbano' AS sistema, num_tarjeta AS id_usuario FROM staging.catalogo_minimo_transurbano
    UNION ALL
    SELECT 'MetroRiel' AS sistema, card AS id_usuario FROM staging.catalogo_minimo_metroriel
    UNION ALL
    SELECT 'Aerometro' AS sistema, user_hash AS id_usuario FROM staging.catalogo_minimo_aerometro;
"""
pd.read_sql(query_cat, conn).to_csv(
    "results/fase1_1.2_catalogos_usuarios_transurbano_metroriel_aerometro.csv",
    index=False
)

# Anexar métricas al Markdown
with open("results/fase1_1.2_resumen_cdc_conteos.md", "a", encoding="utf-8") as f:
    f.write("\n## Usuarios Únicos por Sistema\n")
    f.write(f"- **Transurbano:** {len(tu_keys)}\n")
    f.write(f"- **MetroRiel:** {len(mr_keys)}\n")
    f.write(f"- **Aerómetro:** {len(am_keys)}\n")

cursor.close()
conn.close()