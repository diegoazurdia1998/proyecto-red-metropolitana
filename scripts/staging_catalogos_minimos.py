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
    password=os.getenv("DB_PASSWORD", "amt_secure_pass"),
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

# 1. Transurbano (columna exacta: num_tarjeta)
df_tu = pd.read_csv("./scripts/datos_red/transurbano_transacciones.csv", usecols=["num_tarjeta"])
tu_keys = df_tu["num_tarjeta"].dropna().astype(str).str.strip().unique()
for k in tu_keys:
    cursor.execute("INSERT INTO staging.catalogo_minimo_transurbano (num_tarjeta) VALUES (%s) ON CONFLICT DO NOTHING;",
                   (k,))

# 2. MetroRiel (columna exacta en JSONL: card)
mr_keys = set()
with open("./scripts/datos_red/metroriel_viajes.jsonl", "r", encoding="utf-8") as f:
    for line in f:
        if line.strip():
            row = json.loads(line)
            card = row.get("card")
            if card:
                mr_keys.add(str(card).strip())

for k in mr_keys:
    cursor.execute("INSERT INTO staging.catalogo_minimo_metroriel (card) VALUES (%s) ON CONFLICT DO NOTHING;", (k,))

# 3. Aerómetro (columna exacta: user_hash)
df_am = pd.read_csv("./scripts/datos_red/aerometro_boardings.csv", usecols=["user_hash"])
am_keys = df_am["user_hash"].dropna().astype(str).str.strip().unique()
for k in am_keys:
    cursor.execute(
        "INSERT INTO staging.catalogo_minimo_aerometro (user_hash) VALUES (%s) ON CONFLICT DO NOTHING;",
        (k,)
    )

conn.commit()


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