import os
import psycopg2
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

cursor.execute("""
    CREATE SCHEMA IF NOT EXISTS silver;

    CREATE TABLE IF NOT EXISTS silver.cuarentena (
        id_cuarentena BIGSERIAL PRIMARY KEY,
        fuente VARCHAR(50) NOT NULL,
        regla_violada VARCHAR(100) NOT NULL,
        registro_crudo JSONB NOT NULL,
        timestamp_rechazo TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_cuarentena_regla ON silver.cuarentena(regla_violada);
    CREATE INDEX IF NOT EXISTS idx_cuarentena_fuente ON silver.cuarentena(fuente);
""")
conn.commit()
print("Esquema 'silver' y tabla 'cuarentena' creados con éxito.")

cursor.close()
conn.close()