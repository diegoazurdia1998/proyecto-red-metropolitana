"""
Exporta features + etiqueta + en_poblacion para el corte A a Parquet (evidencia).

Pasos:
  1. dbt run --select features --vars {fecha_corte: '2026-07-01 00:00:00'}
  2. Lee features.p2_features_etiqueta_por_corte WHERE fecha_corte = A AND en_poblacion
  3. Exporta a results/p2_features_A.parquet

Uso:
    python -m scripts.p2_exportar_features_A
"""
import os
import subprocess
import sys

import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

RESULTS_DIR = "./results"
CORTE_A = "2026-07-01 00:00:00"
FECHA_REF = "2026-08-30 00:00:00"


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("DB_USER", "amt_admin"),
        password=os.getenv("DB_PASSWORD"),
        dbname=os.getenv("DB_NAME", "amt_dw"),
    )


def dbt_run_features_A():
    cmd = [
        "dbt", "run",
        "--project-dir", "dbt_transforms",
        "--profiles-dir", "dbt_transforms",
        "--select", "features",
        "--vars",
        f'{{fecha_corte: "{CORTE_A}", fecha_corte_referencia: "{FECHA_REF}"}}',
    ]
    print(f"▶️  {' '.join(cmd)}")
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print(res.stdout)
        print(res.stderr)
        raise RuntimeError(f"dbt run falló (código {res.returncode})")
    print("✅ dbt run features corte A OK")


def exportar_parquet():
    sql = f"""
        SELECT *
        FROM features.p2_features_etiqueta_por_corte
        WHERE fecha_corte = TIMESTAMP '{CORTE_A}'
          AND en_poblacion = TRUE
          AND etiqueta IS NOT NULL
        ORDER BY sk_usuario
    """
    conn = get_db_connection()
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()

    if df.empty:
        print("❌ DataFrame vacío. Revisa que dbt haya corrido bien y que la población exista.")
        sys.exit(1)

    print(f"   Filas: {len(df):,}")
    print(f"   Positivos: {int(df['etiqueta'].sum()):,}")
    print(f"   Tasa positiva: {100.0 * df['etiqueta'].mean():.2f}%")

    os.makedirs(RESULTS_DIR, exist_ok=True)
    ruta = os.path.join(RESULTS_DIR, "p2_features_A.parquet")
    df.to_parquet(ruta, index=False)
    print(f"✅ Parquet: {ruta}")
    return df


def main():
    print("=" * 60)
    print(" EXPORTAR FEATURES CORTE A (evidencia 1.4)")
    print("=" * 60)
    dbt_run_features_A()
    exportar_parquet()
    print("=" * 60)


if __name__ == "__main__":
    main()