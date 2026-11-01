"""
Prueba de recorte (rubric 1.3): demuestra que las features de un corte son
IDÉNTICAS si se calculan con el dataset completo vs con el dataset recortado
a la fecha de corte.

Estrategia:
  1. dbt run con dataset completo (schema 'features').
  2. Exportar features a Parquet -> results/p2_recorte_full.parquet
  3. Crear schema 'p2_recorte' con Silver recortado a timestamp < fecha_corte.
  4. dbt run con --vars apuntando al schema recortado (via target 'recorte').
  5. Exportar features recortadas -> results/p2_recorte_cut.parquet
  6. Comparar: 0 diferencias esperadas.
  7. Guardar reporte -> results/p2_fase1_1.3_prueba_recorte.csv

Uso:
    python -m scripts.p2_prueba_recorte --corte A
    python -m scripts.p2_prueba_recorte --corte B
    python -m scripts.p2_prueba_recorte --corte C   # C no tiene etiqueta, pero features sí se prueban
"""
import argparse
import os
import subprocess
import sys
from datetime import datetime

import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

CORTES = {
    "A": "2026-07-01 00:00:00",
    "B": "2026-07-31 00:00:00",
    "C": "2026-08-30 00:00:00",
}

FEATURE_COLS = [
    "sk_usuario",
    "n_abordajes_28d",
    "n_dias_activos_28d",
    "n_modos_usados_28d",
    "pct_fin_semana_28d",
    "n_abordajes_14d",
    "n_abordajes_7d",
    "recencia_dias_ultimo_abordaje",
    "n_estaciones_distintas_28d",
    "pct_hora_pico_28d",
    "dias_desde_alta",
]

RESULTS_DIR = "./results"


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("DB_USER", "amt_admin"),
        password=os.getenv("DB_PASSWORD"),
        dbname=os.getenv("DB_NAME", "amt_dw"),
    )


def dbt_run(select: str, fecha_corte: str, fecha_ref: str, target: str = None):
    """Corre dbt run con vars. target opcional (default = el de profiles.yml)."""
    cmd = [
        "dbt", "run",
        "--project-dir", "dbt_transforms",
        "--profiles-dir", "dbt_transforms",
        "--select", select,
        "--vars",
        f'{{fecha_corte: "{fecha_corte}", fecha_corte_referencia: "{fecha_ref}"}}',
    ]
    if target:
        cmd.extend(["--target", target])
    print(f"▶️  {' '.join(cmd)}")
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print(res.stdout)
        print(res.stderr)
        raise RuntimeError(f"dbt run falló (código {res.returncode})")
    print("✅ dbt run OK")


def export_features(fecha_corte: str, ruta: str, schema: str = "features") -> pd.DataFrame:
    """Exporta p2_features_por_corte para el corte dado a Parquet."""
    sql = f"""
        SELECT * FROM {schema}.p2_features_por_corte
        WHERE fecha_corte = TIMESTAMP '{fecha_corte}'
        ORDER BY sk_usuario
    """
    conn = get_db_connection()
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()
    os.makedirs(RESULTS_DIR, exist_ok=True)
    df.to_parquet(ruta, index=False)
    print(f"✅ Exportado {len(df)} filas a {ruta}")
    return df


def recortar_dataset(fecha_corte: str):
    """
    Crea schema 'p2_recorte' con copias de los silver_*_validado filtradas a
    timestamp < fecha_corte. dbt apuntará ahí via --target recorte (perfiles.yml).
    """
    fecha_ts = datetime.strptime(fecha_corte, "%Y-%m-%d %H:%M:%S")
    conn = get_db_connection()
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute("DROP SCHEMA IF EXISTS p2_recorte CASCADE;")
        cur.execute("CREATE SCHEMA p2_recorte;")

        recortes = {
            "silver_transmetro_validado": ("timestamp_local", fecha_ts),
            "silver_transurbano_validado": ("timestamp_local", fecha_ts),
            "silver_metroriel_validado": ("timestamp_origen", fecha_ts),
            "silver_aerometro_validado": ("timestamp_local", fecha_ts),
        }
        for tabla, (col_ts, corte) in recortes.items():
            cur.execute(f"""
                CREATE TABLE p2_recorte.{tabla} AS
                SELECT * FROM silver.{tabla}
                WHERE {col_ts} < TIMESTAMP '{corte}'
            """)
            cur.execute(f"SELECT COUNT(*) FROM p2_recorte.{tabla}")
            print(f"   {tabla} recortada: {cur.fetchone()[0]} filas")
    finally:
        cur.close()
        conn.close()


def comparar(df_full: pd.DataFrame, df_cut: pd.DataFrame, corte: str) -> dict:
    """Compara dos DataFrames de features. Devuelve dict con diferencias."""
    full = df_full.set_index("sk_usuario").sort_index()
    cut = df_cut.set_index("sk_usuario").sort_index()

    solo_full = full.index.difference(cut.index)
    solo_cut = cut.index.difference(full.index)
    comunes = full.index.intersection(cut.index)

    resultado = {
        "corte": corte,
        "n_full": len(full),
        "n_recortado": len(cut),
        "n_comunes": len(comunes),
        "solo_en_full": len(solo_full),
        "solo_en_recortado": len(solo_cut),
        "filas_con_diferencias": 0,
        "columnas_con_diferencias": "",
        "estado": "OK",
    }

    if len(solo_full) > 0 or len(solo_cut) > 0:
        resultado["estado"] = "DIFERENCIA_UNIVERSO"

    if len(comunes) > 0:
        f_full = full.loc[comunes, FEATURE_COLS[1:]].fillna(-999.0)
        f_cut = cut.loc[comunes, FEATURE_COLS[1:]].fillna(-999.0)
        diff_mask = ~((f_full - f_cut).abs() < 1e-6)
        if diff_mask.any().any():
            cols_diff = diff_mask.any(axis=0)
            resultado["filas_con_diferencias"] = int(diff_mask.any(axis=1).sum())
            resultado["columnas_con_diferencias"] = ", ".join(cols_diff[cols_diff].index)
            resultado["estado"] = "DIFERENCIA_VALORES"

    return resultado


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--corte", choices=list(CORTES.keys()), required=True)
    parser.add_argument("--fecha-ref", default="2026-08-30 00:00:00",
                        help="fecha_corte_referencia para Silver (default: fin del dataset)")
    args = parser.parse_args()

    corte = args.corte
    fecha_corte = CORTES[corte]
    fecha_ref = args.fecha_ref

    print(f"\n{'='*60}")
    print(f" PRUEBA DE RECORTE — Corte {corte} ({fecha_corte})")
    print(f" fecha_corte_referencia = {fecha_ref}")
    print(f"{'='*60}\n")

    # 1. Correr features con dataset completo
    print("▶️  Paso 1/5: dbt run features (dataset completo)")
    dbt_run("features", fecha_corte, fecha_ref)
    df_full = export_features(
        fecha_corte, os.path.join(RESULTS_DIR, f"p2_recorte_{corte}_full.parquet")
    )

    # 2. Recortar dataset a Silver < fecha_corte
    print(f"\n▶️  Paso 2/5: recortar dataset a < {fecha_corte}")
    recortar_dataset(fecha_corte)

    # 3. Correr features apuntando al schema recortado (target 'recorte' debe existir en profiles.yml)
    print(f"\n▶️  Paso 3/5: dbt run features (target='recorte')")
    dbt_run("features", fecha_corte, fecha_ref, target="recorte")
    df_cut = export_features(
        fecha_corte, os.path.join(RESULTS_DIR, f"p2_recorte_{corte}_cut.parquet"),
        schema="features",  # el macro generate_schema_name del P1 fuerza 'features' para todo target
    )

    # 4. Comparar
    print(f"\n▶️  Paso 4/5: comparar")
    resultado = comparar(df_full, df_cut, corte)

    # 5. Guardar reporte
    print(f"\n▶️  Paso 5/6: guardar reporte")
    ruta_csv = os.path.join(
        RESULTS_DIR,
        "p2_fase1_1.3_prueba_recorte.csv"
    )

    # Leer archivo existente si existe
    if os.path.exists(ruta_csv):
        df_prev = pd.read_csv(ruta_csv)

        # Eliminar registros previos del mismo corte
        df_prev = df_prev[df_prev["corte"] != corte]
    else:
        df_prev = pd.DataFrame()

    # Agregar el resultado actual
    df_actual = pd.DataFrame([resultado])

    # Unir datos antiguos + resultado nuevo
    df_final = pd.concat(
        [df_prev, df_actual],
        ignore_index=True
    )

    # Sobrescribir archivo completo
    df_final.to_csv(
        ruta_csv,
        index=False,
        encoding="utf-8"
    )

    # 6. Restaurar features completas en la DB y limpiar schema p2_recorte
    print(f"\n▶️  Paso 6/6: restaurar features completas y limpiar p2_recorte")
    dbt_run("features", fecha_corte, fecha_ref)  # target dev, regenera completo
    conn = get_db_connection()
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute("DROP SCHEMA IF EXISTS p2_recorte CASCADE;")
        print("✅ Schema p2_recorte eliminado.")
    finally:
        cur.close()
        conn.close()

    print(f"\n{'=' * 60}")
    print(f" RESULTADO CORTE {corte}")
    print(f"{'=' * 60}")
    for k, v in resultado.items():
        print(f"  {k:<30}: {v}")
    print(f"{'=' * 60}\n")

    if resultado["estado"] != "OK":
        print("❌ PRUEBA FALLIDA")
        sys.exit(1)
    print("✅ PRUEBA SUPERADA (0 diferencias)")

if __name__ == "__main__":
    main()