"""
CLI de predicción (rubric 2.4).

Uso:
    python -m scripts.predecir --fecha-corte 2026-08-30

Qué hace:
  1. Carga el modelo HGB de 2.2 (modelos_entrenados/p2_hgb_v1.0.0.pkl).
  2. Verifica que features.p2_features_etiqueta_por_corte tenga el corte pedido.
  3. Lee features de la DB para ese corte y población elegible.
  4. Predice puntaje con HGB.
  5. Aplica umbral 0.3333 (derivado en 2.2) → contactar ∈ {0, 1}.
  6. Resuelve la lista de tarjetas asociadas a cada sk_usuario consultando Silver.
  7. Escribe results/p2_predicciones_<fecha>.csv con:
       unidad_id, tarjetas, puntaje, contactar.

Asume que el pipeline ya preparó los features del corte pedido.
Si el corte no está en la DB, aborta con instrucciones.
"""
import argparse
import os
import subprocess
import sys

import joblib
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

RESULTS_DIR = "./results"
MODELOS_DIR = "./modelos_entrenados"
MODELO_HGB = os.path.join(MODELOS_DIR, "p2_hgb_v1.0.0.pkl")

UMBRAL = 0.3333   # derivado en 2.2: p* = 5 / (60 × 0.25)

FEATURE_COLS = [
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


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("DB_USER", "amt_admin"),
        password=os.getenv("DB_PASSWORD"),
        dbname=os.getenv("DB_NAME", "amt_dw"),
    )


def materializar_corte(fecha_corte: str, fecha_referencia: str = "2026-08-30 00:00:00"):
    """Corre dbt para materializar el corte pedido en features.p2_features_etiqueta_por_corte.

    Como la tabla es materialized='table', solo guarda UN corte a la vez.
    Antes de predecir, nos aseguramos de que el corte pedido esté materializado.
    """
    cmd = [
        "dbt", "run",
        "--project-dir", "dbt_transforms",
        "--profiles-dir", "dbt_transforms",
        "--select", "features",
        "--vars",
        f'{{"fecha_corte": "{fecha_corte}", "fecha_corte_referencia": "{fecha_referencia}"}}',
    ]
    print(f"▶️  Materializando corte {fecha_corte} vía dbt...", flush=True)
    print(f"   {' '.join(cmd)}", flush=True)
    res = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if res.returncode != 0:
        print(res.stdout)
        print(res.stderr)
        raise RuntimeError(f"dbt run features falló (código {res.returncode})")
    print(f"✅ Corte {fecha_corte} materializado en features.p2_features_etiqueta_por_corte", flush=True)


def verificar_corte(conn, fecha_corte: str):
    """Verifica que el corte esté en la DB. Si no está, lo materializa vía dbt."""
    sql = f"""
        SELECT COUNT(*) FROM features.p2_features_etiqueta_por_corte
        WHERE fecha_corte = TIMESTAMP '{fecha_corte}'
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        n = cur.fetchone()[0]

    if n == 0:
        # La tabla tiene otro corte (o está vacía). Materializamos este.
        print(f"ℹ️  El corte {fecha_corte} no está materializado. Lo generamos ahora...", flush=True)
        conn.close()  # liberar conexión mientras dbt corre
        materializar_corte(fecha_corte)
        # Reabrir conexión y verificar
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(sql)
            n = cur.fetchone()[0]
        if n == 0:
            raise RuntimeError(
                f"dbt run no materializó el corte {fecha_corte}. "
                f"Revisa los logs de dbt arriba."
            )

    print(f"✅ Corte {fecha_corte} presente en DB ({n:,} filas)", flush=True)
    return conn


def leer_features(conn, fecha_corte: str) -> pd.DataFrame:
    sql = f"""
        SELECT sk_usuario, {', '.join(FEATURE_COLS)}
        FROM features.p2_features_etiqueta_por_corte
        WHERE fecha_corte = TIMESTAMP '{fecha_corte}'
          AND en_poblacion = TRUE
        ORDER BY sk_usuario
    """
    df = pd.read_sql(sql, conn)
    print(f"   Features leídas: {len(df):,} usuarios en población")
    return df


def leer_tarjetas(conn) -> pd.DataFrame:
    """Devuelve (sk_usuario, tarjetas_concatenadas) para toda la base Silver."""
    sql = """
        WITH unioned AS (
            SELECT NULLIF(REGEXP_REPLACE(tarjeta, '\\D', '', 'g'), '')::bigint::text AS sk_usuario,
                   tarjeta::text AS id_crudo
            FROM silver.silver_transmetro_validado
            WHERE regla_calidad = 'VALIDO' AND tarjeta IS NOT NULL

            UNION ALL
            SELECT NULLIF(REGEXP_REPLACE(num_tarjeta, '\\D', '', 'g'), '')::bigint::text,
                   num_tarjeta::text
            FROM silver.silver_transurbano_validado
            WHERE regla_calidad = 'VALIDO' AND num_tarjeta IS NOT NULL

            UNION ALL
            SELECT NULLIF(REGEXP_REPLACE(num_tarjeta, '\\D', '', 'g'), '')::bigint::text,
                   num_tarjeta::text
            FROM silver.silver_metroriel_validado
            WHERE regla_calidad = 'VALIDO' AND num_tarjeta IS NOT NULL

            UNION ALL
            SELECT user_hash::text,
                   user_hash::text
            FROM silver.silver_aerometro_validado
            WHERE regla_calidad = 'VALIDO' AND user_hash IS NOT NULL
        )
        SELECT sk_usuario, STRING_AGG(DISTINCT id_crudo, ';' ORDER BY id_crudo) AS tarjetas
        FROM unioned
        WHERE sk_usuario IS NOT NULL
        GROUP BY sk_usuario
    """
    return pd.read_sql(sql, conn)


def main():
    parser = argparse.ArgumentParser(description="CLI de predicción de churn (Proyecto 2).")
    parser.add_argument("--fecha-corte", required=True,
                        help="Fecha de corte en formato 'YYYY-MM-DD HH:MM:SS' o 'YYYY-MM-DD'")
    args = parser.parse_args()

    import re
    fecha = args.fecha_corte.strip()
    # Normalizar: aceptar 'YYYY-MM-DD' o 'YYYY-MM-DD HH:MM:SS'
    if len(fecha) == 10:
        fecha = f"{fecha} 00:00:00"
    if not re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", fecha):
        print(f"❌ Fecha de corte inválida: {fecha!r}")
        print("   Formato esperado: 'YYYY-MM-DD' o 'YYYY-MM-DD HH:MM:SS'")
        sys.exit(2)

    print("=" * 60)
    print(f" PREDICCIÓN — corte {fecha}")
    print("=" * 60)

    if not os.path.exists(MODELO_HGB):
        print(f"❌ No existe {MODELO_HGB}. Corre primero python -m scripts.p2_umbral_test_unico")
        sys.exit(1)

    print(f"▶️  Cargando modelo: {MODELO_HGB}")
    model = joblib.load(MODELO_HGB)
    print(f"   Modelo: {type(model).__name__}")

    conn = get_db_connection()
    try:
        conn = verificar_corte(conn, fecha)
        df = leer_features(conn, fecha)
        if df.empty:
            print("❌ No hay usuarios en población para este corte.")
            sys.exit(1)

        print(f"▶️  Prediciendo con umbral {UMBRAL:.4f}")
        X = df[FEATURE_COLS].astype(float).values
        proba = model.predict_proba(X)[:, 1]
        df["puntaje"] = proba
        df["contactar"] = (proba > UMBRAL).astype(int)

        print(f"▶️  Resolviendo tarjetas por usuario")
        df_tarj = leer_tarjetas(conn)
    finally:
        conn.close()

    df_out = df.merge(df_tarj, on="sk_usuario", how="left")
    df_out["unidad_id"] = df_out["sk_usuario"]
    df_out["tarjetas"] = df_out["tarjetas"].fillna("")
    df_out = df_out[["unidad_id", "tarjetas", "puntaje", "contactar"]]
    df_out = df_out.sort_values("puntaje", ascending=False).reset_index(drop=True)

    os.makedirs(RESULTS_DIR, exist_ok=True)
    fecha_tag = fecha.split(" ")[0].replace("-", "")
    ruta = os.path.join(RESULTS_DIR, f"p2_predicciones_{fecha_tag}.csv")
    df_out.to_csv(ruta, index=False, encoding="utf-8")

    n_contactar = int(df_out["contactar"].sum())
    print(f"\n✅ Predicciones escritas: {ruta}")
    print(f"   Total usuarios: {len(df_out):,}")
    print(f"   Contactar (p > {UMBRAL}): {n_contactar:,} ({100.0*n_contactar/len(df_out):.2f}%)")
    print(f"   Puntaje medio: {df_out['puntaje'].mean():.4f} ± {df_out['puntaje'].std():.4f}")
    print("=" * 60)


if __name__ == "__main__":
    main()