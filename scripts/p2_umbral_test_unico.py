"""
Rubric 2.2 — Derivación de umbral + test único en B.

Qué hace:
  1. dbt run del corte B (genera features/etiquetas/población en DB).
  2. Exporta A y B a Parquet (si no existen).
  3. Entrena HGB con TODO A.
  4. Barrido de umbrales en A → p*_empirico (máx beneficio neto esperado).
  5. Reporta p*_teorico (1/3), p*_empirico, p*_elegido (default 1/3).
  6. Evalúa UNA VEZ en B con p*_elegido.
  7. Guarda modelo versionado en modelos_entrenados/p2_hgb_v1.0.0.pkl
     (borra cualquier p2_*.pkl previo).
  8. Escribe bitácora automática en results/p2_fase2_2.2_bitacora.csv.

PROHIBIDO: no re-evaluar B. Si la bitácora ya tiene una fila para el corte B,
aborta con aviso.
"""
import glob
import os
import subprocess
import sys
from datetime import datetime

import joblib
import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import (
    roc_auc_score, average_precision_score,
    precision_score, recall_score, f1_score,
    confusion_matrix,
)

load_dotenv()

RESULTS_DIR = "./results"
MODELOS_DIR = "./modelos_entrenados"
PARQUET_A = os.path.join(RESULTS_DIR, "p2_features_A.parquet")
PARQUET_B = os.path.join(RESULTS_DIR, "p2_features_B.parquet")
MODELO_PATH = os.path.join(MODELOS_DIR, "p2_hgb_v1.0.0.pkl")
BITACORA_CSV = os.path.join(RESULTS_DIR, "p2_fase2_2.2_bitacora.csv")
RESUMEN_MD = os.path.join(RESULTS_DIR, "p2_fase2_2.2_resumen.md")

CORTE_A = "2026-07-01 00:00:00"
CORTE_B = "2026-07-31 00:00:00"
FECHA_REF = "2026-08-30 00:00:00"

# Escenario de costos (fijo en el enunciado)
COSTO_CONTACTO = 5.00      # Q
VALOR_RETENIDO = 60.00     # Q
TASA_RETENCION = 0.25

P_TEORICO = COSTO_CONTACTO / (VALOR_RETENIDO * TASA_RETENCION)  # = 1/3 ≈ 0.3333
P_DEFAULT_ELEGIDO = P_TEORICO  # Si no se decide otro, se usa el teórico

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

HGB_PARAMS = dict(
    max_iter=200,
    learning_rate=0.05,
    max_depth=5,
    min_samples_leaf=20,
    l2_regularization=1.0,
    random_state=42,
)


# ---------------------------------------------------------------------------
# Dependencias
# ---------------------------------------------------------------------------

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("DB_USER", "amt_admin"),
        password=os.getenv("DB_PASSWORD"),
        dbname=os.getenv("DB_NAME", "amt_dw"),
    )


def dbt_run_corte_B():
    cmd = [
        "dbt", "run",
        "--project-dir", "dbt_transforms",
        "--profiles-dir", "dbt_transforms",
        "--select", "features",
        "--vars",
        f'{{fecha_corte: "{CORTE_B}", fecha_corte_referencia: "{FECHA_REF}"}}',
    ]
    print(f"▶️  {' '.join(cmd)}")
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print(res.stdout)
        print(res.stderr)
        raise RuntimeError(f"dbt run falló (código {res.returncode})")
    print("✅ dbt run features corte B OK")


def exportar_corte(fecha_corte: str, ruta: str):
    sql = f"""
        SELECT *
        FROM features.p2_features_etiqueta_por_corte
        WHERE fecha_corte = TIMESTAMP '{fecha_corte}'
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
        raise RuntimeError(f"DataFrame vacío para corte {fecha_corte}.")
    print(f"   Corte {fecha_corte}: {len(df):,} filas, {int(df['etiqueta'].sum()):,} positivos")
    df.to_parquet(ruta, index=False)
    return df


def asegurar_parquets():
    """
    Exporta A y B. Regla:
      - A: exportar SIEMPRE (es el train, no hay riesgo de leakage).
      - B: exportar SOLO si no existe el Parquet (para no re-evaluar silenciosamente).
    """
    print("▶️  Preparando features de A...")
    dbt_run_corte_A()
    df_a = exportar_corte(CORTE_A, PARQUET_A)

    if os.path.exists(PARQUET_B):
        print(f"⚠️  Ya existe {PARQUET_B}. Se usa el existente (no se regenera).")
        print("    Si querés re-evaluar B, borralo manualmente Y borrá la bitácora.")
        df_b = pd.read_parquet(PARQUET_B)
        print(f"   Corte B cacheado: {len(df_b):,} filas, {int(df_b['etiqueta'].sum()):,} positivos")
    else:
        print("▶️  Preparando features de B (primera y ÚNICA vez)...")
        dbt_run_corte_B()
        df_b = exportar_corte(CORTE_B, PARQUET_B)

    return df_a, df_b


def dbt_run_corte_A():
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
        raise RuntimeError(f"dbt run A falló (código {res.returncode})")
    print("✅ dbt run features corte A OK")


# ---------------------------------------------------------------------------
# Modelo + umbral
# ---------------------------------------------------------------------------

def preparar_xy(df: pd.DataFrame):
    faltantes = [c for c in FEATURE_COLS + ["etiqueta"] for _ in [0] if c not in df.columns]
    if faltantes:
        raise RuntimeError(f"Faltan columnas en el DataFrame: {faltantes}")
    df = df.dropna(subset=FEATURE_COLS + ["etiqueta"]).reset_index(drop=True)
    return df[FEATURE_COLS].astype(float).values, df["etiqueta"].astype(int).values


def entrenar_en_A(df_a: pd.DataFrame):
    """Entrena el modelo ELEGIDO en 2.1 (HGB o L2) con TODO el corte A.

    Lee results/p2_fase2_2.1_decision.json para respetar la decisión de 2.1.
    Si no existe, aborta con mensaje claro (falta correr 2.1).
    """
    ruta_decision = os.path.join(RESULTS_DIR, "p2_fase2_2.1_decision.json")
    if not os.path.exists(ruta_decision):
        raise RuntimeError(
            f"Falta {ruta_decision}. Corre primero p2_ensemble_hgb.py (2.1)."
        )

    import json
    from sklearn.linear_model import LogisticRegression
    with open(ruta_decision, "r", encoding="utf-8") as f:
        dec = json.load(f)

    eleccion = dec["eleccion"]
    X_a, y_a = preparar_xy(df_a)

    if eleccion == "HGB":
        model = HistGradientBoostingClassifier(**dec["hgb_params"])
        nombre = "HGB"
    else:
        # "L2" o "Empate técnico" → L2 por interpretabilidad (criterio de 2.1)
        model = LogisticRegression(**dec["l2_params"])
        nombre = "L2"

    model.fit(X_a, y_a)
    print(f"✅ Modelo '{nombre}' (elección 2.1 = '{eleccion}') "
          f"entrenado con TODO A ({len(X_a):,} filas)")
    return model


def beneficio_neto(y_true, y_proba, umbral):
    """Beneficio neto esperado al umbral dado (contactar si proba > umbral)."""
    pred = (y_proba > umbral).astype(int)
    tp = int(((pred == 1) & (y_true == 1)).sum())
    contactados = int(pred.sum())
    beneficio = tp * VALOR_RETENIDO * TASA_RETENCION - contactados * COSTO_CONTACTO
    return beneficio, contactados, tp


def barrido_umbral_en_A(model, df_a: pd.DataFrame) -> pd.DataFrame:
    X_a, y_a = preparar_xy(df_a)
    proba_a = model.predict_proba(X_a)[:, 1]
    filas = []
    for u in np.arange(0.01, 1.00, 0.01):
        b, n_cont, tp = beneficio_neto(y_a, proba_a, u)
        filas.append({
            "umbral": round(float(u), 2),
            "beneficio_neto": b,
            "contactados": n_cont,
            "tp": tp,
        })
    return pd.DataFrame(filas)


def elegir_umbral_empirico(df_barrido: pd.DataFrame) -> float:
    idx = df_barrido["beneficio_neto"].idxmax()
    return float(df_barrido.loc[idx, "umbral"])


# ---------------------------------------------------------------------------
# Evaluación única en B
# ---------------------------------------------------------------------------

def evaluar_en_B(model, df_b: pd.DataFrame, umbral: float) -> dict:
    X_b, y_b = preparar_xy(df_b)
    proba_b = model.predict_proba(X_b)[:, 1]

    roc = roc_auc_score(y_b, proba_b)
    pr = average_precision_score(y_b, proba_b)

    pred = (proba_b > umbral).astype(int)
    prec = precision_score(y_b, pred, zero_division=0)
    rec = recall_score(y_b, pred, zero_division=0)
    f1 = f1_score(y_b, pred, zero_division=0)
    tn, fp, fn, tp = confusion_matrix(y_b, pred, labels=[0, 1]).ravel()

    b, n_cont, _ = beneficio_neto(y_b, proba_b, umbral)
    retenidos = int(tp * TASA_RETENCION)
    beneficio_real = retenidos * VALOR_RETENIDO - n_cont * COSTO_CONTACTO

    return {
        "roc_auc": float(roc),
        "pr_auc": float(pr),
        "precision": float(prec),
        "recall": float(rec),
        "f1": float(f1),
        "tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp),
        "contactados": int(n_cont),
        "tp_contactados": int(tp),
        "retenidos_esperados": int(retenidos),
        "beneficio_neto": float(beneficio_real),
    }


# ---------------------------------------------------------------------------
# Persistencia
# ---------------------------------------------------------------------------

def guardar_modelo(model):
    os.makedirs(MODELOS_DIR, exist_ok=True)
    previos = glob.glob(os.path.join(MODELOS_DIR, "p2_*.pkl"))
    for p in previos:
        os.remove(p)
        print(f"   Borrado modelo previo: {p}")
    joblib.dump(model, MODELO_PATH)
    print(f"✅ Modelo guardado: {MODELO_PATH}")


def hash_commit() -> str:
    try:
        res = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
        )
        return res.stdout.strip()[:12]
    except Exception:
        return "no_disponible"


def leer_bitacora() -> pd.DataFrame:
    if not os.path.exists(BITACORA_CSV):
        return pd.DataFrame()
    return pd.read_csv(BITACORA_CSV)


def escribir_bitacora_y_resumen(df_barrido, umbral_empirico, umbral_elegido, metricas_B, model=None):
    ahora = datetime.now().isoformat(timespec="seconds")
    h = hash_commit()

    fila = {
        "fecha": ahora,
        "commit_hash": h,
        "modelo": type(model).__name__ if model is not None else "HGB",
        "corte_train": "A",
        "corte_test": "B",
        "p_teorico": round(P_TEORICO, 4),
        "p_empirico_A": round(umbral_empirico, 4),
        "p_elegido": round(umbral_elegido, 4),
        "roc_auc_B": round(metricas_B["roc_auc"], 4),
        "pr_auc_B": round(metricas_B["pr_auc"], 4),
        "precision_B": round(metricas_B["precision"], 4),
        "recall_B": round(metricas_B["recall"], 4),
        "f1_B": round(metricas_B["f1"], 4),
        "contactados_B": metricas_B["contactados"],
        "tp_contactados_B": metricas_B["tp_contactados"],
        "retenidos_esperados_B": metricas_B["retenidos_esperados"],
        "beneficio_neto_B": round(metricas_B["beneficio_neto"], 2),
    }
    df_hist = leer_bitacora()
    df_new = pd.concat([df_hist, pd.DataFrame([fila])], ignore_index=True)
    df_new.to_csv(BITACORA_CSV, index=False, encoding="utf-8")
    print(f"✅ Bitácora: {BITACORA_CSV}")

    # Resumen MD
    with open(RESUMEN_MD, "w", encoding="utf-8") as f:
        f.write("# Fase 2.2 — Umbral y test único en B\n\n")
        f.write(f"_Fecha: {ahora} | Commit: `{h}` | Modelo: HGB_\n\n")

        f.write("## Derivación del umbral\n\n")
        f.write("**Escenario de costos (enunciado):**\n")
        f.write(f"- Costo de contactar: **Q{COSTO_CONTACTO:.2f}**\n")
        f.write(f"- Tasa de retención: **{TASA_RETENCION:.0%}**\n")
        f.write(f"- Valor de un retenido: **Q{VALOR_RETENIDO:.2f}**\n\n")
        f.write("**Umbral teórico (cuenta completa):**\n\n")
        f.write(f"```\n")
        f.write(f"p* = costo_contacto / (valor_retenido × tasa_retencion)\n")
        f.write(f"p* = {COSTO_CONTACTO:.2f} / ({VALOR_RETENIDO:.2f} × {TASA_RETENCION:.2f})\n")
        f.write(f"p* = {COSTO_CONTACTO:.2f} / {VALOR_RETENIDO*TASA_RETENCION:.2f}\n")
        f.write(f"p* = {P_TEORICO:.4f}\n")
        f.write(f"```\n\n")
        f.write(f"**Umbral empírico (barrido en A, 0.01–0.99):** `{umbral_empirico:.4f}`\n\n")
        f.write(f"**Umbral elegido:** `{umbral_elegido:.4f}` "
                f"({'> teórico' if umbral_elegido > P_TEORICO else '< teórico' if umbral_elegido < P_TEORICO else '= teórico'})\n\n")

        f.write("## Evaluación única en B\n\n")
        f.write("| Métrica | Valor |\n")
        f.write("|---|---|\n")
        f.write(f"| ROC-AUC | **{metricas_B['roc_auc']:.4f}** |\n")
        f.write(f"| PR-AUC | {metricas_B['pr_auc']:.4f} |\n")
        f.write(f"| Precisión | {metricas_B['precision']:.4f} |\n")
        f.write(f"| Recall | {metricas_B['recall']:.4f} |\n")
        f.write(f"| F1 | {metricas_B['f1']:.4f} |\n")
        f.write(f"| Contactados | {metricas_B['contactados']:,} |\n")
        f.write(f"| TP contactados | {metricas_B['tp_contactados']:,} |\n")
        f.write(f"| Retenidos esperados | {metricas_B['retenidos_esperados']:,} |\n")
        f.write(f"| Beneficio neto | **Q{metricas_B['beneficio_neto']:,.2f}** |\n\n")

        f.write("**Matriz de confusión en B:**\n\n")
        f.write("| | Pred=0 | Pred=1 |\n")
        f.write("|---|---|---|\n")
        f.write(f"| Real=0 | {metricas_B['tn']:,} | {metricas_B['fp']:,} |\n")
        f.write(f"| Real=1 | {metricas_B['fn']:,} | {metricas_B['tp']:,} |\n\n")

        f.write("## Top 5 umbrales (por beneficio neto en A)\n\n")
        top5 = df_barrido.sort_values("beneficio_neto", ascending=False).head(5)
        f.write("| Umbral | Beneficio (A) | Contactados (A) | TP (A) |\n")
        f.write("|---|---|---|---|\n")
        for _, r in top5.iterrows():
            f.write(f"| {r['umbral']:.2f} | Q{r['beneficio_neto']:,.2f} | "
                    f"{int(r['contactados']):,} | {int(r['tp']):,} |\n\n")

        f.write("## Nota: B se evaluó UNA SOLA VEZ\n\n")
        f.write("Por el rubric 2.2, el test en B es único. No se reentrenó ni re-evaluó. "
                "Si se necesita re-evaluar, borrar manualmente `p2_features_B.parquet` "
                "y `p2_fase2_2.2_bitacora.csv`.\n")

    print(f"✅ Resumen MD: {RESUMEN_MD}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print(" RUBRIC 2.2 — Umbral y test único en B")
    print("=" * 60)
    print(f"   p*_teorico = {P_TEORICO:.4f}  (= 5 / (60 × 0.25))")
    print(f"   p*_default_elegido = {P_DEFAULT_ELEGIDO:.4f}")

    # Guard: si ya hay bitácora con corte B, hacer SKIP limpio (exit 0)
    hist = leer_bitacora()
    if not hist.empty and (hist["corte_test"] == "B").any():
        print("\n⚠️  Ya existe una evaluación de B en la bitácora.")
        print("   Por el rubric 2.2, B se evalúa UNA SOLA VEZ. Se omite esta etapa.")
        print("   Para re-evaluar: borrar p2_features_B.parquet Y p2_fase2_2.2_bitacora.csv.")
        print(f"   Bitácora actual: {BITACORA_CSV}")
        # SKIP limpio: exit 0 para que Prefect continúe con 2.3 y 2.4
        sys.exit(0)

    df_a, df_b = asegurar_parquets()

    print("\n▶️  Entrenando modelo final (decisión 2.1) con TODO A")
    model = entrenar_en_A(df_a)

    print("\n▶️  Barrido de umbrales en A (0.01–0.99)")
    df_barrido = barrido_umbral_en_A(model, df_a)
    umbral_empirico = elegir_umbral_empirico(df_barrido)
    top3 = df_barrido.sort_values("beneficio_neto", ascending=False).head(3)
    print("   Top 3 umbrales (beneficio neto en A):")
    print(top3.to_string(index=False))

    umbral_elegido = P_DEFAULT_ELEGIDO
    print(f"\n   p*_teorico  = {P_TEORICO:.4f}")
    print(f"   p*_empirico = {umbral_empirico:.4f}")
    print(f"   p*_elegido  = {umbral_elegido:.4f}  (default = teórico)")

    print(f"\n▶️  Evaluación ÚNICA en B con p*_elegido = {umbral_elegido:.4f}")
    metricas_B = evaluar_en_B(model, df_b, umbral_elegido)
    print("   " + " | ".join([f"{k}={v}" for k, v in metricas_B.items() if k in
                              ("roc_auc", "precision", "recall", "contactados", "beneficio_neto")]))

    print("\n▶️  Guardando modelo")
    guardar_modelo(model)

    print("\n▶️  Bitácora + resumen")
    escribir_bitacora_y_resumen(df_barrido, umbral_empirico, umbral_elegido, metricas_B, model)

    # Guardar CSV del barrido (evidencia de la elección del umbral)
    ruta_barrido = os.path.join(RESULTS_DIR, "p2_fase2_2.2_barrido_umbral_A.csv")
    df_barrido.to_csv(ruta_barrido, index=False, encoding="utf-8")
    print(f"✅ Barrido de umbrales: {ruta_barrido}")

    print("\n" + "=" * 60)
    print(" ✅ Turno 7 completado")
    print("=" * 60)


if __name__ == "__main__":
    main()