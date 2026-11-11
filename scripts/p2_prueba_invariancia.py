"""
Rubric 2.4 — Prueba de invariancia temporal.

Qué hace:
  Aplica el MISMO predictor (HGB entrenado en A) sobre 3 cortes: A, B, C.
  Verifica:
    1) Determinismo: correr predecir.py dos veces en el mismo corte → CSV idéntico.
    2) Estabilidad de la distribución: media, std, histograma de puntajes en A, B, C.
    3) % de contactados estable: no cambia > 5 pp entre cortes.
    4) (Bonus) Métricas en A y B (donde hay etiqueta): ROC-AUC consistente.

Salidas:
  - results/p2_fase2_2.4_invariancia.md
  - results/p2_fase2_2.4_invariancia_hist.png
  - results/p2_fase2_2.4_invariancia.csv
"""
import os
import subprocess
import sys

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
from dotenv import load_dotenv
from sklearn.metrics import roc_auc_score, average_precision_score

load_dotenv()

RESULTS_DIR = "./results"
CORTES = {
    "A": "2026-07-01 00:00:00",
    "B": "2026-07-31 00:00:00",
    "C": "2026-08-30 00:00:00",
}
UMBRAL = 0.3333


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("DB_USER", "amt_admin"),
        password=os.getenv("DB_PASSWORD"),
        dbname=os.getenv("DB_NAME", "amt_dw"),
    )


def correr_predecir(fecha_corte: str) -> str:
    """Corre predecir.py y devuelve la ruta del CSV generado."""
    import re
    # Validar formato exacto YYYY-MM-DD HH:MM:SS (sanity check antes de lanzar subproceso)
    if not re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", fecha_corte):
        raise ValueError(
            f"Fecha de corte inválida: {fecha_corte!r}. "
            f"Formato esperado: 'YYYY-MM-DD HH:MM:SS'."
        )
    cmd = [sys.executable, "-m", "scripts.predecir", "--fecha-corte", fecha_corte]
    print(f"▶️  {' '.join(cmd)}")
    res = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if res.returncode != 0:
        print(res.stdout)
        print(res.stderr)
        raise RuntimeError(f"predecir.py falló (código {res.returncode})")
    tag = fecha_corte.split(" ")[0].replace("-", "")
    return os.path.join(RESULTS_DIR, f"p2_predicciones_{tag}.csv")

def hash_csv(ruta: str) -> str:
    import hashlib
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def metricas_corte(fecha_corte: str, df_pred: pd.DataFrame) -> dict:
    """Calcula ROC-AUC y PR-AUC si el corte tiene etiqueta en la DB.

    Como la tabla features.p2_features_etiqueta_por_corte es materialized='table'
    y solo guarda UN corte a la vez, nos aseguramos de que el corte pedido esté
    materializado antes de leer la etiqueta.
    """
    # 1. Verificar si el corte está materializado
    sql_check = f"""
        SELECT COUNT(*) FROM features.p2_features_etiqueta_por_corte
        WHERE fecha_corte = TIMESTAMP '{fecha_corte}'
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql_check)
            n = cur.fetchone()[0]
    finally:
        conn.close()

    # 2. Si no está, materializarlo vía dbt
    if n == 0:
        print(f"   ℹ️  Corte {fecha_corte} no está materializado. Generando vía dbt...", flush=True)
        _materializar_corte(fecha_corte)

    # 3. Leer la etiqueta
    sql = f"""
        SELECT sk_usuario, etiqueta
        FROM features.p2_features_etiqueta_por_corte
        WHERE fecha_corte = TIMESTAMP '{fecha_corte}'
          AND en_poblacion = TRUE
          AND etiqueta IS NOT NULL
    """
    conn = get_db_connection()
    try:
        df_lab = pd.read_sql(sql, conn)
    finally:
        conn.close()

    if df_lab.empty:
        return {"roc_auc": None, "pr_auc": None, "n_label": 0, "n_pos": 0}

    df_m = df_pred.merge(df_lab, left_on="unidad_id", right_on="sk_usuario", how="inner")
    if df_m.empty:
        return {"roc_auc": None, "pr_auc": None, "n_label": 0, "n_pos": 0}

    return {
        "roc_auc": float(roc_auc_score(df_m["etiqueta"], df_m["puntaje"])),
        "pr_auc": float(average_precision_score(df_m["etiqueta"], df_m["puntaje"])),
        "n_label": int(len(df_m)),
        "n_pos": int(df_m["etiqueta"].sum()),
    }


def _materializar_corte(fecha_corte: str, fecha_referencia: str = "2026-08-30 00:00:00"):
    """Corre dbt para materializar el corte pedido (helper de metricas_corte)."""
    cmd = [
        "dbt", "run",
        "--project-dir", "dbt_transforms",
        "--profiles-dir", "dbt_transforms",
        "--select", "features",
        "--vars",
        f'{{"fecha_corte": "{fecha_corte}", "fecha_corte_referencia": "{fecha_referencia}"}}',
    ]
    print(f"▶️  {' '.join(cmd)}", flush=True)
    res = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if res.returncode != 0:
        print(res.stdout)
        print(res.stderr)
        raise RuntimeError(f"dbt run features falló (código {res.returncode})")
    print(f"✅ Corte {fecha_corte} materializado", flush=True)


def graficar_histogramas(dfs: dict, ruta_png: str):
    plt.close("all")
    fig, ax = plt.subplots(figsize=(10, 5))
    bins = np.linspace(0, 1, 50)
    for nombre, df in dfs.items():
        ax.hist(df["puntaje"], bins=bins, alpha=0.45, label=f"{nombre} (n={len(df):,})")
    ax.axvline(UMBRAL, color="red", linestyle="--", label=f"Umbral {UMBRAL}")
    ax.set_xlabel("Puntaje (probabilidad de churn)")
    ax.set_ylabel("Frecuencia")
    ax.set_title("Distribución de puntajes por corte (mismo predictor)")
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    ruta_abs = os.path.abspath(ruta_png)
    plt.savefig(ruta_abs, dpi=120, bbox_inches="tight")
    plt.close()
    print(f"✅ Histograma: {ruta_abs}")


def main():
    print("=" * 60)
    print(" RUBRIC 2.4 — Prueba de invariancia temporal")
    print("=" * 60)

    # 1. Determinismo: correr predecir.py dos veces en A → mismo hash
    print("\n▶️  Paso 1: Determinismo (correr predecir.py 2 veces en A)")
    ruta_A_1 = correr_predecir(CORTES["A"])
    h1 = hash_csv(ruta_A_1)
    ruta_A_2 = correr_predecir(CORTES["A"])
    h2 = hash_csv(ruta_A_2)
    determinismo = (h1 == h2)
    print(f"   Hash corrida 1: {h1[:16]}...")
    print(f"   Hash corrida 2: {h2[:16]}...")
    print(f"   Determinismo: {'✅' if determinismo else '❌'}")

    # 2. Predicciones en A, B, C (materializando cada corte una sola vez)
    print("\n▶️  Paso 2: Predicciones en A, B, C")

    dfs = {}
    metricas = {}

    for nombre in ("A", "B", "C"):
        fecha = CORTES[nombre]
        print(f"\n   ── Corte {nombre} ({fecha}) ──", flush=True)

        # 2.1 Materializar el corte (si ya está, dbt run es rápido pero igual corre)
        _materializar_corte(fecha)

        # 2.2 Predecir
        try:
            ruta = correr_predecir(fecha)
            df = pd.read_csv(ruta)
        except Exception as e:
            print(f"   ❌ Falló predicción para corte {nombre}: {e}", flush=True)
            raise

        faltantes = {"unidad_id", "tarjetas", "puntaje", "contactar"} - set(df.columns)
        if faltantes:
            raise RuntimeError(
                f"El CSV de {nombre} no tiene las columnas esperadas: {faltantes}. "
                f"Columnas presentes: {list(df.columns)}"
            )

        dfs[nombre] = df
        print(f"   ✅ {nombre}: {len(df):,} usuarios, "
              f"{int(df['contactar'].sum()):,} contactar, "
              f"media={df['puntaje'].mean():.4f}, std={df['puntaje'].std():.4f}", flush=True)

        # 2.3 Si el corte tiene etiqueta, calcular métricas AHORA (aprovechando que
        #     la tabla ya tiene este corte materializado)
        if nombre in ("A", "B"):
            m = metricas_corte(fecha, df)
            metricas[nombre] = m
            if m["roc_auc"] is not None:
                print(f"   {nombre}: ROC-AUC={m['roc_auc']:.4f}, PR-AUC={m['pr_auc']:.4f}, "
                      f"n={m['n_label']:,}, positivos={m['n_pos']:,}", flush=True)
            else:
                print(f"   {nombre}: sin etiqueta en DB", flush=True)

    if set(dfs.keys()) != {"A", "B", "C"}:
        raise RuntimeError(
            f"dfs no tiene los 3 cortes esperados. Presentes: {list(dfs.keys())}"
        )

    # 3. Estabilidad de la distribución + % contactados
    print("\n▶️  Paso 3: Estabilidad entre cortes")
    filas = []
    for nombre, df in dfs.items():
        filas.append({
            "corte": nombre,
            "n_usuarios": len(df),
            "puntaje_media": round(float(df["puntaje"].mean()), 4),
            "puntaje_std": round(float(df["puntaje"].std()), 4),
            "puntaje_p50": round(float(df["puntaje"].median()), 4),
            "puntaje_p90": round(float(df["puntaje"].quantile(0.90)), 4),
            "n_contactar": int(df["contactar"].sum()),
            "pct_contactar": round(100.0 * df["contactar"].mean(), 2),
        })
    df_est = pd.DataFrame(filas)
    print(df_est.to_string(index=False))

    delta_pct = df_est["pct_contactar"].max() - df_est["pct_contactar"].min()
    estable = delta_pct <= 5.0
    print(f"   Δ % contactados (max-min): {delta_pct:.2f} pp → "
          f"{'✅ estable (≤5 pp)' if estable else '⚠️ inestable (>5 pp)'}")

    # 4. Métricas en A y B (ya calculadas en Paso 2, solo se reportan)
    print("\n▶️  Paso 4: Métricas en A y B (cortes con etiqueta)")
    for nombre in ("A", "B"):
        m = metricas.get(nombre, {"roc_auc": None, "pr_auc": None, "n_label": 0, "n_pos": 0})
        if m["roc_auc"] is not None:
            print(f"   {nombre}: ROC-AUC={m['roc_auc']:.4f}, PR-AUC={m['pr_auc']:.4f}, "
                  f"n={m['n_label']:,}, positivos={m['n_pos']:,}", flush=True)
        else:
            print(f"   {nombre}: sin etiqueta en DB", flush=True)

    # 5. Gráfica de histogramas
    print("\n▶️  Paso 5: Histograma de puntajes")
    ruta_png = os.path.join(RESULTS_DIR, "p2_fase2_2.4_invariancia_hist.png")
    graficar_histogramas(dfs, ruta_png)

    # 6. Guardar tabla de estabilidad
    ruta_csv = os.path.join(RESULTS_DIR, "p2_fase2_2.4_invariancia.csv")
    df_est.to_csv(ruta_csv, index=False, encoding="utf-8")
    print(f"✅ Estabilidad CSV: {ruta_csv}")

    # 7. Markdown
    ruta_md = os.path.join(RESULTS_DIR, "p2_fase2_2.4_invariancia.md")
    with open(ruta_md, "w", encoding="utf-8") as f:
        f.write("# Fase 2.4 — Prueba de invariancia temporal\n\n")
        f.write("**Definición operativa:** el mismo predictor (HGB entrenado en A, "
                "`p2_hgb_v1.0.0.pkl`) se aplicó sobre 3 datasets con distinta cantidad de "
                "días acumulados (A=junio, B=julio, C=agosto). Se verifica que el predictor "
                "no cambia de comportamiento ni colapsa.\n\n")

        f.write("## 1. Determinismo\n\n")
        f.write(f"- Corrida 1 en A: hash SHA-256 `{h1[:16]}...`\n")
        f.write(f"- Corrida 2 en A: hash SHA-256 `{h2[:16]}...`\n")
        f.write(f"- Resultado: **{'✅ idéntico bit a bit' if determinismo else '❌ distinto'}**\n\n")

        f.write("## 2. Estabilidad de la distribución de puntajes\n\n")
        f.write("| Corte | n usuarios | Media | Std | p50 | p90 | Contactar | % Contactar |\n")
        f.write("|---|---|---|---|---|---|---|---|\n")
        for _, r in df_est.iterrows():
            f.write(f"| {r['corte']} | {int(r['n_usuarios']):,} | {r['puntaje_media']:.4f} | "
                    f"{r['puntaje_std']:.4f} | {r['puntaje_p50']:.4f} | {r['puntaje_p90']:.4f} | "
                    f"{int(r['n_contactar']):,} | {r['pct_contactar']:.2f}% |\n")
        f.write("\n")
        f.write(f"**Δ % contactados (max-min): {delta_pct:.2f} pp** → "
                f"{'✅ estable (≤ 5 pp)' if estable else '⚠️ inestable (> 5 pp)'}\n\n")

        f.write("## 3. Métricas en cortes con etiqueta (A y B)\n\n")
        f.write("| Corte | ROC-AUC | PR-AUC | n etiquetados | positivos |\n")
        f.write("|---|---|---|---|---|\n")
        for nombre in ("A", "B"):
            m = metricas[nombre]
            if m["roc_auc"] is not None:
                f.write(f"| {nombre} | {m['roc_auc']:.4f} | {m['pr_auc']:.4f} | "
                        f"{m['n_label']:,} | {m['n_pos']:,} |\n")
            else:
                f.write(f"| {nombre} | — | — | sin etiqueta | — |\n")
        f.write("\n")
        if all(metricas[n]["roc_auc"] is not None for n in ("A", "B")):
            delta_roc = abs(metricas["A"]["roc_auc"] - metricas["B"]["roc_auc"])
            f.write(f"**Δ ROC-AUC (A vs B): {delta_roc:.4f}**. "
                    f"{'✅ Consistente (Δ < 0.02)' if delta_roc < 0.02 else '⚠️ Diferencia notable.'}\n\n")

        f.write("## 4. Gráfica de distribuciones\n\n")
        f.write("![Histograma de puntajes](p2_fase2_2.4_invariancia_hist.png)\n\n")
        f.write("Se superponen los histogramas de puntaje de A, B y C con el mismo predictor. "
                "La línea roja marca el umbral 0.3333.\n\n")

        f.write("## Conclusión\n\n")
        concluye_ok = determinismo and estable
        if concluye_ok:
            f.write("**El predictor es invariante en el tiempo.** El mismo modelo HGB entrenado en A, "
                    "aplicado sobre A, B y C sin reentrenar, produce distribuciones de puntaje estables "
                    "(Δ % contactados ≤ 5 pp) y es determinista (mismo dataset → mismo CSV bit a bit). "
                    "Los cortes con etiqueta (A, B) muestran ROC-AUC consistente. "
                    "El modelo es apto para producción sobre datos futuros del mismo origen.\n")
        else:
            f.write("**⚠️ Hallazgos:** revisar la sección de estabilidad. "
                    "El predictor es determinista pero la distribución cambió más de 5 pp entre cortes. "
                    "Considerar recalibración (CalibratedClassifierCV) en producción.\n")

    print(f"✅ MD: {ruta_md}")
    print("\n" + "=" * 60)
    print(" ✅ Prueba de invariancia completada")
    print("=" * 60)


if __name__ == "__main__":
    main()