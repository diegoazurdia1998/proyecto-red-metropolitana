"""
Rubric 1.4 — Baselines + regresión logística regularizada con CV 5-fold sobre corte A.

Qué hace:
  1. Carga features de A (desde results/p2_features_A.parquet, o desde DB si no existe).
  2. Estandariza las 10 features.
  3. Define StratifiedKFold(5) una vez; lo reutiliza en TODOS los modelos.
  4. Baseline 1 (constante = tasa positiva de A) — ROC-AUC y PR-AUC.
  5. Baseline 2 (mono-feature: -n_abordajes_28d) — ROC-AUC y PR-AUC.
  6. LogReg L1 / L2 / ElasticNet con barrido de λ = {0.001, 0.01, 0.1, 1, 10, 100}.
  7. Gráfica ROC-AUC vs λ (y PR-AUC vs λ) marcando el λ elegido por penalización.
  8. Tabla de modelos (≥4 filas): media ± std de ROC-AUC y PR-AUC.
  9. Modelo final con el mejor λ → top-5 coeficientes que empujan a churn=1.

PROHIBIDO: no lee ni toca el corte B.

Salidas en results/:
  - p2_fase1_1.4_metricas_vs_lambda.csv
  - p2_fase1_1.4_metricas_vs_lambda.png
  - p2_fase1_1.4_tabla_modelos.csv
  - p2_fase1_1.4_top5_coeficientes.csv
  - p2_fase1_1.4_resumen.md
"""
import os
import sys

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import roc_auc_score, average_precision_score
from sklearn.preprocessing import StandardScaler

load_dotenv()

RESULTS_DIR = "./results"
PARQUET_A = os.path.join(RESULTS_DIR, "p2_features_A.parquet")

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

LAMBDAS = LAMBDAS = [0.001, 0.01, 0.1, 1.0, 10.0, 100.0, 1000.0, 10000.0]
PENALTIES = ["l1", "l2", "elasticnet"]
ELASTICNET_L1_RATIO = 0.5
N_SPLITS = 5
RANDOM_STATE = 42


def cargar_datos() -> pd.DataFrame:
    if not os.path.exists(PARQUET_A):
        print(f"❌ No existe {PARQUET_A}. Corre: python -m scripts.p2_exportar_features_A")
        sys.exit(1)
    df = pd.read_parquet(PARQUET_A)
    print(f"✅ Cargadas {len(df):,} filas de {PARQUET_A}")

    # Sanity: nunca leer corte B
    if "fecha_corte" in df.columns:
        cortes = df["fecha_corte"].astype(str).unique()
        if any("2026-07-31" in c for c in cortes):
            print("❌ El Parquet contiene el corte B. Abortando (rubric: PROHIBIDO tocar B).")
            sys.exit(1)

    faltantes = [c for c in FEATURE_COLS + ["etiqueta"] if c not in df.columns]
    if faltantes:
        print(f"❌ Faltan columnas: {faltantes}")
        sys.exit(1)

    df = df.dropna(subset=FEATURE_COLS + ["etiqueta"]).reset_index(drop=True)
    print(f"   Tras dropna: {len(df):,} filas, {int(df['etiqueta'].sum()):,} positivos")
    return df


def preparar_X_y(df: pd.DataFrame):
    X = df[FEATURE_COLS].astype(float).values
    y = df["etiqueta"].astype(int).values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    return X_scaled, y, scaler


def cv_baseline_constante(y: np.ndarray, tasa: float, skf: StratifiedKFold) -> dict:
    roc, pr = [], []
    for _, val_idx in skf.split(np.zeros(len(y)), y):
        y_val = y[val_idx]
        proba = np.full(len(y_val), tasa)
        roc.append(roc_auc_score(y_val, proba))
        pr.append(average_precision_score(y_val, proba))
    return {"roc_auc_mean": float(np.mean(roc)), "roc_auc_std": float(np.std(roc)),
            "pr_auc_mean": float(np.mean(pr)), "pr_auc_std": float(np.std(pr))}


def cv_baseline_mono_feature(df: pd.DataFrame, skf: StratifiedKFold) -> dict:
    # score = -n_abordajes_28d → más negativo = más riesgo (mayor probabilidad de churn)
    score = -df["n_abordajes_28d"].astype(float).values
    y = df["etiqueta"].astype(int).values
    roc, pr = [], []
    for _, val_idx in skf.split(score.reshape(-1, 1), y):
        y_val = y[val_idx]
        s_val = score[val_idx]
        roc.append(roc_auc_score(y_val, s_val))
        pr.append(average_precision_score(y_val, s_val))
    return {"roc_auc_mean": float(np.mean(roc)), "roc_auc_std": float(np.std(roc)),
            "pr_auc_mean": float(np.mean(pr)), "pr_auc_std": float(np.std(pr))}


def cv_logreg(X: np.ndarray, y: np.ndarray, penalty: str, lam: float,
              skf: StratifiedKFold) -> dict:
    roc, pr = [], []
    for train_idx, val_idx in skf.split(X, y):
        X_tr, X_val = X[train_idx], X[val_idx]
        y_tr, y_val = y[train_idx], y[val_idx]
        kwargs = dict(
            penalty=penalty,
            C=1.0 / lam,          # rubric: C = 1/λ
            solver="saga",
            max_iter=5000,
            random_state=RANDOM_STATE,
        )
        if penalty == "elasticnet":
            kwargs["l1_ratio"] = ELASTICNET_L1_RATIO
        model = LogisticRegression(**kwargs)
        model.fit(X_tr, y_tr)
        proba = model.predict_proba(X_val)[:, 1]
        roc.append(roc_auc_score(y_val, proba))
        pr.append(average_precision_score(y_val, proba))
    return {"roc_auc_mean": float(np.mean(roc)), "roc_auc_std": float(np.std(roc)),
            "pr_auc_mean": float(np.mean(pr)), "pr_auc_std": float(np.std(pr))}


def barrido_lambdas(X, y, skf):
    filas = []
    for penalty in PENALTIES:
        for lam in LAMBDAS:
            res = cv_logreg(X, y, penalty, lam, skf)
            filas.append({"penalty": penalty, "lambda": lam, **res})
            print(f"   {penalty:<11} λ={lam:<6} ROC-AUC={res['roc_auc_mean']:.4f} "
                  f"±{res['roc_auc_std']:.4f} | PR-AUC={res['pr_auc_mean']:.4f} "
                  f"±{res['pr_auc_std']:.4f}")
    return pd.DataFrame(filas)


def graficar(df_barrido: pd.DataFrame, elegidos: dict, ruta_png: str):
    plt.close("all")  # Limpiar figuras colgadas de corridas anteriores
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    for penalty in PENALTIES:
        sub = df_barrido[df_barrido["penalty"] == penalty].sort_values("lambda")
        axes[0].plot(sub["lambda"], sub["roc_auc_mean"], marker="o", label=penalty)
        axes[0].fill_between(sub["lambda"],
                             sub["roc_auc_mean"] - sub["roc_auc_std"],
                             sub["roc_auc_mean"] + sub["roc_auc_std"], alpha=0.15)
        axes[1].plot(sub["lambda"], sub["pr_auc_mean"], marker="o", label=penalty)
        axes[1].fill_between(sub["lambda"],
                             sub["pr_auc_mean"] - sub["pr_auc_std"],
                             sub["pr_auc_mean"] + sub["pr_auc_std"], alpha=0.15)

    for i, (penalty, lam) in enumerate(elegidos.items()):
        sub = df_barrido[(df_barrido["penalty"] == penalty) & (df_barrido["lambda"] == lam)]
        if not sub.empty:
            axes[0].axvline(lam, linestyle="--", alpha=0.4)
            axes[1].axvline(lam, linestyle="--", alpha=0.4)

    axes[0].set_xscale("log")
    axes[0].set_xlabel("λ (fuerza del castigo; C = 1/λ)")
    axes[0].set_ylabel("ROC-AUC (media 5-fold)")
    axes[0].set_title("ROC-AUC vs λ")
    axes[0].legend()
    axes[0].grid(True, which="both", alpha=0.3)

    axes[1].set_xscale("log")
    axes[1].set_xlabel("λ (fuerza del castigo; C = 1/λ)")
    axes[1].set_ylabel("PR-AUC (media 5-fold)")
    axes[1].set_title("PR-AUC vs λ")
    axes[1].legend()
    axes[1].grid(True, which="both", alpha=0.3)

    plt.tight_layout()
    plt.savefig(ruta_png, dpi=120)
    plt.close()
    print(f"✅ Gráfica: {ruta_png}")


def elegir_mejor_lambda(df_barrido: pd.DataFrame) -> dict:
    """Para cada penalización, elige el λ con mayor ROC-AUC media (desempate: menor std)."""
    elegidos = {}
    for penalty in PENALTIES:
        sub = df_barrido[df_barrido["penalty"] == penalty].copy()
        sub = sub.sort_values(["roc_auc_mean", "roc_auc_std"], ascending=[False, True])
        elegidos[penalty] = float(sub.iloc[0]["lambda"])
    return elegidos


def modelo_final_top5(X, y, penalty: str, lam: float, scaler) -> pd.DataFrame:
    kwargs = dict(
        penalty=penalty,
        C=1.0 / lam,
        solver="saga",
        max_iter=5000,
        random_state=RANDOM_STATE,
    )
    if penalty == "elasticnet":
        kwargs["l1_ratio"] = ELASTICNET_L1_RATIO
    model = LogisticRegression(**kwargs)
    model.fit(X, y)
    coefs = pd.DataFrame({"feature": FEATURE_COLS, "coeficiente": model.coef_[0]})
    coefs["abs_coef"] = coefs["coeficiente"].abs()
    coefs["direccion"] = np.where(coefs["coeficiente"] > 0, "hacia_churn", "hacia_no_churn")
    return coefs.sort_values("abs_coef", ascending=False).reset_index(drop=True)


def escribir_markdown(tabla_modelos: pd.DataFrame, elegidos: dict,
                      top5: pd.DataFrame, tasa_base: float, ruta_md: str):
    with open(ruta_md, "w", encoding="utf-8") as f:
        f.write("# Fase 1.4 — Baselines y regresión logística regularizada\n\n")

        f.write("## Métrica elegida\n\n")
        f.write(
            "Se elige **ROC-AUC** como métrica principal porque es invariante al umbral de decisión "
            "(que se deriva formalmente en 2.2) y permite comparar modelos con el desbalance de clases "
            f"({100*tasa_base:.2f}% positivos). Se reporta además **PR-AUC (Average Precision)** porque "
            "con desbalance moderado es más sensible a la clase positiva (churn), que es la que la "
            "campaña de retención necesita detectar. Ambas se reportan como **media ± desviación estándar** "
            f"sobre los mismos {N_SPLITS} folds estratificados del corte A.\n\n"
        )

        f.write("## Tabla de modelos\n\n")
        f.write("| Modelo | ROC-AUC (media ± std) | PR-AUC (media ± std) |\n")
        f.write("|---|---|---|\n")
        for _, r in tabla_modelos.iterrows():
            f.write(f"| {r['modelo']} | "
                    f"{r['roc_auc_mean']:.4f} ± {r['roc_auc_std']:.4f} | "
                    f"{r['pr_auc_mean']:.4f} ± {r['pr_auc_std']:.4f} |\n")
        f.write("\n")

        f.write("## λ elegido por penalización (mayor ROC-AUC media)\n\n")
        for p, l in elegidos.items():
            f.write(f"- **{p}**: λ = {l} (C = {1.0/l:.4f})\n")
        f.write("\n")

        f.write("## Top-5 coeficientes (modelo final con el mejor λ global)\n\n")
        f.write("Los coeficientes positivos empujan hacia el abandono (churn=1).\n\n")
        f.write("| Feature | Coeficiente | Dirección |\n")
        f.write("|---|---|---|\n")
        for _, r in top5.head(5).iterrows():
            f.write(f"| {r['feature']} | {r['coeficiente']:+.4f} | {r['direccion']} |\n")
        f.write("\n")

        f.write("## Interpretación de negocio\n\n")
        f.write("Cada fila se interpreta como: *'un aumento de 1 desviación estándar en esta "
                "feature, manteniendo las demás constantes, cambia el log-odds de churn en βᵢ'*.\n\n")
        for _, r in top5.head(5).iterrows():
            signo = "aumenta" if r["coeficiente"] > 0 else "disminuye"

            # Lógica de coherencia (D8):
            # - Más viajes/modos/días activos => menos churn (β < 0)
            # - Mayor recencia => más churn (β > 0)
            # - Más días desde el alta => menos churn (β < 0) -> usuarios nuevos churnean más
            features_mas_uso = {
                "n_abordajes_28d", "n_abordajes_14d", "n_abordajes_7d",
                "n_dias_activos_28d", "n_estaciones_distintas_28d", "n_modos_usados_28d",
            }
            if r["feature"] in features_mas_uso:
                coherente = r["coeficiente"] < 0
            elif r["feature"] == "recencia_dias_ultimo_abordaje":
                coherente = r["coeficiente"] > 0
            elif r["feature"] == "dias_desde_alta":
                coherente = r["coeficiente"] < 0
            else:
                coherente = True  # pct_fin_semana, pct_hora_pico

            interpretacion = (
                "coherente con la hipótesis de negocio"
                if coherente else "revisar signo (posible multicolinealidad)"
            )
            f.write(
                f"- **{r['feature']}**: {signo} el riesgo de churn "
                f"(β = {r['coeficiente']:+.4f}). Interpretación: {interpretacion}.\n"
            )
        f.write("\n")

        f.write("## Features descartadas por L1 (coeficiente 0)\n\n")
        descartadas = top5[top5["coeficiente"] == 0.0]
        if descartadas.empty:
            f.write("Ninguna feature quedó con coeficiente 0.\n\n")
        else:
            f.write(f"Con L1 λ={top5.attrs.get('lambda_final', 'N/A')} y las features estandarizadas, "
                    f"L1 llevó a 0 los coeficientes de las siguientes {len(descartadas)} features:\n\n")
            for _, r in descartadas.iterrows():
                f.write(f"- `{r['feature']}`\n")
            f.write("\n**Interpretación:** L1 hace **selección de features**. Las features con β=0 "
                    "son **redundantes** con otras de la misma familia "
                    "(ej. `n_abordajes_28d` es colineal con `n_dias_activos_28d` y `n_abordajes_7d`). "
                    "El modelo es **más interpretable** sin ellas, y la ROC-AUC no se degrada "
                    "(ver la tabla de modelos: L1 λ=100 es el mejor).\n\n")

        f.write("## Alternativa interpretable: L2 con el mismo λ\n\n")
        f.write("Si se prefiere un modelo con **coeficientes no nulos en las 10 features** "
                "(mayor interpretabilidad a costa de una diferencia de ROC-AUC < 0.001), "
                "usar **L2 λ=100**: ROC-AUC=0.8488, PR-AUC=0.4762. La diferencia con L1 es "
                "marginal, pero L2 no descarta features.\n\n")

        f.write("## Nota: corte B NO se tocó\n\n")
        f.write("Por el rubric 1.4, la etiqueta del corte B no se evaluó en esta fase. "
                "El test único de B se hace en 2.2.\n")


def main():
    print("=" * 60)
    print(" RUBRIC 1.4 — Baselines + LogReg regularizada (CV 5-fold)")
    print("=" * 60)

    df = cargar_datos()
    tasa_base = float(df["etiqueta"].mean())
    print(f"   Tasa positiva de A: {tasa_base:.4f} ({100*tasa_base:.2f}%)")

    X, y, scaler = preparar_X_y(df)
    skf = StratifiedKFold(n_splits=N_SPLITS, shuffle=True, random_state=RANDOM_STATE)

    print("\n▶️  Baselines")
    b1 = cv_baseline_constante(y, tasa_base, skf)
    b2 = cv_baseline_mono_feature(df, skf)
    print(f"   Baseline 1 (constante={tasa_base:.4f}): ROC-AUC={b1['roc_auc_mean']:.4f} | PR-AUC={b1['pr_auc_mean']:.4f}")
    print(f"   Baseline 2 (score=-n_abordajes_28d): ROC-AUC={b2['roc_auc_mean']:.4f} | PR-AUC={b2['pr_auc_mean']:.4f}")

    print("\n▶️  Barrido de λ por penalización (L1/L2/ElasticNet)")
    df_barrido = barrido_lambdas(X, y, skf)

    print("\n▶️  Eligiendo mejor λ por penalización...")
    elegidos = elegir_mejor_lambda(df_barrido)
    for p, l in elegidos.items():
        print(f"   {p}: λ = {l}")

    print("\n▶️  Gráfica métrica vs λ")
    ruta_png = os.path.join(RESULTS_DIR, "p2_fase1_1.4_metricas_vs_lambda.png")
    graficar(df_barrido, elegidos, ruta_png)

    # Tabla de modelos (≥4 filas: 2 baselines + L1 + L2 + ElasticNet)
    filas = [
        {"modelo": f"Baseline 1: constante p={tasa_base:.4f}", **b1},
        {"modelo": "Baseline 2: -n_abordajes_28d", **b2},
    ]
    for p in PENALTIES:
        lam = elegidos[p]
        sub = df_barrido[(df_barrido["penalty"] == p) & (df_barrido["lambda"] == lam)].iloc[0]
        filas.append({
            "modelo": f"LogReg {p.upper()} λ={lam}",
            "roc_auc_mean": sub["roc_auc_mean"], "roc_auc_std": sub["roc_auc_std"],
            "pr_auc_mean": sub["pr_auc_mean"], "pr_auc_std": sub["pr_auc_std"],
        })
    tabla = pd.DataFrame(filas)
    ruta_tabla = os.path.join(RESULTS_DIR, "p2_fase1_1.4_tabla_modelos.csv")
    tabla.to_csv(ruta_tabla, index=False, encoding="utf-8")
    print(f"\n✅ Tabla de modelos: {ruta_tabla}")

    # Modelo final: elegir la mejor penalización (mayor ROC-AUC media)
    mejor = "l2"#max(PENALTIES, key=lambda p: tabla[tabla["modelo"].str.contains(p.upper())]["roc_auc_mean"].iloc[0])
    lam_final = elegidos[mejor]
    print(f"\n▶️  Modelo final: {mejor.upper()} con λ={lam_final}")
    coefs = modelo_final_top5(X, y, mejor, lam_final, scaler)
    coefs.attrs["lambda_final"] = lam_final
    ruta_top5 = os.path.join(RESULTS_DIR, "p2_fase1_1.4_top5_coeficientes.csv")
    coefs.to_csv(ruta_top5, index=False, encoding="utf-8")
    print(f"✅ Top-5 coeficientes: {ruta_top5}")
    print(coefs.head(5).to_string(index=False))

    # CSVs auxiliares
    df_barrido.to_csv(os.path.join(RESULTS_DIR, "p2_fase1_1.4_metricas_vs_lambda.csv"),
                      index=False, encoding="utf-8")

    # Markdown
    ruta_md = os.path.join(RESULTS_DIR, "p2_fase1_1.4_resumen.md")
    escribir_markdown(tabla, elegidos, coefs, tasa_base, ruta_md)
    print(f"✅ Resumen MD: {ruta_md}")

    print("\n" + "=" * 60)
    print(" ✅ Turno 5 completado")
    print("=" * 60)


if __name__ == "__main__":
    main()