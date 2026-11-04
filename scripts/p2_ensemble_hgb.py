"""
Rubric 2.1 — Ensemble (HistGradientBoosting) y sesgo-varianza.

Qué hace:
  1. Carga features del corte A (results/p2_features_A.parquet).
  2. Usa los MISMOS folds de 1.4: StratifiedKFold(5, shuffle=True, random_state=42).
  3. Entrena HistGradientBoostingClassifier con hiperparámetros base (Q25).
  4. Reporta train vs val ROC-AUC por fold + media ± std (Q27).
  5. Importancia por permutación EN VALIDACIÓN con scoring='roc_auc', n_repeats=10 (Q26).
  6. Compara contra L2 λ=100 (modelo final de 1.4) y decide cuál va a 2.2.

PROHIBIDO: no lee ni toca el corte B.

Salidas en results/:
  - p2_fase2_2.1_folds.csv
  - p2_fase2_2.1_importancia_permutacion.csv
  - p2_fase2_2.1_resumen.md
"""
import os
import sys

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance
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

N_SPLITS = 5
RANDOM_STATE = 42

HGB_PARAMS = dict(
    max_iter=200,
    learning_rate=0.05,
    max_depth=5,
    min_samples_leaf=20,
    l2_regularization=1.0,
    random_state=RANDOM_STATE,
)

L2_PARAMS = dict(
    penalty="l2",
    C=1.0 / 100.0,   # λ = 100 → C = 0.01 (modelo elegido en 1.4)
    solver="saga",
    max_iter=5000,
    random_state=RANDOM_STATE,
)


def cargar_datos() -> pd.DataFrame:
    if not os.path.exists(PARQUET_A):
        print(f"❌ No existe {PARQUET_A}. Corre: python -m scripts.p2_exportar_features_A")
        sys.exit(1)
    df = pd.read_parquet(PARQUET_A)
    print(f"✅ Cargadas {len(df):,} filas de {PARQUET_A}")

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


def cv_hgb(X, y, skf) -> list:
    filas = []
    for i, (train_idx, val_idx) in enumerate(skf.split(X, y), start=1):
        X_tr, X_val = X[train_idx], X[val_idx]
        y_tr, y_val = y[train_idx], y[val_idx]

        model = HistGradientBoostingClassifier(**HGB_PARAMS)
        model.fit(X_tr, y_tr)

        p_tr = model.predict_proba(X_tr)[:, 1]
        p_val = model.predict_proba(X_val)[:, 1]

        filas.append({
            "fold": i,
            "n_train": len(train_idx),
            "n_val": len(val_idx),
            "roc_auc_train": roc_auc_score(y_tr, p_tr),
            "roc_auc_val": roc_auc_score(y_val, p_val),
            "pr_auc_train": average_precision_score(y_tr, p_tr),
            "pr_auc_val": average_precision_score(y_val, p_val),
        })
    df = pd.DataFrame(filas)
    df["delta_roc_auc"] = df["roc_auc_train"] - df["roc_auc_val"]
    return df


def cv_l2(X, y, skf) -> dict:
    roc, pr = [], []
    for train_idx, val_idx in skf.split(X, y):
        X_tr, X_val = X[train_idx], X[val_idx]
        y_tr, y_val = y[train_idx], y[val_idx]
        model = LogisticRegression(**L2_PARAMS)
        model.fit(X_tr, y_tr)
        p_val = model.predict_proba(X_val)[:, 1]
        roc.append(roc_auc_score(y_val, p_val))
        pr.append(average_precision_score(y_val, p_val))
    return {"roc_auc_mean": float(np.mean(roc)), "roc_auc_std": float(np.std(roc)),
            "pr_auc_mean": float(np.mean(pr)), "pr_auc_std": float(np.std(pr))}


def importancia_permutacion(X, y, skf) -> pd.DataFrame:
    """
    Calcula importancia por permutación EN VALIDACIÓN (promedio de 5 folds).
    Para cada fold: entrena HGB con train, calcula permutation_importance sobre val.
    """
    importancias = []
    for i, (train_idx, val_idx) in enumerate(skf.split(X, y), start=1):
        X_tr, X_val = X[train_idx], X[val_idx]
        y_tr, y_val = y[train_idx], y[val_idx]

        model = HistGradientBoostingClassifier(**HGB_PARAMS)
        model.fit(X_tr, y_tr)

        res = permutation_importance(
            model, X_val, y_val,
            scoring="roc_auc",
            n_repeats=10,
            random_state=RANDOM_STATE,
            n_jobs=-1,
        )
        importancias.append({
            "fold": i,
            "feature": FEATURE_COLS,
            "importance_mean": res.importances_mean,
            "importance_std": res.importances_std,
        })

    # Concatenar y promediar por feature
    df_all = pd.concat([pd.DataFrame(d) for d in importancias], ignore_index=True)
    df_res = (
        df_all.groupby("feature")["importance_mean"]
        .agg(["mean", "std"])
        .reset_index()
    )

    df_res.columns = [
        "feature",
        "importance_mean",
        "importance_std_across_folds"
    ]
    df_res = df_res.sort_values("importance_mean", ascending=False).reset_index(drop=True)
    return df_res


def graficar(df_folds: pd.DataFrame, df_imp: pd.DataFrame, ruta_png: str):
    plt.close("all")
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # Subgráfica 1: train vs val por fold
    x = df_folds["fold"]
    axes[0].plot(x, df_folds["roc_auc_train"], marker="o", label="Train")
    axes[0].plot(x, df_folds["roc_auc_val"], marker="s", label="Validación")
    axes[0].set_xlabel("Fold")
    axes[0].set_ylabel("ROC-AUC")
    axes[0].set_title("HGB — train vs validación por fold")
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)
    axes[0].set_xticks(x)

    # Subgráfica 2: importancia por permutación
    df_imp_sorted = df_imp.sort_values("importance_mean", ascending=True)
    y_pos = np.arange(len(df_imp_sorted))
    axes[1].barh(y_pos, df_imp_sorted["importance_mean"],
                 xerr=df_imp_sorted["importance_std_across_folds"], capsize=3)
    axes[1].set_yticks(y_pos)
    axes[1].set_yticklabels(df_imp_sorted["feature"])
    axes[1].set_xlabel("Importancia (caída de ROC-AUC al permutar)")
    axes[1].set_title("Importancia por permutación en validación")
    axes[1].grid(True, alpha=0.3, axis="x")

    plt.tight_layout()
    ruta_abs = os.path.abspath(ruta_png)
    if os.path.exists(ruta_abs):
        try:
            with open(ruta_abs, "a+b"):
                pass
        except PermissionError:
            raise RuntimeError(
                f"El archivo {ruta_abs} está bloqueado. Cerralo y volvé a correr."
            )
    plt.savefig(ruta_abs, dpi=120, bbox_inches="tight")
    plt.close()
    print(f"✅ Gráfica: {ruta_abs}")


def decidir(df_folds: pd.DataFrame, l2_res: dict) -> dict:
    hgb_roc = df_folds["roc_auc_val"].mean()
    hgb_roc_std = df_folds["roc_auc_val"].std()
    hgb_delta = df_folds["delta_roc_auc"].mean()
    l2_roc = l2_res["roc_auc_mean"]

    if hgb_roc > l2_roc + 0.005:
        eleccion = "HGB"
        motivo = (f"HGB supera a L2 por {hgb_roc - l2_roc:+.4f} en ROC-AUC de validación. "
                  "Se lleva HGB a 2.2.")
    elif hgb_roc < l2_roc - 0.005:
        eleccion = "L2"
        motivo = (f"L2 supera a HGB por {l2_roc - hgb_roc:+.4f} en ROC-AUC de validación. "
                  "Se lleva L2 a 2.2 (modelo de 1.4).")
    else:
        eleccion = "Empate técnico"
        motivo = (f"Diferencia de ROC-AUC entre HGB y L2 es marginal "
                  f"({hgb_roc - l2_roc:+.4f}). Se recomienda L2 por interpretabilidad "
                  "y porque ya es el modelo final de 1.4.")

    overfit = "sí" if hgb_delta > 0.05 else ("leve" if hgb_delta > 0.02 else "no")

    return {
        "eleccion": eleccion,
        "motivo": motivo,
        "hgb_roc_media": hgb_roc,
        "hgb_roc_std": hgb_roc_std,
        "hgb_delta_train_val": hgb_delta,
        "l2_roc_media": l2_roc,
        "overfitting": overfit,
    }


def escribir_markdown(df_folds, df_imp, l2_res, decision, ruta_md):
    with open(ruta_md, "w", encoding="utf-8") as f:
        f.write("# Fase 2.1 — Ensemble (HGB) y sesgo-varianza\n\n")

        f.write("## Configuración\n\n")
        f.write(f"- Modelo: `HistGradientBoostingClassifier` con hiperparámetros base.\n")
        f.write(f"- Folds: `StratifiedKFold(n_splits={N_SPLITS}, shuffle=True, random_state={RANDOM_STATE})` (los MISMOS de 1.4).\n")
        f.write(f"- Importancia por permutación con `scoring='roc_auc'`, `n_repeats=10`, calculada **en validación**.\n")
        f.write(f"- Corte B **NO** se tocó.\n\n")

        f.write("## Tabla 1: HGB — train vs validación por fold\n\n")
        f.write("| Fold | n_train | n_val | ROC-AUC train | ROC-AUC val | Δ (train − val) |\n")
        f.write("|---|---|---|---|---|---|\n")
        for _, r in df_folds.iterrows():
            f.write(f"| {int(r['fold'])} | {int(r['n_train']):,} | {int(r['n_val']):,} | "
                    f"{r['roc_auc_train']:.4f} | {r['roc_auc_val']:.4f} | "
                    f"{r['delta_roc_auc']:+.4f} |\n")
        f.write("\n")
        f.write(f"**Resumen:**\n\n")
        f.write(f"- ROC-AUC val (media ± std): **{df_folds['roc_auc_val'].mean():.4f} ± {df_folds['roc_auc_val'].std():.4f}**\n")
        f.write(f"- ROC-AUC train (media ± std): {df_folds['roc_auc_train'].mean():.4f} ± {df_folds['roc_auc_train'].std():.4f}\n")
        f.write(f"- Δ promedio (train − val): {df_folds['delta_roc_auc'].mean():+.4f}\n\n")

        f.write("## Comparación HGB vs L2 (modelo final de 1.4)\n\n")
        f.write("| Modelo | ROC-AUC val (media ± std) | PR-AUC val (media ± std) |\n")
        f.write("|---|---|---|\n")
        f.write(f"| L2 λ=100 (1.4) | {l2_res['roc_auc_mean']:.4f} ± {l2_res['roc_auc_std']:.4f} | "
                f"{l2_res['pr_auc_mean']:.4f} ± {l2_res['pr_auc_std']:.4f} |\n")
        f.write(f"| **HGB** | **{df_folds['roc_auc_val'].mean():.4f} ± {df_folds['roc_auc_val'].std():.4f}** | "
                f"{df_folds['pr_auc_val'].mean():.4f} ± {df_folds['pr_auc_val'].std():.4f} |\n\n")

        f.write("## Tabla 2: Importancia por permutación (en validación)\n\n")
        f.write("| Feature | Importancia media | Std entre folds |\n")
        f.write("|---|---|---|\n")
        for _, r in df_imp.iterrows():
            f.write(f"| `{r['feature']}` | {r['importance_mean']:+.4f} | {r['importance_std_across_folds']:.4f} |\n")
        f.write("\n")
        f.write("**Interpretación:** la importancia mide cuánto cae la ROC-AUC de validación "
                "cuando se permuta esa feature. Valores **positivos** = la feature aporta. "
                "Valores **negativos o ~0** = la feature es ruido o redundante para el modelo.\n\n")

        f.write("## Sesgo-varianza\n\n")
        f.write(f"- **Δ (train − val) promedio:** {decision['hgb_delta_train_val']:+.4f}\n")
        f.write(f"- **Diagnóstico:** {decision['overfitting']} sobreajuste.\n")
        if decision["overfitting"] == "no":
            f.write("  El modelo generaliza bien (Δ < 0.02).\n\n")
        elif decision["overfitting"] == "leve":
            f.write("  Hay un sobreajuste leve (0.02 < Δ < 0.05). Es esperable en HGB con max_iter=200. "
                    "Si se necesita, reducir max_iter o max_depth.\n\n")
        else:
            f.write("  **Alerta:** sobreajuste significativo (Δ > 0.05). Considerar reducir "
                    "max_iter o max_depth en 2.2.\n\n")

        f.write("## Decisión justificada\n\n")
        f.write(f"**Modelo elegido para 2.2: `{decision['eleccion']}`**\n\n")
        f.write(f"{decision['motivo']}\n\n")
        if decision["eleccion"] == "L2":
            f.write("L2 λ=100 tiene además la ventaja de tener coeficientes interpretables en las 10 features, "
                    "lo cual es útil para la ficha del modelo (2.4).\n\n")
        elif decision["eleccion"] == "HGB":
            f.write("HGB captura interacciones no lineales que L2 no puede modelar. "
                    "Se documenta la importancia por permutación como contraparte interpretativa.\n\n")
        else:
            f.write("En empate técnico, se prioriza L2 por interpretabilidad y porque ya está "
                    "entrenado y validado en 1.4.\n\n")

        f.write("## Nota: corte B NO se tocó\n\n")
        f.write("Por el rubric 2.1, el corte B sigue intocable. El test único se hace en 2.2.\n")


def main():
    print("=" * 60)
    print(" RUBRIC 2.1 — Ensemble (HGB) y sesgo-varianza")
    print("=" * 60)

    df = cargar_datos()
    X, y, scaler = preparar_X_y(df)
    skf = StratifiedKFold(n_splits=N_SPLITS, shuffle=True, random_state=RANDOM_STATE)

    print("\n▶️  CV HGB (5 folds, mismos folds de 1.4)")
    df_folds = cv_hgb(X, y, skf)
    print(df_folds[["fold", "roc_auc_train", "roc_auc_val", "delta_roc_auc"]].to_string(index=False))

    print("\n▶️  CV L2 (referencia de 1.4)")
    l2_res = cv_l2(X, y, skf)
    print(f"   L2 ROC-AUC val = {l2_res['roc_auc_mean']:.4f} ± {l2_res['roc_auc_std']:.4f}")

    print("\n▶️  Importancia por permutación en validación")
    df_imp = importancia_permutacion(X, y, skf)
    print(df_imp.to_string(index=False))

    print("\n▶️  Gráfica")
    ruta_png = os.path.join(RESULTS_DIR, "p2_fase2_2.1_train_val_e_importancia.png")
    graficar(df_folds, df_imp, ruta_png)

    print("\n▶️  Decisión")
    decision = decidir(df_folds, l2_res)
    print(f"   Elección: {decision['eleccion']}")
    print(f"   Motivo: {decision['motivo']}")
    print(f"   Overfitting: {decision['overfitting']} (Δ = {decision['hgb_delta_train_val']:+.4f})")

    # Guardar CSVs
    df_folds.to_csv(os.path.join(RESULTS_DIR, "p2_fase2_2.1_folds.csv"),
                    index=False, encoding="utf-8")
    df_imp.to_csv(os.path.join(RESULTS_DIR, "p2_fase2_2.1_importancia_permutacion.csv"),
                  index=False, encoding="utf-8")
    print(f"\n✅ CSVs guardados en results/")

    # MD
    ruta_md = os.path.join(RESULTS_DIR, "p2_fase2_2.1_resumen.md")
    escribir_markdown(df_folds, df_imp, l2_res, decision, ruta_md)
    print(f"✅ MD: {ruta_md}")

    print("\n" + "=" * 60)
    print(" ✅ Turno 6 completado")
    print("=" * 60)


if __name__ == "__main__":
    main()