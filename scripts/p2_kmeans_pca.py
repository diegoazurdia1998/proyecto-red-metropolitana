"""
Rubric 2.3 — KMeans + PCA + segmentos + acciones de retención.

Qué hace:
  1. Carga features del corte A (results/p2_features_A.parquet).
  2. Usa 5 features comportamentales (Q37): n_abordajes_28d, n_dias_activos_28d,
     n_modos_usados_28d, recencia_dias_ultimo_abordaje, n_estaciones_distintas_28d.
  3. Estandariza (StandardScaler).
  4. Barrido k in {2..8}: inercia (codo) + silueta.
  5. Elige k por silueta (Q39).
  6. Ajusta KMeans final y asigna segmentos.
  7. PCA 2D para visualización (varianza explicada en el título).
  8. Describe segmentos por medianas + tamaño + tasa de abandono en A (Q40).
  9. Propone acciones de retención por segmento (Q42).

NO usa el segmento como feature del modelo (Q41).

Salidas en results/:
  - p2_fase2_2.3_k_seleccion.png
  - p2_fase2_2.3_pca_2d.png
  - p2_fase2_2.3_segmentos.csv        (asignación usuario→segmento)
  - p2_fase2_2.3_perfil_segmentos.csv (medianas + tamaño + tasa churn)
  - p2_fase2_2.3_resumen.md           (tablas + acciones de retención)
"""
import os
import sys

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

load_dotenv()

RESULTS_DIR = "./results"
MODELOS_DIR = "./modelos_entrenados"
PARQUET_A = os.path.join(RESULTS_DIR, "p2_features_A.parquet")

# 5 features comportamentales (Q37). Excluye pct_fin_semana, pct_hora_pico,
# n_abordajes_7d y n_abordajes_14d (temporales/redundantes para perfilar tipos).
CLUSTER_FEATURES = [
    "n_abordajes_28d",
    "n_dias_activos_28d",
    "n_modos_usados_28d",
    "recencia_dias_ultimo_abordaje",
    "n_estaciones_distintas_28d",
]

# Nombres legibles para tablas
FEATURE_LABELS = {
    "n_abordajes_28d": "Abordajes en 28d",
    "n_dias_activos_28d": "Días activos en 28d",
    "n_modos_usados_28d": "Modos usados",
    "recencia_dias_ultimo_abordaje": "Días desde último abordaje",
    "n_estaciones_distintas_28d": "Estaciones distintas en 28d",
}

K_RANGE = list(range(2, 9))   # Q39: k ∈ {2..8}
RANDOM_STATE = 42

# Acciones de retención por tipo de segmento. Se mapean por "perfil" después
# de ver las medianas; acá van las plantillas canónicas.
ACCIONES_RETENCION = {
    "intensivos_regulares": "Programa de lealtad con puntos por viaje + upgrades ocasionales.",
    "nuevos_esporadicos": "Onboarding personalizado (1er mes) + descuento en el segundo viaje.",
    "regulares_hora_pico": "Notificaciones de rutas y alertas de servicio; mantener comunicación semanal.",
    "baja_frecuencia": "Campaña de reactivación vía SMS/email con beneficio por volver.",
    "multi_modo_intensivos": "Atención VIP / tarifa preferencial por uso multimodal.",
    "inactivos_en_riesgo": "Contacto personal de retención con oferta de valor (descuento significativo).",
}


def cargar_datos() -> pd.DataFrame:
    if not os.path.exists(PARQUET_A):
        print(f"❌ No existe {PARQUET_A}. Corre: python -m scripts.p2_exportar_features_A")
        sys.exit(1)
    df = pd.read_parquet(PARQUET_A)
    print(f"✅ Cargadas {len(df):,} filas de {PARQUET_A}")

    faltantes = [c for c in CLUSTER_FEATURES if c not in df.columns]
    if faltantes:
        print(f"❌ Faltan features: {faltantes}")
        sys.exit(1)

    df = df.dropna(subset=CLUSTER_FEATURES).reset_index(drop=True)
    print(f"   Tras dropna: {len(df):,} filas")
    return df


def estandarizar(df: pd.DataFrame):
    scaler = StandardScaler()
    X = scaler.fit_transform(df[CLUSTER_FEATURES].values)
    return X, scaler


def barrido_k(X) -> pd.DataFrame:
    filas = []
    for k in K_RANGE:
        km = KMeans(n_clusters=k, random_state=RANDOM_STATE, n_init=10)
        labels = km.fit_predict(X)
        inercia = km.inertia_
        sil = silhouette_score(X, labels)
        filas.append({"k": k, "inercia": inercia, "silueta": sil})
        print(f"   k={k}: inercia={inercia:,.1f}  silueta={sil:.4f}")
    return pd.DataFrame(filas)


def elegir_k(df_k: pd.DataFrame) -> int:
    """Elige k por silueta (Q39)."""
    idx = df_k["silueta"].idxmax()
    return int(df_k.loc[idx, "k"])


def graficar_k(df_k: pd.DataFrame, k_opt: int, ruta_png: str):
    plt.close("all")
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))

    axes[0].plot(df_k["k"], df_k["inercia"], marker="o", color="#1f77b4")
    axes[0].axvline(k_opt, color="red", linestyle="--", label=f"k elegido = {k_opt}")
    axes[0].set_xlabel("k")
    axes[0].set_ylabel("Inercia (WCSS)")
    axes[0].set_title("Método del codo")
    axes[0].legend()
    axes[0].grid(alpha=0.3)

    axes[1].plot(df_k["k"], df_k["silueta"], marker="s", color="#2ca02c")
    axes[1].axvline(k_opt, color="red", linestyle="--", label=f"k elegido = {k_opt}")
    axes[1].set_xlabel("k")
    axes[1].set_ylabel("Silueta media")
    axes[1].set_title("Silueta por k (mayor = mejor)")
    axes[1].legend()
    axes[1].grid(alpha=0.3)

    plt.tight_layout()
    _guardar_png(ruta_png)


def pca_y_graficar(X, labels: np.ndarray, ruta_png: str):
    pca = PCA(n_components=2, random_state=RANDOM_STATE)
    X_pca = pca.fit_transform(X)
    var_exp = pca.explained_variance_ratio_
    total_var = var_exp.sum() * 100

    plt.close("all")
    fig, ax = plt.subplots(figsize=(10, 6))
    unique = np.unique(labels)
    colores = plt.cm.tab10(np.linspace(0, 1, len(unique)))
    for c, color in zip(unique, colores):
        mask = labels == c
        ax.scatter(X_pca[mask, 0], X_pca[mask, 1],
                   s=8, alpha=0.5, color=color, label=f"Segmento {c} (n={mask.sum():,})")
    ax.set_xlabel(f"PC1 ({var_exp[0]*100:.1f}% varianza)")
    ax.set_ylabel(f"PC2 ({var_exp[1]*100:.1f}% varianza)")
    ax.set_title(f"PCA 2D — segmentos KMeans (varianza explicada total: {total_var:.1f}%)")
    ax.legend(markerscale=3, loc="best")
    ax.grid(alpha=0.3)

    plt.tight_layout()
    _guardar_png(ruta_png)
    return pca, var_exp


def _guardar_png(ruta_png: str):
    ruta_abs = os.path.abspath(ruta_png)
    if os.path.exists(ruta_abs):
        try:
            with open(ruta_abs, "a+b"):
                pass
        except PermissionError:
            raise RuntimeError(f"El archivo {ruta_abs} está bloqueado. Cerralo y volvé a correr.")
    plt.savefig(ruta_abs, dpi=120, bbox_inches="tight")
    plt.close()
    print(f"✅ Gráfica: {ruta_abs}")


def perfil_segmentos(df: pd.DataFrame, labels: np.ndarray) -> pd.DataFrame:
    df = df.copy()
    df["segmento"] = labels
    filas = []
    for s in sorted(df["segmento"].unique()):
        sub = df[df["segmento"] == s]
        fila = {"segmento": int(s), "n_usuarios": len(sub)}
        for f in CLUSTER_FEATURES:
            fila[f"mediana_{f}"] = float(sub[f].median())
        if "etiqueta" in sub.columns:
            fila["tasa_abandono_pct"] = 100.0 * float(sub["etiqueta"].mean())
        filas.append(fila)
    return pd.DataFrame(filas).sort_values("n_usuarios", ascending=False).reset_index(drop=True)


def clasificar_segmento(fila: pd.Series, df_perfil: pd.DataFrame) -> str:
    """
    Traduce el perfil numérico a una etiqueta interpretable. Es heurística.
    """
    med_abordajes = fila["mediana_n_abordajes_28d"]
    med_recencia = fila["mediana_recencia_dias_ultimo_abordaje"]
    med_modos = fila["mediana_n_modos_usados_28d"]

    max_abordajes = df_perfil["mediana_n_abordajes_28d"].max()
    min_abordajes = df_perfil["mediana_n_abordajes_28d"].min()
    max_recencia = df_perfil["mediana_recencia_dias_ultimo_abordaje"].max()

    if med_abordajes >= 0.75 * max_abordajes and med_modos >= 2:
        return "intensivos_regulares"
    if med_recencia >= 0.75 * max_recencia and med_abordajes <= 0.30 * max_abordajes:
        return "inactivos_en_riesgo"
    if med_abordajes >= 0.60 * max_abordajes and med_modos <= 1:
        return "regulares_hora_pico"
    if med_abordajes <= max(min_abordajes * 1.5, 5):
        return "baja_frecuencia"
    if med_modos >= 2:
        return "multi_modo_intensivos"
    return "nuevos_esporadicos"


def escribir_markdown(df_k, k_opt, df_perfil, pca, var_exp, df_seg_user, ruta_md):
    with open(ruta_md, "w", encoding="utf-8") as f:
        f.write("# Fase 2.3 — KMeans + PCA + segmentos y retención\n\n")

        f.write("## Configuración\n\n")
        f.write(f"- Features de clustering ({len(CLUSTER_FEATURES)}):\n")
        for c in CLUSTER_FEATURES:
            f.write(f"  - `{c}` — {FEATURE_LABELS[c]}\n")
        f.write(f"- Estandarización: `StandardScaler` (media 0, std 1).\n")
        f.write(f"- Barrido `k ∈ {{{min(K_RANGE)}..{max(K_RANGE)}}}`.\n")
        f.write(f"- **k elegido: {k_opt}** (mayor silueta).\n")
        f.write(f"- Segmentos NO se usan como feature del modelo (Q41).\n\n")

        f.write("## Tabla 1: Barrido de k\n\n")
        f.write("| k | Inercia (WCSS) | Silueta |\n")
        f.write("|---|---|---|\n")
        for _, r in df_k.iterrows():
            marker = "**" if int(r["k"]) == k_opt else ""
            f.write(f"| {marker}{int(r['k'])}{marker} | {r['inercia']:,.1f} | {r['silueta']:.4f} |\n")
        f.write("\n")

        f.write("## Tabla 2: Perfil de segmentos (medianas + tamaño + tasa de abandono)\n\n")
        f.write("| Segmento | n | % pob. | Abordajes 28d | Días activos | Modos | Recencia (d) | Estaciones | Tasa abandono |\n")
        f.write("|---|---|---|---|---|---|---|---|---|\n")
        total = df_perfil["n_usuarios"].sum()
        for _, r in df_perfil.iterrows():
            pct = 100.0 * r["n_usuarios"] / total
            f.write(
                f"| {int(r['segmento'])} | {int(r['n_usuarios']):,} | {pct:.1f}% | "
                f"{r['mediana_n_abordajes_28d']:.1f} | "
                f"{r['mediana_n_dias_activos_28d']:.1f} | "
                f"{r['mediana_n_modos_usados_28d']:.1f} | "
                f"{r['mediana_recencia_dias_ultimo_abordaje']:.1f} | "
                f"{r['mediana_n_estaciones_distintas_28d']:.1f} | "
                f"{r.get('tasa_abandono_pct', float('nan')):.2f}% |\n"
            )
        f.write("\n")

        f.write("## Tabla 3: PCA 2D — varianza explicada\n\n")
        f.write(f"- PC1: **{var_exp[0]*100:.2f}%**\n")
        f.write(f"- PC2: **{var_exp[1]*100:.2f}%**\n")
        f.write(f"- Total (2D): **{var_exp.sum()*100:.2f}%**\n\n")
        f.write("El gráfico PCA 2D muestra la separación visual de los segmentos. "
                "La varianza explicada indica cuánta información retienen los dos componentes.\n\n")

        f.write("## Tabla 4: Acciones de retención por segmento\n\n")
        f.write("La clasificación de cada segmento es heurística, basada en las medianas.\n\n")
        f.write("| Segmento | Tipo | Acción sugerida |\n")
        f.write("|---|---|---|\n")
        for _, r in df_perfil.iterrows():
            tipo = r.get("tipo", "sin_clasificar")
            accion = ACCIONES_RETENCION.get(tipo, "Revisar perfil; acción por definir.")
            f.write(f"| {int(r['segmento'])} | `{tipo}` | {accion} |\n")
        f.write("\n")

        f.write("## Notas\n\n")
        f.write("- Los segmentos se calculan sobre el corte A (Q38).\n")
        f.write("- La tasa de abandono por segmento se calcula como `mean(etiqueta)` en A.\n")
        f.write("- Los segmentos son una capa de análisis post-modelo; **no se usan como feature** "
                "(Q41), por lo que no violan el test único en B ni requieren recalcular la CV.\n")
        f.write("- Archivo de asignación usuario→segmento: `results/p2_fase2_2.3_segmentos.csv`.\n")


def main():
    print("=" * 60)
    print(" RUBRIC 2.3 — KMeans + PCA + segmentos")
    print("=" * 60)

    df = cargar_datos()
    X, scaler = estandarizar(df)
    print(f"   Features usadas: {CLUSTER_FEATURES}")
    print(f"   Shape: {X.shape}")

    print(f"\n▶️  Barrido de k ∈ {{{min(K_RANGE)}..{max(K_RANGE)}}}")
    df_k = barrido_k(X)

    k_opt = elegir_k(df_k)
    print(f"\n✅ k elegido por silueta: {k_opt}")

    print(f"\n▶️  Gráfica codo + silueta")
    ruta_k_png = os.path.join(RESULTS_DIR, "p2_fase2_2.3_k_seleccion.png")
    graficar_k(df_k, k_opt, ruta_k_png)

    print(f"\n▶️  Ajustando KMeans final con k={k_opt}")
    km = KMeans(n_clusters=k_opt, random_state=RANDOM_STATE, n_init=10)
    labels = km.fit_predict(X)
    print(f"   Inercia: {km.inertia_:,.1f}")
    print(f"   Silueta: {silhouette_score(X, labels):.4f}")

    print("\n▶️  PCA 2D")
    ruta_pca_png = os.path.join(RESULTS_DIR, "p2_fase2_2.3_pca_2d.png")
    pca, var_exp = pca_y_graficar(X, labels, ruta_pca_png)

    print("\n▶️  Perfil de segmentos")
    df_perfil = perfil_segmentos(df, labels)
    # Clasificar tipo
    df_perfil["tipo"] = df_perfil.apply(lambda r: clasificar_segmento(r, df_perfil), axis=1)
    print(df_perfil[["segmento", "n_usuarios", "tipo", "tasa_abandono_pct"]].to_string(index=False))

    print("\n▶️  Guardando artefactos")
    df_seg_user = df[["sk_usuario"] + CLUSTER_FEATURES].copy()
    df_seg_user["segmento"] = labels
    df_seg_user.to_csv(os.path.join(RESULTS_DIR, "p2_fase2_2.3_segmentos.csv"),
                       index=False, encoding="utf-8")
    df_k.to_csv(os.path.join(RESULTS_DIR, "p2_fase2_2.3_k_seleccion.csv"),
                index=False, encoding="utf-8")
    df_perfil.to_csv(os.path.join(RESULTS_DIR, "p2_fase2_2.3_perfil_segmentos.csv"),
                     index=False, encoding="utf-8")

    # Guardar KMeans + scaler para asignar C en 2.4
    import joblib
    os.makedirs(MODELOS_DIR, exist_ok=True)
    joblib.dump(
        {"kmeans": km, "scaler": scaler, "features": CLUSTER_FEATURES,
         "k": k_opt, "perfil": df_perfil},
        os.path.join(MODELOS_DIR, "p2_kmeans_v1.0.0.pkl"),
    )
    print(f"✅ KMeans guardado: {MODELOS_DIR}/p2_kmeans_v1.0.0.pkl")

    ruta_md = os.path.join(RESULTS_DIR, "p2_fase2_2.3_resumen.md")
    escribir_markdown(df_k, k_opt, df_perfil, pca, var_exp, df_seg_user, ruta_md)
    print(f"✅ MD: {ruta_md}")

    print("\n" + "=" * 60)
    print(" ✅ Turno 8 completado")
    print("=" * 60)


if __name__ == "__main__":
    main()