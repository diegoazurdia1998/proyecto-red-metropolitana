"""
Rubric 2.4 — Prueba de invariancia temporal (versión final integrada).

Verifica:
  1) Determinismo: predecir.py 2 veces con los mismos datos → mismo hash.
  2) No-reentrenamiento: el .pkl no cambia entre corridas (predecir.py solo carga).
  3) Invariancia temporal: 90 días vs 120 días al MISMO corte C
     → mismo CSV: misma población, mismos puntajes, mismas tarjetas, mismos contactar.
  4) Reporte: MD con veredicto por componente + 2 gráficas (hist + scatter diagonal).

Uso:
    python -m scripts.p2_prueba_invariancia \
        --datos-90 ./scripts/datos_red \
        --datos-120 ./scripts/P2_datos_red_defensa_120dias \
        --fecha-corte "2026-08-30 00:00:00"
"""
import argparse
import hashlib
import os
import subprocess
import sys

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

RESULTS_DIR = "./results"
MODELO_HGB = "./modelos_entrenados/p2_hgb_v1.0.0.pkl"


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------
def hash_archivo(ruta: str) -> str:
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def correr_predecir(datos: str, fecha_corte: str, sufijo: str) -> tuple[str, str]:
    if not os.path.exists(datos):
        raise FileNotFoundError(f"No existe la carpeta: {datos}")
    cmd = [
        sys.executable, "-m", "scripts.predecir",
        "--fecha-corte", fecha_corte,
        "--datos", datos,
    ]
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    print(f"\n▶️  {' '.join(cmd)}", flush=True)
    res = subprocess.run(
        cmd, capture_output=True, text=True,
        encoding="utf-8", errors="replace",
        env=env,                     # ← clave
    )
    if res.returncode != 0:
        print(res.stdout)
        print(res.stderr)
        raise RuntimeError(f"predecir.py falló (código {res.returncode})")

    # Verificar que cargó el modelo y no reentrenó
    if "Modelo:" not in res.stdout:
        raise RuntimeError(
            "predecir.py no reportó 'Modelo:'. No se puede confirmar que cargó el .pkl."
        )

    tag = fecha_corte.split(" ")[0].replace("-", "")
    ruta_original = os.path.join(RESULTS_DIR, f"p2_predicciones_{tag}.csv")
    if not os.path.exists(ruta_original):
        raise FileNotFoundError(f"No se generó el CSV: {ruta_original}")

    # Conservar copia con sufijo para no pisar entre corridas
    ruta = os.path.join(RESULTS_DIR, f"p2_predicciones_{tag}_{sufijo}.csv")
    os.replace(ruta_original, ruta)
    print(f"✅ CSV: {ruta}")
    return ruta, res.stdout


def comparar_csv(ruta1: str, ruta2: str) -> dict:
    """Compara dos CSV de predicciones por unidad_id."""
    df1 = pd.read_csv(ruta1)
    df2 = pd.read_csv(ruta2)

    for nombre, df in (("90d", df1), ("120d", df2)):
        faltan = {"unidad_id", "tarjetas", "puntaje", "contactar"} - set(df.columns)
        if faltan:
            raise RuntimeError(f"CSV {nombre} sin columnas: {faltan}")

    df1["unidad_id"] = df1["unidad_id"].astype(str)
    df2["unidad_id"] = df2["unidad_id"].astype(str)

    u1 = set(df1["unidad_id"])
    u2 = set(df2["unidad_id"])
    comunes = u1 & u2
    solo1 = u1 - u2
    solo2 = u2 - u1

    res = {
        "n1": len(df1), "n2": len(df2),
        "comunes": len(comunes), "solo1": len(solo1), "solo2": len(solo2),
        "poblacion_identica": (len(solo1) == 0 and len(solo2) == 0),
        "hash1": hash_archivo(ruta1), "hash2": hash_archivo(ruta2),
        "puntajes_identicos": None, "puntajes_max_diff": None,
        "puntajes_mean_diff": None,
        "contactar_distintos": None, "tarjetas_distintas": None,
        "df_comunes": None,
    }

    if comunes:
        m1 = df1[df1["unidad_id"].isin(comunes)].set_index("unidad_id").sort_index()
        m2 = df2[df2["unidad_id"].isin(comunes)].set_index("unidad_id").sort_index()

        diff_p = (m1["puntaje"] - m2["puntaje"]).abs()
        res["puntajes_max_diff"] = float(diff_p.max())
        res["puntajes_mean_diff"] = float(diff_p.mean())
        res["puntajes_identicos"] = bool((diff_p < 1e-9).all())

        res["contactar_distintos"] = int((m1["contactar"] != m2["contactar"]).sum())

        t1 = m1["tarjetas"].fillna("").astype(str).str.strip()
        t2 = m2["tarjetas"].fillna("").astype(str).str.strip()
        res["tarjetas_distintas"] = int((t1 != t2).sum())

        # Guardar para la gráfica scatter
        res["df_comunes"] = pd.DataFrame({
            "puntaje_90d": m1["puntaje"],
            "puntaje_120d": m2["puntaje"],
        })

    res["identicos_bit_a_bit"] = (res["hash1"] == res["hash2"])
    return res


def guardar_diff(res_inv: dict, ruta1: str, ruta2: str, ruta_diff: str) -> int:
    """Guarda un CSV con las unidades que difieren (puntaje, contactar o tarjetas)."""
    if res_inv["comunes"] == 0:
        return 0
    df1 = pd.read_csv(ruta1); df1["unidad_id"] = df1["unidad_id"].astype(str)
    df2 = pd.read_csv(ruta2); df2["unidad_id"] = df2["unidad_id"].astype(str)
    m = df1.merge(df2, on="unidad_id", how="inner",
                  suffixes=("_90d", "_120d"))
    m["dif_puntaje"] = (m["puntaje_90d"] - m["puntaje_120d"]).abs()
    m["dif_contactar"] = (m["contactar_90d"] != m["contactar_120d"])
    t1 = m["tarjetas_90d"].fillna("").astype(str).str.strip()
    t2 = m["tarjetas_120d"].fillna("").astype(str).str.strip()
    m["dif_tarjetas"] = (t1 != t2)
    dif = m[(m["dif_puntaje"] > 1e-9) | m["dif_contactar"] | m["dif_tarjetas"]]
    dif.to_csv(ruta_diff, index=False, encoding="utf-8")
    return len(dif)


# ---------------------------------------------------------------------------
# Gráficas
# ---------------------------------------------------------------------------
def graficar_histogramas(ruta_90: str, ruta_120: str, ruta_png: str):
    """Barras lado a lado: si las distribuciones son idénticas, cada bin
    muestra dos barras de la MISMA altura."""
    df1 = pd.read_csv(ruta_90)
    df2 = pd.read_csv(ruta_120)
    plt.close("all")
    fig, ax = plt.subplots(figsize=(10, 5))
    bins = np.linspace(0, 1, 40)
    width = (bins[1] - bins[0]) * 0.4
    ax.hist(df1["puntaje"], bins=bins, width=width, align="edge",
            label=f"90d (n={len(df1):,})", color="#1f77b4")
    ax.hist(df2["puntaje"], bins=bins, width=width, align="edge",
            label=f"120d (n={len(df2):,})", color="#ff7f0e", alpha=0.9)
    ax.set_xlabel("Puntaje (probabilidad de abandono)")
    ax.set_ylabel("Frecuencia")
    ax.set_title("Distribución de puntajes — corte C (90d vs 120d, mismo modelo)")
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(ruta_png, dpi=120, bbox_inches="tight")
    plt.close()
    print(f"✅ Histograma: {os.path.abspath(ruta_png)}")


def graficar_scatter_diagonal(res_inv: dict, ruta_png: str):
    """Scatter puntaje_90d vs puntaje_120d. Si invariancia pasa → diagonal perfecta."""
    df = res_inv["df_comunes"]
    if df is None or df.empty:
        print("⚠️  Sin unidades comunes; no se genera scatter.")
        return
    plt.close("all")
    fig, ax = plt.subplots(figsize=(6, 6))
    ax.scatter(df["puntaje_90d"], df["puntaje_120d"],
               s=4, alpha=0.25, color="#2ca02c", edgecolors="none")
    ax.plot([0, 1], [0, 1], "r--", linewidth=1, label="Diagonal (identidad)")
    ax.set_xlabel("Puntaje con 90 días")
    ax.set_ylabel("Puntaje con 120 días")
    ax.set_title(f"Scatter de puntajes — corte C\n"
                 f"{len(df):,} unidades comunes; max |Δ| = {res_inv['puntajes_max_diff']:.2e}")
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(ruta_png, dpi=120, bbox_inches="tight")
    plt.close()
    print(f"✅ Scatter: {os.path.abspath(ruta_png)}")


# ---------------------------------------------------------------------------
# Reporte
# ---------------------------------------------------------------------------
def escribir_markdown(res_det, res_inv, hash_modelo_antes, hash_modelo_despues,
                      n_diff, ruta_md, png_hist, png_scatter, fecha_corte):
    veredicto = {
        "determinismo": res_det["identicos_bit_a_bit"],
        "no_reentrena": (hash_modelo_antes == hash_modelo_despues),
        "poblacion_identica": res_inv["poblacion_identica"],
        "puntajes_identicos": bool(res_inv["puntajes_identicos"]),
        "contactar_identicos": res_inv["contactar_distintos"] == 0,
        "tarjetas_identicas": res_inv["tarjetas_distintas"] == 0,
        "csv_identico": res_inv["identicos_bit_a_bit"],
    }
    todo_ok = all(veredicto.values())

    with open(ruta_md, "w", encoding="utf-8") as f:
        f.write("# Fase 2.4 — Prueba de invariancia temporal\n\n")
        f.write(f"**Corte evaluado:** `{fecha_corte}`\n\n")
        f.write("**Definición operativa:** el mismo modelo (`p2_hgb_v1.0.0.pkl`) y el mismo "
                "`predecir.py` se aplican sobre dos datasets:\n\n")
        f.write("- **90 días** (datos originales del proyecto, hasta el 29-ago).\n")
        f.write("- **120 días** (datos con 30 días adicionales, hasta el 28-sep).\n\n")
        f.write("Para el corte C, ambos deben producir **el mismo CSV**. Si difieren, "
                "hay fuga de datos futuros.\n\n")

        f.write("## 0. Veredicto por componente\n\n")
        f.write("| Componente | Resultado |\n|---|---|\n")
        for k, v in veredicto.items():
            f.write(f"| {k} | {'✅' if v else '❌'} |\n")
        f.write(f"\n**Veredicto global:** {'✅ PASA' if todo_ok else '❌ FALLA'}\n\n")

        f.write("## 1. Determinismo (mismos datos, dos corridas)\n\n")
        f.write(f"- Hash corrida 1: `{res_det['hash1'][:16]}...`\n")
        f.write(f"- Hash corrida 2: `{res_det['hash2'][:16]}...`\n")
        f.write(f"- **¿Idénticos bit a bit?** {'✅ Sí' if res_det['identicos_bit_a_bit'] else '❌ No'}\n\n")

        f.write("## 2. No-reentrenamiento del modelo\n\n")
        f.write(f"- Hash del `.pkl` antes:  `{hash_modelo_antes[:16]}...`\n")
        f.write(f"- Hash del `.pkl` después: `{hash_modelo_despues[:16]}...`\n")
        f.write(f"- **¿El modelo cambió?** {'❌ Sí (reentrenó)' if hash_modelo_antes != hash_modelo_despues else '✅ No (solo carga)'}\n\n")

        f.write("## 3. Invariancia temporal (90d vs 120d)\n\n")
        f.write("| Métrica | Valor |\n|---|---|\n")
        f.write(f"| Usuarios en 90d | {res_inv['n1']:,} |\n")
        f.write(f"| Usuarios en 120d | {res_inv['n2']:,} |\n")
        f.write(f"| Usuarios en común | {res_inv['comunes']:,} |\n")
        f.write(f"| Solo en 90d | {res_inv['solo1']:,} |\n")
        f.write(f"| Solo en 120d | {res_inv['solo2']:,} |\n")
        if res_inv["puntajes_identicos"] is not None:
            f.write(f"| Max |Δ puntaje| | {res_inv['puntajes_max_diff']:.2e} |\n")
            f.write(f"| Media |Δ puntaje| | {res_inv['puntajes_mean_diff']:.2e} |\n")
        f.write(f"| Contactar distintos | {res_inv['contactar_distintos']:,} |\n")
        f.write(f"| Tarjetas distintas | {res_inv['tarjetas_distintas']:,} |\n")
        f.write(f"| Unidades en el diff | {n_diff:,} |\n")
        f.write(f"| **¿CSV idéntico bit a bit?** | {'✅ Sí' if res_inv['identicos_bit_a_bit'] else '❌ No'} |\n\n")

        f.write("## 4. Gráficas\n\n")
        f.write(f"### Histograma superpuesto\n\n![Histograma]({os.path.basename(png_hist)})\n\n")
        f.write(f"### Scatter 90d vs 120d (debe ser la diagonal)\n\n![Scatter]({os.path.basename(png_scatter)})\n\n")

        f.write("## 5. Conclusión\n\n")
        if todo_ok:
            f.write("**La prueba de invariancia pasa.** El mismo `predecir.py` y el mismo "
                    "modelo, aplicados sobre 90 y 120 días, producen **exactamente el mismo CSV** "
                    "para el corte C. El pipeline filtra `ts < fecha_corte` antes de calcular "
                    "features, población y tarjetas, y no reentrena. **No hay fuga de datos futuros.**\n\n")
            f.write("**Evidencia:**\n")
            f.write(f"- Hash del CSV 90d:  `{res_inv['hash1'][:16]}...`\n")
            f.write(f"- Hash del CSV 120d: `{res_inv['hash2'][:16]}...`\n")
        else:
            f.write("**⚠️ La prueba FALLA.** Revisar los componentes marcados ❌ arriba.\n\n")
            f.write("Posibles causas:\n")
            f.write("- `predecir.py` no filtra correctamente por `fecha_corte`.\n")
            f.write("- Los datos de 120d tienen eventos previos al corte distintos a los de 90d.\n")
            f.write("- El cálculo de features usa la fecha máxima del dataset en lugar del corte.\n")
            f.write("- La resolución de tarjetas itera sobre registros posteriores al corte.\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--datos-90", required=True)
    parser.add_argument("--datos-120", required=True)
    parser.add_argument("--fecha-corte", default="2026-08-30 00:00:00")
    args = parser.parse_args()

    os.makedirs(RESULTS_DIR, exist_ok=True)
    print("=" * 60)
    print(" RUBRIC 2.4 — Prueba de invariancia temporal (final)")
    print("=" * 60)

    if not os.path.exists(MODELO_HGB):
        print(f"❌ No existe {MODELO_HGB}")
        sys.exit(1)
    print(f"✅ Modelo: {MODELO_HGB}")
    hash_modelo_antes = hash_archivo(MODELO_HGB)

    # 1. Determinismo: 90d dos veces
    print("\n▶️  Paso 1: Determinismo (90d, 2 corridas)")
    ruta_90_1, _ = correr_predecir(args.datos_90, args.fecha_corte, "90d_run1")
    h1 = hash_archivo(ruta_90_1)
    ruta_90_2, _ = correr_predecir(args.datos_90, args.fecha_corte, "90d_run2")
    h2 = hash_archivo(ruta_90_2)
    res_det = {"hash1": h1, "hash2": h2, "identicos_bit_a_bit": (h1 == h2)}
    print(f"   Hash 1: {h1[:16]}...")
    print(f"   Hash 2: {h2[:16]}...")
    print(f"   Determinismo: {'✅' if res_det['identicos_bit_a_bit'] else '❌'}")

    # 2. Invariancia: 90d vs 120d
    print("\n▶️  Paso 2: Invariancia (90d vs 120d)")
    ruta_90, _ = correr_predecir(args.datos_90, args.fecha_corte, "90d")
    ruta_120, _ = correr_predecir(args.datos_120, args.fecha_corte, "120d")
    res_inv = comparar_csv(ruta_90, ruta_120)

    print(f"   Usuarios 90d:  {res_inv['n1']:,}")
    print(f"   Usuarios 120d: {res_inv['n2']:,}")
    print(f"   Comunes:       {res_inv['comunes']:,}")
    print(f"   Solo 90d:      {res_inv['solo1']:,}")
    print(f"   Solo 120d:     {res_inv['solo2']:,}")
    if res_inv["puntajes_identicos"] is not None:
        print(f"   Max |Δ puntaje|: {res_inv['puntajes_max_diff']:.2e}")
    print(f"   Contactar distintos: {res_inv['contactar_distintos']:,}")
    print(f"   Tarjetas distintas:  {res_inv['tarjetas_distintas']:,}")
    print(f"   ¿CSV idéntico bit a bit? {'✅' if res_inv['identicos_bit_a_bit'] else '❌'}")

    # 3. Verificar que el modelo no cambió
    hash_modelo_despues = hash_archivo(MODELO_HGB)
    no_reentrena = (hash_modelo_antes == hash_modelo_despues)
    print(f"\n   ¿Modelo sin cambios? {'✅ Sí' if no_reentrena else '❌ Reentrenó'}")

    # 4. Diff explícito
    ruta_diff = os.path.join(RESULTS_DIR, "p2_invariancia_diff.csv")
    n_diff = guardar_diff(res_inv, ruta_90, ruta_120, ruta_diff)
    print(f"✅ Diff: {ruta_diff} ({n_diff} filas con diferencias)")

    # 5. Gráficas
    print("\n▶️  Paso 3: Gráficas")
    png_hist = os.path.join(RESULTS_DIR, "p2_fase2_2.4_invariancia_hist.png")
    png_scatter = os.path.join(RESULTS_DIR, "p2_fase2_2.4_invariancia_scatter.png")
    graficar_histogramas(ruta_90, ruta_120, png_hist)
    graficar_scatter_diagonal(res_inv, png_scatter)

    # 6. Reporte
    print("\n▶️  Paso 4: Reporte")
    ruta_md = os.path.join(RESULTS_DIR, "p2_fase2_2.4_invariancia.md")
    escribir_markdown(res_det, res_inv, hash_modelo_antes, hash_modelo_despues,
                      n_diff, ruta_md, png_hist, png_scatter, args.fecha_corte)
    print(f"✅ Reporte: {ruta_md}")

    # 7. Veredicto
    todo_ok = (res_det["identicos_bit_a_bit"] and res_inv["identicos_bit_a_bit"]
               and no_reentrena and res_inv["poblacion_identica"])
    print("\n" + "=" * 60)
    print(" ✅ PRUEBA DE INVARIANCIA PASADA" if todo_ok else " ❌ PRUEBA DE INVARIANCIA FALLIDA")
    print("=" * 60)
    sys.exit(0 if todo_ok else 1)


if __name__ == "__main__":
    main()