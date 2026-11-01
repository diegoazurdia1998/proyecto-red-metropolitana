"""
Exporta el resumen por corte (rubric 1.2) a CSV + Markdown.

- Lee de features.p2_resumen_por_corte (generado por dbt).
- Salvaguarda: si p2_features_etiqueta_por_corte tiene datos para un corte,
  compara poblacion_elegible y positivos. Aborta si hay discrepancia.
- Genera:
    results/p2_fase1_1.2_resumen_cortes.csv
    results/p2_fase1_1.2_resumen.md
"""
import os
import sys
from datetime import datetime

import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

RESULTS_DIR = "./results"


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("DB_USER", "amt_admin"),
        password=os.getenv("DB_PASSWORD"),
        dbname=os.getenv("DB_NAME", "amt_dw"),
    )


def leer_resumen(conn) -> pd.DataFrame:
    sql = """
        SELECT corte, fecha_corte, poblacion_total, poblacion_elegible,
               positivos_churn, tasa_churn_pct
        FROM features.p2_resumen_por_corte
        ORDER BY fecha_corte
    """
    return pd.read_sql(sql, conn)


def verificar_contra_features(conn, resumen: pd.DataFrame) -> list:
    """Salvaguarda: compara resumen vs p2_features_etiqueta_por_corte."""
    errores = []
    for _, row in resumen.iterrows():
        corte = row["corte"]
        fecha = row["fecha_corte"]

        sql = f"""
            SELECT
                COUNT(*) FILTER (WHERE en_poblacion)                    AS poblacion_elegible,
                COUNT(*) FILTER (WHERE en_poblacion AND etiqueta = 1)   AS positivos
            FROM features.p2_features_etiqueta_por_corte
            WHERE fecha_corte = TIMESTAMP '{fecha}'
        """
        df_ref = pd.read_sql(sql, conn)
        if df_ref.empty:
            # El corte no está en la DB (porque solo se guarda el último).
            # No es error, solo se informa.
            continue

        ref_pob = int(df_ref["poblacion_elegible"].iloc[0]) if pd.notna(df_ref["poblacion_elegible"].iloc[0]) else 0
        ref_pos = int(df_ref["positivos"].iloc[0]) if pd.notna(df_ref["positivos"].iloc[0]) else 0

        if ref_pob == 0 and ref_pos == 0:
            # Corte C (etiqueta NULL) o tabla vacía → no comparable.
            continue

        if ref_pob != int(row["poblacion_elegible"]):
            errores.append(
                f"Corte {corte}: poblacion_elegible resumen={row['poblacion_elegible']} "
                f"vs features={ref_pob}"
            )
        if pd.notna(row["positivos_churn"]) and ref_pos != int(row["positivos_churn"]):
            errores.append(
                f"Corte {corte}: positivos_churn resumen={row['positivos_churn']} "
                f"vs features={ref_pos}"
            )
    return errores


def generar_markdown(resumen: pd.DataFrame, ruta_md: str, errores: list):
    ahora = datetime.now().isoformat(timespec="seconds")
    with open(ruta_md, "w", encoding="utf-8") as f:
        f.write("# Fase 1.2 — Resumen por corte: población, positivos y tasa de churn\n\n")
        f.write(f"_Generado: {ahora}_\n\n")

        f.write("## Tabla 1: Corte / Población / Positivos / Tasa (cuenta completa)\n\n")
        f.write(resumen.to_markdown(index=False))
        f.write("\n\n")

        f.write("## Cuenta completa (paso a paso)\n\n")
        for _, r in resumen.iterrows():
            f.write(f"### Corte {r['corte']} ({r['fecha_corte']})\n\n")
            f.write(f"- **Población total (con ≥2 abordajes en 28d):** {int(r['poblacion_total']):,}\n")
            f.write(f"- **Población elegible (sin DELETE CDC previo):** {int(r['poblacion_elegible']):,}\n")
            if pd.isna(r["positivos_churn"]):
                f.write(f"- **Positivos (churn):** NULL (etiqueta oculta por diseño)\n")
                f.write(f"- **Tasa de churn:** NULL\n\n")
            else:
                f.write(f"- **Positivos (churn):** {int(r['positivos_churn']):,}\n")
                f.write(f"- **Tasa de churn:** {r['tasa_churn_pct']:.2f}%\n\n")

        f.write("## Notas metodológicas\n\n")
        f.write("- **Población:** unidades con ≥2 abordajes válidos en los 28 días previos al corte, y sin un DELETE del CDC aplicado antes del corte.\n")
        f.write("- **Etiqueta:** 1 si la unidad NO tiene ningún abordaje válido (en ningún modo) en los 30 días siguientes al corte; 0 si tiene al menos uno.\n")
        f.write("- **Corte C:** etiqueta oculta por diseño del proyecto (no se evalúa, se usa para uso real / invariancia).\n")
        f.write("- **Unidad:** persona (`sk_usuario` canónico). Ver `p2_fase1_1.2_costo_unidad.md`.\n")

        if errores:
            f.write("\n## ⚠️ Salvaguarda: discrepancias detectadas\n\n")
            for e in errores:
                f.write(f"- {e}\n")
        else:
            f.write("\n## ✅ Salvaguarda: consistencia verificada con `p2_features_etiqueta_por_corte`\n")


def main():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    conn = get_db_connection()
    try:
        resumen = leer_resumen(conn)
        if resumen.empty:
            print("❌ features.p2_resumen_por_corte está vacía. Corre dbt run --select p2_resumen_por_corte.")
            sys.exit(1)

        ruta_csv = os.path.join(RESULTS_DIR, "p2_fase1_1.2_resumen_cortes.csv")
        resumen.to_csv(ruta_csv, index=False, encoding="utf-8")
        print(f"✅ CSV: {ruta_csv}")

        print("🔍 Salvaguarda: comparando con p2_features_etiqueta_por_corte...")
        errores = verificar_contra_features(conn, resumen)
        if errores:
            print("⚠️  Discrepancias encontradas:")
            for e in errores:
                print(f"   - {e}")
        else:
            print("✅ Sin discrepancias.")

        ruta_md = os.path.join(RESULTS_DIR, "p2_fase1_1.2_resumen.md")
        generar_markdown(resumen, ruta_md, errores)
        print(f"✅ MD: {ruta_md}")

        print("\n--- RESULTADO ---")
        print(resumen.to_string(index=False))
        print("-----------------\n")

        if errores:
            print("❌ FALLO: hay discrepancias. Revisa el MD.")
            sys.exit(1)
        print("✅ Exportación completada.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()