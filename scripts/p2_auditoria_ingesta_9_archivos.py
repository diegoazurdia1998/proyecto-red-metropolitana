"""
Auditoría 1.1 del P2: conteos esperadas vs cargadas por archivo + tabla de tiempos.

Salidas:
    results/p2_fase1_1.1_conteos.csv
    results/p2_fase1_1.1_tiempos.csv
    results/p2_fase1_1.1_resumen.md

No modifica el script del P1 (auditoria_ingesta_9_archivos.py).
"""
import os
import glob
import sys
import pandas as pd
import pyarrow.parquet as pq

# Permitir import del helper desde scripts/
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from p2_medir_tiempos import leer_tiempos  # noqa: E402

DATOS_RED_DIR = "./scripts/datos_red"
BRONZE_DIR = "./bronze"
RESULTS_DIR = "./results"

ARCHIVOS_CONFIG = [
    {"archivo": "tm_estaciones.csv",             "dataset_bronze": "tm_estaciones",             "tipo": "CSV",   "via": "Batch"},
    {"archivo": "tu_paradas.csv",                "dataset_bronze": "tu_paradas",                "tipo": "CSV",   "via": "Batch"},
    {"archivo": "mr_estaciones.csv",             "dataset_bronze": "mr_estaciones",             "tipo": "CSV",   "via": "Batch"},
    {"archivo": "am_estaciones.csv",             "dataset_bronze": "am_estaciones",             "tipo": "CSV",   "via": "Batch"},
    {"archivo": "transurbano_transacciones.csv", "dataset_bronze": "transurbano_transacciones", "tipo": "CSV",   "via": "Batch"},
    {"archivo": "metroriel_viajes.jsonl",        "dataset_bronze": "metroriel_viajes",          "tipo": "JSONL", "via": "Batch"},
    {"archivo": "cdc_padron_usuarios.csv",       "dataset_bronze": "cdc_padron_usuarios",       "tipo": "CSV",   "via": "CDC"},
    {"archivo": "transmetro_validaciones.csv",   "dataset_bronze": "transmetro_validaciones",   "tipo": "CSV",   "via": "Streaming (Kafka)"},
    {"archivo": "aerometro_boardings.csv",       "dataset_bronze": "aerometro_boardings",       "tipo": "CSV",   "via": "Streaming (Kafka)"},
]


def contar_filas_origen(ruta: str, tipo: str) -> int:
    if not os.path.exists(ruta):
        return 0
    if tipo == "CSV":
        with open(ruta, "rb") as f:
            return max(0, sum(1 for _ in f) - 1)
    if tipo == "JSONL":
        with open(ruta, "r", encoding="utf-8") as f:
            return sum(1 for line in f if line.strip())
    return 0


def contar_filas_bronze(dataset_name: str) -> int:
    patron = os.path.join(BRONZE_DIR, dataset_name, "**", "*.parquet")
    archivos = glob.glob(patron, recursive=True)
    if not archivos:
        archivos = glob.glob(os.path.join(BRONZE_DIR, dataset_name, "*.parquet"))
    total = 0
    for arch in archivos:
        try:
            total += pq.read_metadata(arch).num_rows
        except Exception:
            total += len(pd.read_parquet(arch))
    return total


def _explicar(item: dict, filas_origen: int, filas_bronze: int, diferencia: int) -> str:
    """Deriva una explicación legible para toda diferencia != 0."""
    if filas_origen == 0 and filas_bronze == 0:
        return "archivo_faltante_en_datos_red"
    if filas_origen == 0 and filas_bronze > 0:
        return "bronze_con_datos_pero_origen_vacio (residuo_run_anterior?)"
    if filas_origen > 0 and filas_bronze == 0:
        if "Streaming" in item["via"]:
            return "streaming_no_leido (offsets Kafka no reseteados)"
        return "bronze_vacio (ingesta Batch/CDC no ejecutada o fallida)"
    if diferencia < 0:
        return f"faltan_{-diferencia}_filas_en_bronze (ingesta parcial)"
    if diferencia > 0:
        return f"sobran_{diferencia}_filas_en_bronze (duplicado entre corridas)"
    return "ok"


def generar_reporte():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    reporte = []
    for item in ARCHIVOS_CONFIG:
        ruta_origen = os.path.join(DATOS_RED_DIR, item["archivo"])
        filas_origen = contar_filas_origen(ruta_origen, item["tipo"])
        filas_bronze = contar_filas_bronze(item["dataset_bronze"])
        diferencia = filas_bronze - filas_origen
        explicacion = _explicar(item, filas_origen, filas_bronze, diferencia)

        reporte.append({
            "archivo_fuente": item["archivo"],
            "dataset_bronze": item["dataset_bronze"],
            "via_ingesta": item["via"],
            "filas_origen": filas_origen,
            "filas_bronze": filas_bronze,
            "diferencia": diferencia,
            "explicacion": explicacion,
            "estado": "OK" if diferencia == 0 and filas_origen > 0 else "REVISAR",
        })

    df_conteos = pd.DataFrame(reporte)
    ruta_conteos = os.path.join(RESULTS_DIR, "p2_fase1_1.1_conteos.csv")
    df_conteos.to_csv(ruta_conteos, index=False, encoding="utf-8")

    df_tiempos = leer_tiempos()
    ruta_tiempos = os.path.join(RESULTS_DIR, "p2_fase1_1.1_tiempos.csv")
    df_tiempos.to_csv(ruta_tiempos, index=False, encoding="utf-8")

    ruta_md = os.path.join(RESULTS_DIR, "p2_fase1_1.1_resumen.md")
    with open(ruta_md, "w", encoding="utf-8") as f:
        f.write("# Fase 1.1 — Auditoría de ingesta (9 fuentes) y tiempos por etapa\n\n")
        f.write("## Tabla 1: Conteos esperadas vs cargadas\n\n")
        f.write(df_conteos.to_markdown(index=False))
        f.write("\n\n")
        f.write(f"- **Total filas origen:** {df_conteos['filas_origen'].sum():,}\n")
        f.write(f"- **Total filas Bronze:** {df_conteos['filas_bronze'].sum():,}\n")
        balance = df_conteos["diferencia"].sum()
        f.write(f"- **Balance general:** {balance:+d} filas\n")
        f.write(f"- **Fuentes con diferencia != 0:** {(df_conteos['diferencia'] != 0).sum()} / 9\n\n")
        f.write("## Tabla 2: Tiempos por etapa\n\n")
        if df_tiempos.empty:
            f.write("_No hay tiempos registrados (ejecutar vía Prefect flow)._\n")
        else:
            f.write(df_tiempos.to_markdown(index=False))
            f.write(f"\n\n- **Tiempo total del pipeline:** {df_tiempos['duration_s'].sum():.2f} s\n")

    print(f"Auditoría 1.1 generada:")
    print(f"   -> {ruta_conteos}")
    print(f"   -> {ruta_tiempos}")
    print(f"   -> {ruta_md}\n")
    print(df_conteos.to_string(index=False))


if __name__ == "__main__":
    generar_reporte()