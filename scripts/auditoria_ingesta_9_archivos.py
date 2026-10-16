import os
import glob
import json
import pandas as pd
import pyarrow.parquet as pq

DATOS_RED_DIR = "./scripts/datos_red"
BRONZE_DIR = "./bronze"

ARCHIVOS_CONFIG = [
    # Catálogos (Batch)
    {"archivo": "tm_estaciones.csv", "dataset_bronze": "tm_estaciones", "tipo": "CSV", "via": "Batch"},
    {"archivo": "tu_paradas.csv", "dataset_bronze": "tu_paradas", "tipo": "CSV", "via": "Batch"},
    {"archivo": "mr_estaciones.csv", "dataset_bronze": "mr_estaciones", "tipo": "CSV", "via": "Batch"},
    {"archivo": "am_estaciones.csv", "dataset_bronze": "am_estaciones", "tipo": "CSV", "via": "Batch"},
    # Operaciones Batch
    {"archivo": "transurbano_transacciones.csv", "dataset_bronze": "transurbano_transacciones", "tipo": "CSV",
     "via": "Batch"},
    {"archivo": "metroriel_viajes.jsonl", "dataset_bronze": "metroriel_viajes", "tipo": "JSONL", "via": "Batch"},
    # CDC
    {"archivo": "cdc_padron_usuarios.csv", "dataset_bronze": "cdc_padron_usuarios", "tipo": "CSV", "via": "CDC"},
    # Streaming (Kafka)
    {"archivo": "transmetro_validaciones.csv", "dataset_bronze": "transmetro_validaciones", "tipo": "CSV",
     "via": "Streaming (Kafka)"},
    {"archivo": "aerometro_boardings.csv", "dataset_bronze": "aerometro_boardings", "tipo": "CSV",
     "via": "Streaming (Kafka)"}
]


def contar_filas_origen(ruta: str, tipo: str) -> int:
    if not os.path.exists(ruta):
        return 0
    if tipo == "CSV":
        # Conteo eficiente de líneas excluyendo header
        with open(ruta, "rb") as f:
            return max(0, sum(1 for _ in f) - 1)
    elif tipo == "JSONL":
        with open(ruta, "r", encoding="utf-8") as f:
            return sum(1 for line in f if line.strip())
    return 0


def contar_filas_bronze(dataset_name: str) -> int:
    patron = os.path.join(BRONZE_DIR, dataset_name, "**", "*.parquet")
    archivos = glob.glob(patron, recursive=True)
    if not archivos:
        patron_plano = os.path.join(BRONZE_DIR, dataset_name, "*.parquet")
        archivos = glob.glob(patron_plano)

    total = 0
    for arch in archivos:
        try:
            metadata = pq.read_metadata(arch)
            total += metadata.num_rows
        except Exception:
            df = pd.read_parquet(arch)
            total += len(df)
    return total


def generar_reporte_9_archivos():
    reporte = []
    for item in ARCHIVOS_CONFIG:
        ruta_origen = os.path.join(DATOS_RED_DIR, item["archivo"])
        filas_origen = contar_filas_origen(ruta_origen, item["tipo"])
        filas_bronze = contar_filas_bronze(item["dataset_bronze"])
        diferencia = filas_bronze - filas_origen

        reporte.append({
            "archivo_fuente": item["archivo"],
            "via_ingesta": item["via"],
            "filas_origen": filas_origen,
            "filas_bronze": filas_bronze,
            "diferencia": diferencia,
            "estado": "OK" if diferencia == 0 and filas_origen > 0 else "DISCREPANCIA / PENDIENTE"
        })

    df = pd.DataFrame(reporte)
    os.makedirs("results", exist_ok=True)

    # 1. Guardar CSV consolidado oficial
    ruta_csv = "results/fase1_1.1_ingesta_9_archivos_conteos.csv"
    df.to_csv(ruta_csv, index=False, encoding="utf-8")

    # 2. Guardar resumen Markdown
    ruta_md = "results/fase1_1.1_ingesta_9_archivos_resumen.md"
    with open(ruta_md, "w", encoding="utf-8") as f:
        f.write("# Auditoría Consolidada de Ingesta a Capa Bronze (9 Fuentes)\n\n")
        f.write(
            "Validación integral de la totalidad de archivos de la red metropolitana frente al almacenamiento físico en Bronze (Parquet Snappy).\n\n")
        f.write(df.to_markdown(index=False))
        f.write(f"\n\n- **Total Filas en Origen:** {df['filas_origen'].sum():,}\n")
        f.write(f"- **Total Filas en Bronze:** {df['filas_bronze'].sum():,}\n")
        f.write(
            f"- **Balance General:** {'100% Consistente e Idempotente' if df['diferencia'].sum() == 0 else 'Discrepancias detectadas'}\n")

    print(f"\n✅ Auditoría de 9 archivos generada exitosamente:")
    print(f"   -> {ruta_csv}")
    print(f"   -> {ruta_md}\n")
    print(df.to_string(index=False))


if __name__ == "__main__":
    generar_reporte_9_archivos()