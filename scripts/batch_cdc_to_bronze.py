import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

DATOS_RED_DIR = "./scripts/datos_red"
BRONZE_DIR = "./bronze"


def asegurar_directorio(path: str):
    os.makedirs(path, exist_ok=True)


def escribir_parquet_particionado(df: pd.DataFrame, dataset_name: str, date_col: str = None):
    """Agrega metadata obligatoria y persiste en Bronze particionado por fecha."""
    ahora = datetime.utcnow()
    df["ingestion_timestamp"] = ahora.isoformat()
    df["source_file"] = f"{dataset_name}"

    if date_col and date_col in df.columns:
        # Extraer YYYY/MM/DD para partición física
        fechas = pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y/%m/%d")
        df["_part_fecha"] = fechas.fillna(ahora.strftime("%Y/%m/%d"))
    else:
        # Catálogos y CDC se particionan por la fecha de ingesta
        df["_part_fecha"] = ahora.strftime("%Y/%m/%d")

    # Escritura particionada por carpetas físicas
    for particion, sub_df in df.groupby("_part_fecha"):
        ruta_salida = os.path.join(BRONZE_DIR, dataset_name, particion)
        asegurar_directorio(ruta_salida)

        datos_limpios = sub_df.drop(columns=["_part_fecha"])
        tabla = pa.Table.from_pandas(datos_limpios, preserve_index=False)
        nombre_archivo = f"{dataset_name}_{ahora.strftime('%H%M%S')}.parquet"
        pq.write_table(tabla, os.path.join(ruta_salida, nombre_archivo), compression="SNAPPY")

    return len(df)


def procesar_catalogos():
    conteo = {}
    catalogos = ["tm_estaciones.csv", "tu_paradas.csv", "mr_estaciones.csv", "am_estaciones.csv"]
    for cat in catalogos:
        nombre = cat.replace(".csv", "")
        ruta = os.path.join(DATOS_RED_DIR, cat)
        df = pd.read_csv(ruta)
        conteo[cat] = escribir_parquet_particionado(df, nombre)
    return conteo


def procesar_transurbano():
    # Ingesta batch de Transurbano
    ruta = os.path.join(DATOS_RED_DIR, "transurbano_transacciones.csv")
    df = pd.read_csv(ruta)
    # Parsear temporalmente para partición física
    df["_temp_fecha"] = pd.to_datetime(df["fecha"], format="%d/%m/%Y", errors="coerce")
    total = escribir_parquet_particionado(df.drop(columns=["_temp_fecha"]), "transurbano_transacciones",
                                          date_col="_temp_fecha")
    return {"transurbano_transacciones.csv": total}


def procesar_metroriel_jsonl():
    # Preserva el JSON crudo en Bronze (Data Lake)
    ruta = os.path.join(DATOS_RED_DIR, "metroriel_viajes.jsonl")
    registros = []
    with open(ruta, "r", encoding="utf-8") as f:
        for linea in f:
            if linea.strip():
                registros.append(json.loads(linea))

    df = pd.json_normalize(registros)
    # Particionar por fecha de inicio o abordaje si existe
    col_fecha = [c for c in df.columns if "fecha" in c.lower() or "timestamp" in c.lower() or "inicio" in c.lower()]
    target_col = col_fecha[0] if col_fecha else None

    total = escribir_parquet_particionado(df, "metroriel_viajes", date_col=target_col)
    return {"metroriel_viajes.jsonl": total}


def procesar_cdc_padron():
    # CDC se almacena íntegro en Bronze con sus operaciones CRUD crudas
    ruta = os.path.join(DATOS_RED_DIR, "cdc_padron_usuarios.csv")
    df = pd.read_csv(ruta)
    total = escribir_parquet_particionado(df, "cdc_padron_usuarios")
    return {"cdc_padron_usuarios.csv": total}


if __name__ == "__main__":
    conteos_totales = {}
    conteos_totales.update(procesar_catalogos())
    conteos_totales.update(procesar_transurbano())
    conteos_totales.update(procesar_metroriel_jsonl())
    conteos_totales.update(procesar_cdc_padron())

    # Asegurar directorio de resultados
    os.makedirs("results", exist_ok=True)

    # 1. Preparar DataFrame con los conteos
    filas = [{"archivo_fuente": arch, "registros_ingeridos": cant} for arch, cant in conteos_totales.items()]
    df_batch = pd.DataFrame(filas)
    gran_total = df_batch["registros_ingeridos"].sum() if not df_batch.empty else 0

    # 2. Guardar métricas tabulares (.csv)
    ruta_csv = f"results/fase1_1.1_ingesta_batch_cdc_conteos.csv"
    df_batch.to_csv(ruta_csv, index=False, encoding="utf-8")
    print(f"Métricas batch/CDC guardadas en: {ruta_csv}")

    # 3. Guardar reporte documental (.md)
    ruta_md = f"results/fase1_1.1_ingesta_batch_cdc_resumen.md"
    with open(ruta_md, "w", encoding="utf-8") as f:
        f.write("# Reporte de Ingesta Batch y CDC a Bronze (Subfase 1.1)\n\n")
        f.write(f"- **Destino:** Capa Bronze (Archivos Parquet comprimidos con SNAPPY)\n")
        f.write(f"- **Total Registros Procesados:** {gran_total}\n\n")
        f.write("## Detalle de Registros por Archivo Fuente\n\n")
        f.write(df_batch.to_markdown(index=False))
        f.write("\n\n---\n")
        f.write("### Fuentes Ingeridas:\n")
        f.write("- **Catálogos:** Estaciones de Transmetro, paradas de Transurbano, estaciones de MetroRiel y Aerómetro.\n")
        f.write("- **Transacciones:** Transurbano (CSV) y MetroRiel (JSONL).\n")
        f.write("- **Log CDC:** Padrón histórico de usuarios de Transmetro.\n")

    print(f"Reporte documental guardado en: {ruta_md}")