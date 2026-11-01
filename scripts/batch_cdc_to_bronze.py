import os
import sys
import json
import gc
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone

DATOS_RED_DIR = "./scripts/datos_red"
BRONZE_DIR = "./bronze"

CHUNK_CSV = 100_000
CHUNK_JSONL = 25_000
IS_TTY = sys.stdout.isatty()


def _log(msg: str):
    """Print con flush para que Prefect lo vea en vivo."""
    print(msg, flush=True)


def _progreso(nombre: str, procesadas: int, total_estimado: int = None):
    if IS_TTY and total_estimado:
        pct = 100.0 * procesadas / total_estimado
        sys.stdout.write(f"\r  [{nombre}] {procesadas:,} filas ({pct:5.1f}%)")
        sys.stdout.flush()
    else:
        _log(f"  [{nombre}] {procesadas:,} filas procesadas...")


def _contar_lineas(ruta: str) -> int:
    """Cuenta líneas rápido (sin cargar en RAM) para estimar progreso."""
    with open(ruta, "rb") as f:
        return sum(1 for _ in f)


def asegurar_directorio(path: str):
    os.makedirs(path, exist_ok=True)


# ---------------------------------------------------------------------------
# Escritura incremental
# ---------------------------------------------------------------------------
def escribir_parquet_incremental(
    df: pd.DataFrame,
    dataset_name: str,
    date_col: str = None,
    chunk_id: int = None,
) -> int:
    ahora = datetime.now(timezone.utc)
    df = df.copy()
    df["ingestion_timestamp"] = ahora.isoformat()
    df["source_file"] = dataset_name

    if date_col and date_col in df.columns:
        fechas = pd.to_datetime(df[date_col], errors="coerce")
        df["_part_fecha"] = fechas.dt.strftime("%Y/%m/%d").fillna(ahora.strftime("%Y/%m/%d"))
    else:
        df["_part_fecha"] = ahora.strftime("%Y/%m/%d")

    df = df.drop(columns=["_temp_fecha"], errors="ignore")

    for particion, sub_df in df.groupby("_part_fecha"):
        ruta_salida = os.path.join(BRONZE_DIR, dataset_name, particion)
        asegurar_directorio(ruta_salida)
        datos_limpios = sub_df.drop(columns=["_part_fecha"], errors="ignore")
        tabla = pa.Table.from_pandas(datos_limpios, preserve_index=False)

        # NUEVO: nombre unico por chunk para no sobrescribir entre chunks
        if chunk_id is not None:
            nombre_archivo = f"{dataset_name}_chunk_{chunk_id:05d}.parquet"
        else:
            nombre_archivo = f"{dataset_name}.parquet"

        pq.write_table(
            tabla,
            os.path.join(ruta_salida, nombre_archivo),
            compression="SNAPPY",
        )
        del tabla, datos_limpios, sub_df

    n = len(df)
    del df
    return n

# ---------------------------------------------------------------------------
# CSV genérico chunked con feedback
# ---------------------------------------------------------------------------
def procesar_csv_chunked(
    nombre_archivo: str,
    dataset_name: str,
    date_col: str = None,
    parse_dates: dict = None,
    chunksize: int = CHUNK_CSV,
    usecols: list = None,
    con_progreso: bool = True,
) -> int:
    ruta = os.path.join(DATOS_RED_DIR, nombre_archivo)
    t0 = time.time()
    _log(f"[{dataset_name}] leyendo {nombre_archivo} en chunks de {chunksize:,} ...")

    total_lineas = None
    if con_progreso:
        try:
            total_lineas = _contar_lineas(ruta) - 1  # -1 cabecera
            _log(f"  [{dataset_name}] ~{total_lineas:,} filas estimadas")
        except Exception:
            total_lineas = None

    total = 0
    chunks_procesados = 0
    lector = pd.read_csv(
        ruta,
        chunksize=chunksize,
        dtype=str,
        low_memory=False,
        usecols=usecols,
    )
    for i, chunk in enumerate(lector):
        if parse_dates:
            for col, fmt in parse_dates.items():
                if col in chunk.columns:
                    chunk[col] = pd.to_datetime(chunk[col], format=fmt, errors="coerce")
        total += escribir_parquet_incremental(chunk, dataset_name, date_col=date_col, chunk_id=i)
        chunks_procesados += 1
        del chunk
        gc.collect()
        if con_progreso:
            _progreso(dataset_name, total, total_lineas)

    if IS_TTY and con_progreso:
        sys.stdout.write("\n")
    dt = time.time() - t0
    _log(f"OK [{dataset_name}] {total:,} filas en {dt:.1f}s ({chunks_procesados} chunks)")
    return total


# ---------------------------------------------------------------------------
# Catálogos
# ---------------------------------------------------------------------------
def procesar_catalogos():
    conteo = {}
    catalogos = ["tm_estaciones.csv", "tu_paradas.csv", "mr_estaciones.csv", "am_estaciones.csv"]
    _log(f"[catálogos] {len(catalogos)} archivos pequeños")
    for cat in catalogos:
        nombre = cat.replace(".csv", "")
        ruta = os.path.join(DATOS_RED_DIR, cat)
        df = pd.read_csv(ruta, dtype=str)
        conteo[cat] = escribir_parquet_incremental(df, nombre, chunk_id=0)
        _log(f" OK {cat}: {len(df):,} filas")
        del df
        gc.collect()
    return conteo


# ---------------------------------------------------------------------------
# Transurbano
# ---------------------------------------------------------------------------
def procesar_transurbano():
    ruta = os.path.join(DATOS_RED_DIR, "transurbano_transacciones.csv")
    _log("[transurbano_transacciones] particionando por 'fecha' (dd/mm/yyyy)")
    t0 = time.time()
    total_lineas = _contar_lineas(ruta) - 1
    _log(f"  ~{total_lineas:,} filas estimadas")

    total = 0
    lector = pd.read_csv(ruta, chunksize=CHUNK_CSV, dtype=str, low_memory=False)
    for i, chunk in enumerate(lector):
        chunk["_temp_fecha"] = pd.to_datetime(chunk["fecha"], format="%d/%m/%Y", errors="coerce")
        total += escribir_parquet_incremental(
            chunk, "transurbano_transacciones", date_col="_temp_fecha", chunk_id=i
        )
        del chunk
        gc.collect()
        _progreso("transurbano_transacciones", total, total_lineas)

    if IS_TTY:
        sys.stdout.write("\n")
    _log(f"[transurbano_transacciones] {total:,} filas en {time.time()-t0:.1f}s")
    return {"transurbano_transacciones.csv": total}


# ---------------------------------------------------------------------------
# MetroRiel JSONL
# ---------------------------------------------------------------------------
def _volcar_chunk_metroriel(registros: list, chunk_id: int) -> int:
    df = pd.json_normalize(registros, max_level=1)
    if "entry.ts" in df.columns:
        target = "entry.ts"
    elif "exit.ts" in df.columns:
        target = "exit.ts"
    else:
        target = None
    n = escribir_parquet_incremental(
        df, "metroriel_viajes", date_col=target, chunk_id=chunk_id
    )
    del df
    return n


def procesar_metroriel_jsonl():
    ruta = os.path.join(DATOS_RED_DIR, "metroriel_viajes.jsonl")
    _log(f">> [metroriel_viajes] leyendo JSONL en chunks de {CHUNK_JSONL:,} ...")
    t0 = time.time()
    total_lineas = _contar_lineas(ruta)
    _log(f"  ~{total_lineas:,} lineas estimadas")

    total = 0
    buffer = []
    chunk_id = 0
    with open(ruta, "r", encoding="utf-8") as f:
        for linea in f:
            linea = linea.strip()
            if not linea:
                continue
            buffer.append(json.loads(linea))
            if len(buffer) >= CHUNK_JSONL:
                total += _volcar_chunk_metroriel(buffer, chunk_id)
                chunk_id += 1
                buffer.clear()
                gc.collect()
                _progreso("metroriel_viajes", total, total_lineas)
    if buffer:
        total += _volcar_chunk_metroriel(buffer, chunk_id)
        buffer.clear()

    if IS_TTY:
        sys.stdout.write("\n")
    _log(f"[OK] [metroriel_viajes] {total:,} filas en {time.time()-t0:.1f}s")
    return {"metroriel_viajes.jsonl": total}

# ---------------------------------------------------------------------------
# CDC padrón
# ---------------------------------------------------------------------------
def procesar_cdc_padron():
    total = procesar_csv_chunked(
        "cdc_padron_usuarios.csv",
        "cdc_padron_usuarios",
        chunksize=50_000,
    )
    return {"cdc_padron_usuarios.csv": total}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    t_inicio = time.time()
    _log("=" * 60)
    _log(" INGESTA BATCH + CDC -> BRONZE (Subfase 1.1)")
    _log("=" * 60)

    conteos_totales = {}
    conteos_totales.update(procesar_catalogos())
    conteos_totales.update(procesar_transurbano())
    conteos_totales.update(procesar_metroriel_jsonl())
    conteos_totales.update(procesar_cdc_padron())

    os.makedirs("results", exist_ok=True)
    filas = [{"archivo_fuente": a, "registros_ingeridos": n} for a, n in conteos_totales.items()]
    df_batch = pd.DataFrame(filas)
    gran_total = int(df_batch["registros_ingeridos"].sum()) if not df_batch.empty else 0

    ruta_csv = "results/fase1_1.1_ingesta_batch_cdc_conteos.csv"
    df_batch.to_csv(ruta_csv, index=False, encoding="utf-8")

    ruta_md = "results/fase1_1.1_ingesta_batch_cdc_resumen.md"
    with open(ruta_md, "w", encoding="utf-8") as f:
        f.write("# Reporte de Ingesta Batch y CDC a Bronze (Subfase 1.1)\n\n")
        f.write(f"- **Destino:** Capa Bronze (Parquet SNAPPY, particionado por fecha)\n")
        f.write(f"- **Total Registros Procesados:** {gran_total:,}\n\n")
        f.write("## Detalle por Archivo Fuente\n\n")
        f.write(df_batch.to_markdown(index=False))
        f.write("\n")

    _log("=" * 60)
    _log(f"OK BATCH/CDC COMPLETADO: {gran_total:,} registros en {time.time()-t_inicio:.1f}s")
    _log("=" * 60)