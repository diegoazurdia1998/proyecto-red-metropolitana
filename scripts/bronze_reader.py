import os
import glob
import pandas as pd

BRONZE_DIR = os.getenv("BRONZE_DIR", "./bronze")

def leer_parquet_bronze(dataset_name: str) -> pd.DataFrame:
    """
    Lee recursivamente todos los archivos .parquet dentro de bronze/<dataset_name>/**/
    y remueve las columnas técnicas de auditoría de Bronze si existen.
    """
    patron = os.path.join(BRONZE_DIR, dataset_name, "**", "*.parquet")
    archivos = glob.glob(patron, recursive=True)
    if not archivos:
        patron_plano = os.path.join(BRONZE_DIR, dataset_name, "*.parquet")
        archivos = glob.glob(patron_plano)
    if not archivos:
        raise FileNotFoundError(f"No se encontraron archivos Parquet en {BRONZE_DIR}/{dataset_name}")

    dfs = [pd.read_parquet(f) for f in sorted(archivos)]
    df = pd.concat(dfs, ignore_index=True)

    cols_auditoria = [c for c in ["ingestion_timestamp", "source_file"] if c in df.columns]
    if cols_auditoria:
        df = df.drop(columns=cols_auditoria)

    return df