"""
CLI de predicción (rubric 2.4) — versión final, robusta contra datos corruptos.

Uso:
    python -m scripts.predecir --fecha-corte 2026-08-30 --datos ./scripts/P2_datos_red_defensa_120dias

Qué hace:
  1. Carga el modelo HGB (modelos_entrenados/p2_hgb_v1.0.0.pkl).
  2. Carga los 9 CSV/JSONL crudos respetando sus esquemas reales.
  3. Filtra ESTRICTAMENTE a timestamp < fecha_corte y dentro de [2026-06-01, 2026-10-01].
  4. Descarta filas con fechas imposibles (años fuera de rango, nulas, etc.).
  5. Calcula las 10 features en pandas (misma lógica que p2_features_por_corte.sql).
  6. Aplica población (>=2 abordajes en 28d + sin DELETE CDC antes del corte).
  7. Predice con HGB.
  8. Aplica umbral 0.3333 → contactar ∈ {0, 1}.
  9. Resuelve la lista de tarjetas por usuario.
  10. Escribe results/p2_predicciones_<fecha>.csv con: unidad_id, tarjetas, puntaje, contactar.

Salida determinista: misma entrada + mismo modelo → mismo CSV bit a bit.
"""
import argparse
import json
import os
import re
import sys
from collections import defaultdict

import joblib
import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

RESULTS_DIR = "./results"
MODELOS_DIR = "./modelos_entrenados"
MODELO_HGB = os.path.join(MODELOS_DIR, "p2_hgb_v1.0.0.pkl")
UMBRAL = 0.3333

FECHA_MIN = pd.Timestamp("2026-06-01 00:00:00")
FECHA_MAX = pd.Timestamp("2026-10-01 00:00:00")

FEATURE_COLS = [
    "n_abordajes_28d", "n_dias_activos_28d", "n_modos_usados_28d",
    "pct_fin_semana_28d", "n_abordajes_14d", "n_abordajes_7d",
    "recencia_dias_ultimo_abordaje", "n_estaciones_distintas_28d",
    "pct_hora_pico_28d", "dias_desde_alta",
]


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------

def normalizar_id(valor) -> str | None:
    """Tarjetas: solo dígitos, bigint, string. Hash: tal cual."""
    if pd.isna(valor):
        return None
    s = re.sub(r"\D", "", str(valor))
    if s == "":
        return None
    return str(int(s))


def parse_fecha(serie, formato=None) -> pd.Series:
    """Parsea fechas con formato opcional. Devuelve tz-naive."""
    s = pd.to_datetime(serie, errors="coerce", format=formato)
    # Si quedó con tz, quitarlo
    if hasattr(s.dt, "tz") and s.dt.tz is not None:
        s = s.dt.tz_localize(None)
    return s


def filtrar_fechas(df: pd.DataFrame, col_ts: str, fecha_corte: pd.Timestamp, nombre: str) -> pd.DataFrame:
    """Filtra filas a [FECHA_MIN, fecha_corte). Reporta cuántas se descartaron y por qué."""
    n0 = len(df)
    ts = pd.to_datetime(df[col_ts], errors="coerce")
    if hasattr(ts.dt, "tz") and ts.dt.tz is not None:
        ts = ts.dt.tz_localize(None)

    n_nat = ts.isna().sum()
    fuera_rango = ((ts < FECHA_MIN) | (ts >= fecha_corte)).sum()
    mask = ts.notna() & (ts >= FECHA_MIN) & (ts < fecha_corte)
    df = df[mask].copy()
    df["ts"] = ts[mask].values

    n_desc = n0 - len(df)
    if n_desc > 0:
        print(f"   [{nombre}] Descartadas {n_desc:,}/{n0:,} "
              f"(NaT: {n_nat:,}, fuera de rango: {fuera_rango:,})")
    return df


# ---------------------------------------------------------------------------
# Cargadores por fuente
# ---------------------------------------------------------------------------

def cargar_transmetro(carpeta: str, fecha_corte: pd.Timestamp) -> pd.DataFrame:
    ruta = os.path.join(carpeta, "transmetro_validaciones.csv")
    if not os.path.exists(ruta):
        return pd.DataFrame(columns=["sk_usuario", "ts", "modo", "estacion", "id_crudo"])
    df = pd.read_csv(ruta, usecols=["tarjeta", "estacion_id", "fecha_hora"])
    df["fecha_hora"] = pd.to_datetime(df["fecha_hora"], errors="coerce", format="%Y-%m-%d %H:%M:%S")
    df = filtrar_fechas(df, "fecha_hora", fecha_corte, "transmetro")
    df["sk_usuario"] = df["tarjeta"].apply(normalizar_id)
    df = df[df["sk_usuario"].notna()].copy()
    df["modo"] = "transmetro"
    df["estacion"] = df["estacion_id"].astype(str)
    df["id_crudo"] = df["tarjeta"].astype(str)
    return df[["sk_usuario", "ts", "modo", "estacion", "id_crudo"]]


def cargar_transurbano(carpeta: str, fecha_corte: pd.Timestamp) -> pd.DataFrame:
    ruta = os.path.join(carpeta, "transurbano_transacciones.csv")
    if not os.path.exists(ruta):
        return pd.DataFrame(columns=["sk_usuario", "ts", "modo", "estacion", "id_crudo"])
    df = pd.read_csv(ruta, usecols=["fecha", "hora", "num_tarjeta", "cod_parada"])
    # fecha viene como DD/MM/YYYY
    df["ts_str"] = df["fecha"].astype(str) + " " + df["hora"].astype(str)
    df["ts_dt"] = pd.to_datetime(df["ts_str"], errors="coerce", dayfirst=True, format="%d/%m/%Y %H:%M:%S")
    df = filtrar_fechas(df, "ts_dt", fecha_corte, "transurbano")
    df["sk_usuario"] = df["num_tarjeta"].apply(normalizar_id)
    df = df[df["sk_usuario"].notna()].copy()
    df["modo"] = "transurbano"
    df["estacion"] = df["cod_parada"].astype(str)
    df["id_crudo"] = df["num_tarjeta"].astype(str)
    return df[["sk_usuario", "ts", "modo", "estacion", "id_crudo"]]


def cargar_metroriel(carpeta: str, fecha_corte: pd.Timestamp) -> pd.DataFrame:
    ruta = os.path.join(carpeta, "metroriel_viajes.jsonl")
    if not os.path.exists(ruta):
        return pd.DataFrame(columns=["sk_usuario", "ts", "modo", "estacion", "id_crudo"])
    rows = []
    with open(ruta, "r", encoding="utf-8") as f:
        for linea in f:
            linea = linea.strip()
            if not linea:
                continue
            try:
                obj = json.loads(linea)
            except json.JSONDecodeError:
                continue
            card = obj.get("card")
            entry = obj.get("entry") or {}
            ts = entry.get("ts")
            st = entry.get("station")
            if card and ts:
                rows.append({"card": card, "ts_str": ts, "estacion": str(st) if st is not None else ""})
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["sk_usuario", "ts", "modo", "estacion", "id_crudo"])
    df["ts_dt"] = pd.to_datetime(df["ts_str"], errors="coerce")
    df = filtrar_fechas(df, "ts_dt", fecha_corte, "metroriel")
    df["sk_usuario"] = df["card"].apply(normalizar_id)
    df = df[df["sk_usuario"].notna()].copy()
    df["modo"] = "metroriel"
    df["id_crudo"] = df["card"].astype(str)
    return df[["sk_usuario", "ts", "modo", "estacion", "id_crudo"]]


def cargar_aerometro(carpeta: str, fecha_corte: pd.Timestamp) -> pd.DataFrame:
    ruta = os.path.join(carpeta, "aerometro_boardings.csv")
    if not os.path.exists(ruta):
        return pd.DataFrame(columns=["sk_usuario", "ts", "modo", "estacion", "id_crudo"])
    df = pd.read_csv(ruta, usecols=["user_hash", "station_code", "timestamp_utc"])
    # timestamp_utc viene como ISO con Z: 2026-06-01T13:37:52Z
    ts = pd.to_datetime(df["timestamp_utc"], errors="coerce", utc=True)
    ts = ts.dt.tz_localize(None)
    df["ts_dt"] = ts
    df = filtrar_fechas(df, "ts_dt", fecha_corte, "aerometro")
    df["sk_usuario"] = df["user_hash"].astype(str)
    df = df[df["sk_usuario"].notna() & (df["sk_usuario"] != "")].copy()
    df["modo"] = "aerometro"
    df["estacion"] = df["station_code"].astype(str)
    df["id_crudo"] = df["user_hash"].astype(str)
    return df[["sk_usuario", "ts", "modo", "estacion", "id_crudo"]]


def cargar_cdc_activos(carpeta: str, fecha_corte: pd.Timestamp) -> set:
    """sk_usuario con es_activa=TRUE a la fecha de corte (aplica último evento por seq)."""
    ruta = os.path.join(carpeta, "cdc_padron_usuarios.csv")
    if not os.path.exists(ruta):
        return set()
    df = pd.read_csv(ruta, usecols=["seq", "commit_ts", "op", "tarjeta"])
    df["commit_ts"] = pd.to_datetime(df["commit_ts"], errors="coerce")
    df = df[df["commit_ts"].notna() & (df["commit_ts"] < fecha_corte)]
    df = df.sort_values("seq").reset_index(drop=True)
    df["sk_usuario"] = df["tarjeta"].apply(normalizar_id)
    df = df[df["sk_usuario"].notna()]
    estado = {}
    for _, row in df.iterrows():
        uid = row["sk_usuario"]
        op = str(row["op"]).strip().upper()
        if op in ("INSERT", "UPDATE"):
            estado[uid] = True
        elif op == "DELETE":
            estado[uid] = False
    return {u for u, a in estado.items() if a}


# ---------------------------------------------------------------------------
# Cálculo de features (misma lógica que p2_features_por_corte.sql)
# ---------------------------------------------------------------------------

def calcular_features(transmetro, transurbano, metroriel, aerometro, fecha_corte):
    uni = pd.concat([transmetro, transurbano, metroriel, aerometro], ignore_index=True)
    uni = uni.dropna(subset=["sk_usuario", "ts"])
    uni = uni[(uni["ts"] >= FECHA_MIN) & (uni["ts"] < fecha_corte)]

    if uni.empty:
        return pd.DataFrame(columns=["sk_usuario"] + FEATURE_COLS)

    inicio_28d = fecha_corte - pd.Timedelta(days=28)
    inicio_14d = fecha_corte - pd.Timedelta(days=14)
    inicio_7d = fecha_corte - pd.Timedelta(days=7)

    # primer abordaje histórico (para dias_desde_alta)
    hist = uni.groupby("sk_usuario")["ts"].min().rename("primer_abordaje_historico")

    ventana = uni[uni["ts"] >= inicio_28d].copy()
    if ventana.empty:
        return pd.DataFrame(columns=["sk_usuario"] + FEATURE_COLS)

    ventana["es_fin_semana"] = ventana["ts"].dt.dayofweek >= 5
    ventana["es_hora_pico"] = (
        ((ventana["ts"].dt.hour >= 6) & (ventana["ts"].dt.hour <= 8))
        | ((ventana["ts"].dt.hour >= 17) & (ventana["ts"].dt.hour <= 19))
    )

    g = ventana.groupby("sk_usuario")

    # Calcular n_abordajes_14d y _7d de forma vectorizada
    n14 = ventana[ventana["ts"] >= inicio_14d].groupby("sk_usuario").size().rename("n_abordajes_14d")
    n7 = ventana[ventana["ts"] >= inicio_7d].groupby("sk_usuario").size().rename("n_abordajes_7d")

    feats = pd.DataFrame({
        "n_abordajes_28d": g.size(),
        "n_dias_activos_28d": g["ts"].apply(lambda s: s.dt.date.nunique()),
        "n_modos_usados_28d": g["modo"].nunique(),
        "pct_fin_semana_28d": g["es_fin_semana"].mean(),
        "ultimo_ts": g["ts"].max(),
        "n_estaciones_distintas_28d": g["estacion"].nunique(),
        "pct_hora_pico_28d": g["es_hora_pico"].mean(),
    }).reset_index()

    feats = feats.merge(n14, on="sk_usuario", how="left")
    feats = feats.merge(n7, on="sk_usuario", how="left")
    feats["n_abordajes_14d"] = feats["n_abordajes_14d"].fillna(0).astype(int)
    feats["n_abordajes_7d"] = feats["n_abordajes_7d"].fillna(0).astype(int)

    feats["recencia_dias_ultimo_abordaje"] = (fecha_corte - feats["ultimo_ts"]).dt.total_seconds() / 86400.0
    feats = feats.merge(hist, on="sk_usuario", how="left")
    feats["dias_desde_alta"] = (fecha_corte - feats["primer_abordaje_historico"]).dt.total_seconds() / 86400.0
    feats = feats.drop(columns=["ultimo_ts", "primer_abordaje_historico"])

    for c in FEATURE_COLS:
        if c not in feats.columns:
            feats[c] = 0.0
    return feats[["sk_usuario"] + FEATURE_COLS]


# ---------------------------------------------------------------------------
# Modo --datos
# ---------------------------------------------------------------------------

def predecir_con_datos(carpeta: str, fecha_corte: pd.Timestamp, model):
    print(f"▶️  Cargando datos crudos de {carpeta}...")
    transmetro = cargar_transmetro(carpeta, fecha_corte)
    transurbano = cargar_transurbano(carpeta, fecha_corte)
    metroriel = cargar_metroriel(carpeta, fecha_corte)
    aerometro = cargar_aerometro(carpeta, fecha_corte)
    for n, df in [("transmetro", transmetro), ("transurbano", transurbano),
                  ("metroriel", metroriel), ("aerometro", aerometro)]:
        print(f"   {n}: {len(df):,} filas válidas (ts < {fecha_corte})")

    print("▶️  Calculando features...")
    feats = calcular_features(transmetro, transurbano, metroriel, aerometro, fecha_corte)
    print(f"   Usuarios con features: {len(feats):,}")

    print("▶️  Aplicando población (>=2 abordajes en 28d + sin DELETE CDC)...")
    feats = feats[feats["n_abordajes_28d"] >= 2]
    print(f"   Usuarios con >=2 abordajes: {len(feats):,}")

    activos = cargar_cdc_activos(carpeta, fecha_corte)
    if activos:
        # Población: activo en CDC o no aparece en CDC (aerómetro puro)
        inactivos = set()
        ruta_cdc = os.path.join(carpeta, "cdc_padron_usuarios.csv")
        if os.path.exists(ruta_cdc):
            df_cdc = pd.read_csv(ruta_cdc, usecols=["seq", "commit_ts", "op", "tarjeta"])
            df_cdc["commit_ts"] = pd.to_datetime(df_cdc["commit_ts"], errors="coerce")
            df_cdc = df_cdc[df_cdc["commit_ts"].notna() & (df_cdc["commit_ts"] < fecha_corte)]
            df_cdc = df_cdc.sort_values("seq").reset_index(drop=True)
            df_cdc["sk_usuario"] = df_cdc["tarjeta"].apply(normalizar_id)
            df_cdc = df_cdc[df_cdc["sk_usuario"].notna()]
            estado = {}
            for _, row in df_cdc.iterrows():
                uid = row["sk_usuario"]
                op = str(row["op"]).strip().upper()
                if op in ("INSERT", "UPDATE"):
                    estado[uid] = True
                elif op == "DELETE":
                    estado[uid] = False
            inactivos = {u for u, a in estado.items() if not a}
        feats = feats[~feats["sk_usuario"].isin(inactivos)]
    print(f"   Usuarios en población: {len(feats):,}")

    print(f"▶️  Prediciendo con umbral {UMBRAL:.4f}...")
    X = feats[FEATURE_COLS].astype(float).values
    proba = model.predict_proba(X)[:, 1]
    feats["puntaje"] = proba
    feats["contactar"] = (proba > UMBRAL).astype(int)

    # Resolver tarjetas
    print("▶️  Resolviendo tarjetas por usuario...", flush=True)
    tarjetas_por_usuario = defaultdict(set)
    nombres = ["transmetro", "transurbano", "metroriel", "aerometro"]
    for i, (nombre, df) in enumerate(zip(nombres, [transmetro, transurbano, metroriel, aerometro]), 1):
        if "id_crudo" not in df.columns:
            print(f"   [{i}/4] {nombre}: sin id_crudo, se omite", flush=True)
            continue
        # Construir mapping por grupo (vectorizado)
        for sk, grupo in df[["sk_usuario", "id_crudo"]].dropna().groupby("sk_usuario"):
            for v in grupo["id_crudo"].unique():
                if v:
                    tarjetas_por_usuario[sk].add(str(v).strip())
        print(f"   [{i}/4] {nombre}: {len(df):,} filas procesadas, "
              f"{len(tarjetas_por_usuario):,} usuarios acumulados", flush=True)

    print(f"▶️  Aplicando tarjetas a {len(feats):,} usuarios...", flush=True)
    feats["tarjetas"] = feats["sk_usuario"].apply(
        lambda u: ";".join(sorted(tarjetas_por_usuario.get(u, {u})))
    )
    print(f"✅ Tarjetas resueltas", flush=True)
    return feats


# ---------------------------------------------------------------------------
# Modo DB
# ---------------------------------------------------------------------------

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("DB_USER", "amt_admin"),
        password=os.getenv("DB_PASSWORD"),
        dbname=os.getenv("DB_NAME", "amt_dw"),
    )


def predecir_con_db(fecha_corte_str: str, model):
    conn = get_db_connection()
    try:
        sql = f"""
            SELECT sk_usuario, {', '.join(FEATURE_COLS)}
            FROM features.p2_features_etiqueta_por_corte
            WHERE fecha_corte = TIMESTAMP '{fecha_corte_str}'
              AND en_poblacion = TRUE
            ORDER BY sk_usuario
        """
        df = pd.read_sql(sql, conn)
        if df.empty:
            raise RuntimeError(
                f"No hay features para el corte {fecha_corte_str} en la DB. "
                "Pasá --datos <carpeta> para procesar desde CSV."
            )
        X = df[FEATURE_COLS].astype(float).values
        proba = model.predict_proba(X)[:, 1]
        df["puntaje"] = proba
        df["contactar"] = (proba > UMBRAL).astype(int)
        df["tarjetas"] = ""
        return df
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="CLI de predicción de churn (P2).")
    parser.add_argument("--fecha-corte", required=True)
    parser.add_argument("--datos", default=None)
    args = parser.parse_args()

    fecha = args.fecha_corte.strip()
    if len(fecha) == 10:
        fecha = f"{fecha} 00:00:00"
    if not re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", fecha):
        print(f"❌ Fecha inválida: {fecha!r}")
        sys.exit(2)
    fecha_corte = pd.Timestamp(fecha)

    print("=" * 60)
    print(f" PREDICCIÓN — corte {fecha}")
    print("=" * 60)

    if not os.path.exists(MODELO_HGB):
        print(f"❌ No existe {MODELO_HGB}")
        sys.exit(1)
    model = joblib.load(MODELO_HGB)
    print(f"✅ Modelo: {type(model).__name__}")

    if args.datos:
        feats = predecir_con_datos(args.datos, fecha_corte, model)
    else:
        feats = predecir_con_db(fecha, model)

    df_out = feats.copy()
    df_out["unidad_id"] = df_out["sk_usuario"]
    df_out["tarjetas"] = df_out["tarjetas"].fillna("")
    df_out = df_out[["unidad_id", "tarjetas", "puntaje", "contactar"]]
    df_out = df_out.sort_values("puntaje", ascending=False).reset_index(drop=True)

    os.makedirs(RESULTS_DIR, exist_ok=True)
    tag = fecha.split(" ")[0].replace("-", "")
    ruta = os.path.join(RESULTS_DIR, f"p2_predicciones_{tag}.csv")
    df_out.to_csv(ruta, index=False, encoding="utf-8")

    n_cont = int(df_out["contactar"].sum())
    print(f"\n✅ Predicciones escritas: {ruta}")
    print(f"   Usuarios: {len(df_out):,}")
    print(f"   Contactar (p > {UMBRAL}): {n_cont:,} ({100.0*n_cont/len(df_out):.2f}%)")
    print(f"   Puntaje medio: {df_out['puntaje'].mean():.4f} ± {df_out['puntaje'].std():.4f}")
    print("=" * 60)


if __name__ == "__main__":
    main()