"""
Mide el 'costo de la unidad' (rubric 1.2) para el corte A.

Tres métricas (Q9/Q10/Q14):
  1. Costo de identidad: % de usuarios de la población con >= 2 tarjetas asociadas.
  2. Cobertura de identidad: % de identificadores crudos mapeables a sk_usuario canónico.
  3. Falsos positivos potenciales por tarjeta: de los usuarios con etiqueta_persona=0
     y >= 2 tarjetas, ¿cuántas tarjetas "abandonaron" por separado?

Salidas:
  results/p2_fase1_1.2_costo_unidad.csv
  results/p2_fase1_1.2_costo_unidad.md

Fuentes:
  - staging.padron_transmetro_actual (tarjetas asociadas por usuario, es_activa)
  - silver_*_validado (identificadores crudos + abordajes por tarjeta)
  - features.p2_features_etiqueta_por_corte (etiqueta Persona para corte A)

Aerómetro: cuenta como 1 identificador (user_hash). Limitación documentada.
"""
import os
import sys
from datetime import datetime

import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

RESULTS_DIR = "./results"
CORTE_A = "2026-07-01 00:00:00"


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("DB_USER", "amt_admin"),
        password=os.getenv("DB_PASSWORD"),
        dbname=os.getenv("DB_NAME", "amt_dw"),
    )


def verificar_dependencias(conn):
    """Aborta con mensaje claro si faltan tablas clave."""
    faltantes = []
    with conn.cursor() as cur:
        for tabla in [
            "staging.padron_transmetro_actual",
            "silver.silver_transmetro_validado",
            "silver.silver_transurbano_validado",
            "silver.silver_metroriel_validado",
            "silver.silver_aerometro_validado",
            "features.p2_features_etiqueta_por_corte",
        ]:
            cur.execute(f"SELECT to_regclass('{tabla}')")
            if cur.fetchone()[0] is None:
                faltantes.append(tabla)
    if faltantes:
        print("❌ Faltan tablas requeridas:")
        for t in faltantes:
            print(f"   - {t}")
        print("Solución: corre `python -m scripts.orchestration.pipeline_flow` y")
        print("          `dbt run --select features --vars '{\"fecha_corte\": \"2026-07-01 00:00:00\"}'`.")
        sys.exit(1)


def leer_identificadores_crudos(conn) -> pd.DataFrame:
    """
    Devuelve (identificador_crudo, sk_usuario, modo) para todos los registros VALIDO.
    El identificador_crudo es el string original del Silver (tarjeta / num_tarjeta / user_hash).
    """
    sql = """
        SELECT tarjeta::text AS id_crudo,
               NULLIF(REGEXP_REPLACE(tarjeta, '\\D', '', 'g'), '')::bigint::text AS sk_usuario,
               'transmetro'::text AS modo
        FROM silver.silver_transmetro_validado
        WHERE regla_calidad = 'VALIDO' AND tarjeta IS NOT NULL

        UNION ALL
        SELECT num_tarjeta::text,
               NULLIF(REGEXP_REPLACE(num_tarjeta, '\\D', '', 'g'), '')::bigint::text,
               'transurbano'::text
        FROM silver.silver_transurbano_validado
        WHERE regla_calidad = 'VALIDO' AND num_tarjeta IS NOT NULL

        UNION ALL
        SELECT num_tarjeta::text,
               NULLIF(REGEXP_REPLACE(num_tarjeta, '\\D', '', 'g'), '')::bigint::text,
               'metroriel'::text
        FROM silver.silver_metroriel_validado
        WHERE regla_calidad = 'VALIDO' AND num_tarjeta IS NOT NULL

        UNION ALL
        SELECT user_hash::text,
               user_hash::text,
               'aerometro'::text
        FROM silver.silver_aerometro_validado
        WHERE regla_calidad = 'VALIDO' AND user_hash IS NOT NULL
    """
    return pd.read_sql(sql, conn)


def leer_poblacion_corte_a(conn) -> pd.DataFrame:
    sql = f"""
        SELECT sk_usuario, etiqueta
        FROM features.p2_features_etiqueta_por_corte
        WHERE fecha_corte = TIMESTAMP '{CORTE_A}'
          AND en_poblacion = TRUE
    """
    return pd.read_sql(sql, conn)


def leer_padron(conn) -> pd.DataFrame:
    sql = """
        SELECT NULLIF(REGEXP_REPLACE(tarjeta, '\\D', '', 'g'), '')::bigint::text AS sk_usuario,
               tarjeta::text AS id_crudo,
               es_activa
        FROM staging.padron_transmetro_actual
        WHERE tarjeta IS NOT NULL
          AND NULLIF(REGEXP_REPLACE(tarjeta, '\\D', '', 'g'), '') IS NOT NULL
    """
    return pd.read_sql(sql, conn)


def metrica_1_costo_identidad(ids: pd.DataFrame, poblacion: pd.DataFrame) -> dict:
    """% de usuarios de la población con >= 2 tarjetas asociadas."""
    # Tarjetas por usuario (contando Aerómetro como 1 identificador = user_hash)
    tarjetas_por_usuario = (
        ids[ids["sk_usuario"].notna()]
        .groupby("sk_usuario")["id_crudo"]
        .nunique()
        .reset_index()
        .rename(columns={"id_crudo": "n_tarjetas"})
    )
    df = poblacion.merge(tarjetas_por_usuario, on="sk_usuario", how="left")
    df["n_tarjetas"] = df["n_tarjetas"].fillna(1).astype(int)

    n_usuarios = len(df)
    n_multi = int((df["n_tarjetas"] >= 2).sum())
    return {
        "metrica": "1_costo_identidad",
        "descripcion": "Fracción de usuarios de la población con >= 2 tarjetas asociadas",
        "numerador": n_multi,
        "denominador": n_usuarios,
        "valor": round(100.0 * n_multi / max(n_usuarios, 1), 2),
        "unidad": "%",
    }


def metrica_2_cobertura_identidad(ids: pd.DataFrame) -> dict:
    """% de user_hash de Aerómetro que se pueden mapear a un sk_usuario de tarjeta.
    Esperado: 0% (limitación de gobernanza de Aerómetro)."""
    aerometro = ids[ids["modo"] == "aerometro"][["id_crudo", "sk_usuario"]].drop_duplicates()
    tarjetas = ids[ids["modo"] != "aerometro"][["sk_usuario"]].drop_duplicates()
    n_aero = aerometro["id_crudo"].nunique()
    n_vinculados = aerometro[aerometro["sk_usuario"].isin(tarjetas["sk_usuario"])]["id_crudo"].nunique()
    return {
        "metrica": "2_cobertura_identidad",
        "descripcion": "Fracción de user_hash de Aerómetro vinculables a un sk_usuario de tarjeta",
        "numerador": n_vinculados,
        "denominador": n_aero,
        "valor": round(100.0 * n_vinculados / max(n_aero, 1), 2),
        "unidad": "%",
    }


def metrica_3_falsos_positivos(
    ids: pd.DataFrame, poblacion: pd.DataFrame, conn
) -> dict:
    """
    Falsos positivos potenciales por tarjeta.

    Para usuarios con etiqueta_persona = 0 y >= 2 tarjetas:
      - Calcular etiqueta por tarjeta (1 si esa tarjeta NO tiene abordajes en 30d).
      - Sumar tarjetas con etiqueta_tarjeta = 1.
    """
    # Universo: usuarios con etiqueta_persona = 0 y >= 2 tarjetas
    tarjetas_por_usuario = (
        ids[ids["sk_usuario"].notna()]
        .groupby("sk_usuario")["id_crudo"]
        .nunique()
        .reset_index()
        .rename(columns={"id_crudo": "n_tarjetas"})
    )
    df = poblacion.merge(tarjetas_por_usuario, on="sk_usuario", how="inner")
    df_multi_nochurn = df[(df["n_tarjetas"] >= 2) & (df["etiqueta"] == 0)]
    sk_objetivo = set(df_multi_nochurn["sk_usuario"].tolist())

    if not sk_objetivo:
        return {
            "metrica": "3_falsos_positivos",
            "descripcion": "Tarjetas 'abandonadas' de personas con etiqueta_persona=0 y >= 2 tarjetas",
            "numerador": 0,
            "denominador": int(df["n_tarjetas"].sum()),
            "valor": 0.0,
            "unidad": "%",
        }

    # Etiqueta por tarjeta: ¿tiene abordajes en 30d siguientes a CORTE_A?
    ids_objetivo = ids[ids["sk_usuario"].isin(sk_objetivo)][["sk_usuario", "id_crudo"]].drop_duplicates()
    ids_objetivo["sk_usuario"] = ids_objetivo["sk_usuario"].astype(str)

    # Traer abordajes por (id_crudo) en 30d siguientes. Necesitamos el timestamp por Silver.
    sql_abordajes = f"""
        WITH unioned AS (
            SELECT tarjeta::text AS id_crudo, timestamp_local
            FROM silver.silver_transmetro_validado
            WHERE regla_calidad = 'VALIDO'
            UNION ALL
            SELECT num_tarjeta::text, timestamp_local
            FROM silver.silver_transurbano_validado
            WHERE regla_calidad = 'VALIDO'
            UNION ALL
            SELECT num_tarjeta::text, timestamp_origen
            FROM silver.silver_metroriel_validado
            WHERE regla_calidad = 'VALIDO'
            UNION ALL
            SELECT user_hash::text, timestamp_local
            FROM silver.silver_aerometro_validado
            WHERE regla_calidad = 'VALIDO'
        )
        SELECT id_crudo, COUNT(*) AS n_abordajes_30d
        FROM unioned
        WHERE timestamp_local >= TIMESTAMP '{CORTE_A}'
          AND timestamp_local <  TIMESTAMP '{CORTE_A}' + INTERVAL '30 days'
        GROUP BY id_crudo
    """
    abordajes = pd.read_sql(sql_abordajes, conn)
    set_con_abordajes = set(abordajes["id_crudo"].tolist())

    # Para cada tarjeta del universo: si NO está en set_con_abordajes → etiqueta_tarjeta=1
    tarjetas_universo = ids_objetivo["id_crudo"].tolist()
    falsos_positivos = sum(1 for t in tarjetas_universo if t not in set_con_abordajes)

    total_tarjetas_poblacion = int(df["n_tarjetas"].sum())

    return {
        "metrica": "3_falsos_positivos",
        "descripcion": "Tarjetas 'abandonadas' de personas con etiqueta_persona=0 y >= 2 tarjetas",
        "numerador": falsos_positivos,
        "denominador": total_tarjetas_poblacion,
        "valor": round(100.0 * falsos_positivos / max(total_tarjetas_poblacion, 1), 2),
        "unidad": "%",
    }


def generar_markdown(metricas: list, ruta_md: str):
    ahora = datetime.now().isoformat(timespec="seconds")
    with open(ruta_md, "w", encoding="utf-8") as f:
        f.write("# Fase 1.2 — Costo de la unidad (Persona vs Tarjeta)\n\n")
        f.write(f"_Generado: {ahora}_\n\n")

        f.write("## Justificación de la unidad: Persona (`sk_usuario`)\n\n")
        f.write(
            "La unidad de análisis es **Persona**, no Tarjeta. Esta decisión se tomó y justificó "
            "en el Proyecto 1: Transmetro, Transurbano y MetroRiel comparten un identificador "
            "entero subyacente que se normaliza con `REGEXP_REPLACE(tarjeta, '\\D', '', 'g')::bigint::text`, "
            "mientras que Aerómetro usa `user_hash` (hex de 12 chars) y no puede correlacionarse "
            "determinísticamente con los otros sistemas. El P2 no redefine la unidad; **mide el costo "
            "empírico** de haber elegido Persona.\n\n"
        )

        f.write("## Las 3 métricas del costo\n\n")
        f.write("| # | Métrica | Numerador | Denominador | Valor |\n")
        f.write("|---|---|---|---|---|\n")
        for m in metricas:
            f.write(
                f"| {m['metrica']} | {m['descripcion']} | "
                f"{m['numerador']:,} | {m['denominador']:,} | "
                f"{m['valor']}{m['unidad']} |\n"
            )
        f.write("\n")

        f.write("### Interpretación de negocio\n\n")
        for m in metricas:
            f.write(f"**{m['metrica']}**\n\n")
            if m["metrica"] == "1_costo_identidad":
                f.write(
                    f"El {m['valor']}% de la población usa ≥2 modos (y por eso tiene ≥2 tarjetas asociadas). "
                    "Esto **crea la posibilidad** de falsos positivos si se trabaja a nivel tarjeta "
                    "(una tarjeta puede 'abandonar' mientras la persona sigue viajando con otra), pero "
                    "**el daño real se mide en la métrica 3**.\n\n"
                )
            elif m["metrica"] == "2_cobertura_identidad":
                f.write(
                    f"El {m['valor']}% de los identificadores crudos se mapean a un `sk_usuario` "
                    "canónico. La cobertura **no es 100%** porque Aerómetro no puede correlacionarse "
                    "con los otros sistemas: cada `user_hash` es un identificador aislado. "
                    "La decisión Persona **hereda esta limitación** y la documenta como gobernanza.\n\n"
                )
            elif m["metrica"] == "3_falsos_positivos":
                f.write(
                    f"El {m['valor']}% de las tarjetas de la población serían **falsos positivos** "
                    "si se trabajara a nivel tarjeta (tarjeta sin abordajes en 30d, pero persona "
                    "con al menos un abordaje en otra tarjeta). Este es el **costo principal** de la "
                    "unidad: contactos de retención desperdiciados en usuarios que en realidad "
                    "siguen viajando.\n\n"
                )

        f.write("## Limitación declarada: Aerómetro\n\n")
        f.write(
            "Aerómetro usa `user_hash` de 12 chars. Cada hash es un identificador **aislado**: "
            "no se puede mapear a un `sk_usuario` de Transmetro/Transurbano/MetroRiel, ni a otro "
            "`user_hash` del mismo usuario en Aerómetro. Para el cálculo del costo, Aerómetro cuenta "
            "como **1 tarjeta** en el denominador, lo cual es conservador: si un usuario usa Aerómetro "
            "y Transmetro, aparecerá como 2 identificadores no vinculados, aunque en realidad sea 1 persona.\n"
        )
        f.write("## Relación entre las 3 métricas y costo monetario\n\n")
        f.write("| Métrica | Rol en el análisis |\n")
        f.write("|---|---|\n")
        f.write("| **1_costo_identidad** | Magnitud del fenómeno multi-tarjeta (población en riesgo). |\n")
        f.write("| **2_cobertura_identidad** | Limitación de gobernanza: Aerómetro no se puede vincular. |\n")
        f.write("| **3_falsos_positivos** | **Costo materializado**: tarjetas que se marcarían como abandonadas sin serlo. |\n\n")

        m3 = next((m for m in metricas if m["metrica"] == "3_falsos_positivos"), None)
        if m3:
            falsos_pos = m3["numerador"]
            costo_por_contacto = 5.0
            costo_corte = falsos_pos * costo_por_contacto
            f.write("**Costo monetario de la decisión Persona vs Tarjeta:**\n\n")
            f.write(f"- Falsos positivos por corte: **{falsos_pos:,} tarjetas**.\n")
            f.write(f"- Costo de contacto: Q{costo_por_contacto:.2f} × {falsos_pos:,} = **Q{costo_corte:,.2f} por corte**.\n")
            f.write(f"- Proyectado a 12 cortes/año: **Q{costo_corte * 12:,.2f}/año**.\n\n")
            f.write("**Conclusión:** la decisión Persona (vs Tarjeta) evita estos contactos "
                    "desperdiciados. Es el ahorro que justifica la complejidad de la resolución de identidad.\n")


def main():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    conn = get_db_connection()
    try:
        verificar_dependencias(conn)

        print("▶️  Leyendo fuentes...")
        ids = leer_identificadores_crudos(conn)
        poblacion = leer_poblacion_corte_a(conn)
        print(f"   identificadores crudos: {ids['id_crudo'].nunique():,}")
        print(f"   población corte A: {len(poblacion):,}")

        if poblacion.empty:
            print("❌ Población del corte A vacía. Corre dbt run para el corte A primero.")
            sys.exit(1)

        print("▶️  Calculando métricas...")
        m1 = metrica_1_costo_identidad(ids, poblacion)
        m2 = metrica_2_cobertura_identidad(ids)
        m3 = metrica_3_falsos_positivos(ids, poblacion, conn)
        metricas = [m1, m2, m3]

        df = pd.DataFrame(metricas)
        ruta_csv = os.path.join(RESULTS_DIR, "p2_fase1_1.2_costo_unidad.csv")
        df.to_csv(ruta_csv, index=False, encoding="utf-8")
        print(f"✅ CSV: {ruta_csv}")

        ruta_md = os.path.join(RESULTS_DIR, "p2_fase1_1.2_costo_unidad.md")
        generar_markdown(metricas, ruta_md)
        print(f"✅ MD: {ruta_md}")

        print("\n--- RESULTADO ---")
        print(df.to_string(index=False))
        print("-----------------\n")
    finally:
        conn.close()


if __name__ == "__main__":
    main()