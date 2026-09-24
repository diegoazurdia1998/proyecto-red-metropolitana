import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    user=os.getenv("DB_USER", "amt_admin"),
    password=os.getenv("DB_PASSWORD", "amt_secure_pass"),
    dbname=os.getenv("DB_NAME", "amt_dw")
)
cursor = conn.cursor()

# 1. Limpiar cuarentena previa para garantizar idempotencia
cursor.execute("TRUNCATE TABLE silver.cuarentena;")

# 2. Enrutar registros inválidos hacia silver.cuarentena
fuentes = [
    ("Transmetro", "silver.silver_transmetro_validado"),
    ("MetroRiel", "silver.silver_metroriel_validado"),
    ("Aerometro", "silver.silver_aerometro_validado"),
    ("Transurbano", "silver.silver_transurbano_validado")
]

for fuente, tabla in fuentes:
    cursor.execute(f"""
        INSERT INTO silver.cuarentena (fuente, regla_violada, registro_crudo)
        SELECT 
            '{fuente}' AS fuente,
            regla_calidad AS regla_violada,
            to_jsonb(t) AS registro_crudo
        FROM {tabla} t
        WHERE regla_calidad <> 'VALIDO';
    """)

conn.commit()

# 3. Reporte de métricas de cuarentena por regla y fuente
cursor.execute("""
    SELECT fuente, regla_violada, COUNT(*) AS cantidad
    FROM silver.cuarentena
    GROUP BY fuente, regla_violada
    ORDER BY fuente, cantidad DESC;
""")
metricas = cursor.fetchall()

# Asegurar directorio de resultados
os.makedirs("results", exist_ok=True)

# 3. Consultar métricas de cuarentena
cursor.execute("""
    SELECT 
        fuente, 
        regla_violada, 
        COUNT(*) AS registros_rechazados
    FROM silver.cuarentena
    GROUP BY fuente, regla_violada
    ORDER BY fuente, registros_rechazados DESC;
""")
filas = cursor.fetchall()
columnas = ["fuente", "regla_violada", "registros_rechazados"]
df_metricas = pd.DataFrame(filas, columns=columnas)

total_rechazos = df_metricas["registros_rechazados"].sum() if not df_metricas.empty else 0

# Imprimir en consola
print("\n=== REPORTE DE CALIDAD Y CUARENTENA (Subfase 1.3) ===")
for _, row in df_metricas.iterrows():
    print(f"- [{row['fuente']}] Regla violada: {row['regla_violada']} -> {row['registros_rechazados']} registros")
print(f"\nTotal registros en cuarentena: {total_rechazos}")

# 4. Guardar versión tabular (.csv)
ruta_csv = f"results/fase1_1.3_metricas_cuarentena.csv"
df_metricas.to_csv(ruta_csv, index=False, encoding="utf-8")
print(f"Métricas tabulares guardadas en: {ruta_csv}")

# 5. Guardar versión documental (.md)
ruta_md = f"results/fase1_1.3_reporte_calidad_cuarentena.md"
with open(ruta_md, "w", encoding="utf-8") as f:
    f.write(f"# Reporte de Calidad de Datos y Cuarentena (Subfase 1.3)\n\n")
    f.write(f"- **Total Registros en Cuarentena:** {total_rechazos}\n\n")
    f.write("## Distribución de Fallas por Fuente y Regla\n\n")
    f.write(df_metricas.to_markdown(index=False))
    f.write("\n\n---\n")
    f.write("### Criterios de Aislamiento Aplicados:\n")
    f.write("- **DUPLICADO_TORNIQUETE:** Marcaciones consecutivas en ventana <= 120 segundos.\n")
    f.write("- **PARADA_NULA:** Código de estación/parada vacío o nulo.\n")
    f.write("- **FECHA_FUTURA:** Timestamps posteriores al tiempo actual.\n")
    f.write("- **VIAJE_SIN_SALIDA:** Registros sin evento de salida en sistemas cerrados (MetroRiel).\n")

print(f"Reporte narrativo guardado en: {ruta_md}")

cursor.close()
conn.close()