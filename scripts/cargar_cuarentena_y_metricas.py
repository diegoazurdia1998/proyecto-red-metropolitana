import os
import csv
import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    user=os.getenv("DB_USER", "amt_admin"),
    password=os.getenv("DB_PASSWORD"),
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
    ("Transurbano", "silver.silver_transurbano_validado"),
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
    print(f"[OK] Cuarentena cargada desde {tabla}", flush=True)

conn.commit()

# 3. Métricas por regla y fuente (agregacion en SQL, sin pandas)
cursor.execute("""
    SELECT fuente, regla_violada, COUNT(*) AS registros_rechazados
    FROM silver.cuarentena
    GROUP BY fuente, regla_violada
    ORDER BY fuente, registros_rechazados DESC;
""")
filas = cursor.fetchall()
columnas = ["fuente", "regla_violada", "registros_rechazados"]

total_rechazos = sum(f[2] for f in filas) if filas else 0

# Imprimir en consola
print("\n=== REPORTE DE CALIDAD Y CUARENTENA (Subfase 1.3) ===", flush=True)
for fuente, regla, cant in filas:
    print(f"- [{fuente}] Regla violada: {regla} -> {cant} registros", flush=True)
print(f"\nTotal registros en cuarentena: {total_rechazos}", flush=True)

# 4. Guardar version tabular (.csv) sin pandas
os.makedirs("results", exist_ok=True)
ruta_csv = "results/fase1_1.3_metricas_cuarentena.csv"
with open(ruta_csv, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(columnas)
    writer.writerows(filas)
print(f"Metricas tabulares guardadas en: {ruta_csv}", flush=True)

# 5. Guardar version documental (.md) sin pandas/tabulate
ruta_md = "results/fase1_1.3_reporte_calidad_cuarentena.md"
with open(ruta_md, "w", encoding="utf-8") as f:
    f.write("# Reporte de Calidad de Datos y Cuarentena (Subfase 1.3)\n\n")
    f.write(f"- **Total Registros en Cuarentena:** {total_rechazos}\n\n")
    f.write("## Distribucion de Fallas por Fuente y Regla\n\n")
    f.write("| fuente | regla_violada | registros_rechazados |\n")
    f.write("|---|---|---|\n")
    for fuente, regla, cant in filas:
        f.write(f"| {fuente} | {regla} | {cant} |\n")
    f.write("\n\n---\n")
    f.write("### Criterios de Aislamiento Aplicados:\n")
    f.write("- **DUPLICADO_TORNIQUETE:** Marcaciones consecutivas en ventana <= 120 segundos.\n")
    f.write("- **PARADA_NULA:** Codigo de estacion/parada vacio o nulo.\n")
    f.write("- **FECHA_FUTURA:** Timestamps posteriores al tiempo actual.\n")
    f.write("- **VIAJE_SIN_SALIDA:** Registros sin evento de salida en sistemas cerrados (MetroRiel).\n")

print(f"Reporte narrativo guardado en: {ruta_md}", flush=True)

cursor.close()
conn.close()