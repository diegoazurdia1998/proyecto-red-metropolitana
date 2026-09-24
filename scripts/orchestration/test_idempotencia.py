from pipeline_flow import red_metropolitana_pipeline
import pandas as pd
import os

def ejecutar_prueba_idempotencia():
    print("--- INICIANDO CORRIDA 1 ---")
    conteos_1 = red_metropolitana_pipeline(corrida_label="CORRIDA 1")

    print("\n--- INICIANDO CORRIDA 2 (INMEDIATA) ---")
    conteos_2 = red_metropolitana_pipeline(corrida_label="CORRIDA 2")

    es_identico = True
    for tabla, c1 in conteos_1.items():
        c2 = conteos_2.get(tabla, -1)
        estado = "IDÉNTICO" if c1 == c2 else "FALLO"
        if c1 != c2:
            es_identico = False
        print(f"{tabla:<32} | {c1:>10} | {c2:>10} | {estado:>10}")

    os.makedirs("results", exist_ok=True)

    # 1. Generar CSV de conteos
    filas = []
    for tabla, c1 in conteos_1.items():
        c2 = conteos_2.get(tabla, -1)
        filas.append({"tabla": tabla, "conteo_corrida_1": c1, "conteo_corrida_2": c2, "diferencia": c2 - c1})

    df_diff = pd.DataFrame(filas)
    df_diff.to_csv("results/fase1_1.5_conteos_corridas.csv", index=False)

    # 2. Generar reporte Markdown de idempotencia
    with open("results/fase1_1.5_evidencia_idempotencia.md", "w", encoding="utf-8") as f:
        f.write("# Evidencia de Idempotencia de Flujo (Subfase 1.5)\n\n")
        f.write(df_diff.to_markdown(index=False))
        f.write(f"\n\n**Veredicto:** {'Idempotente (Cero duplicados)' if es_identico else 'Fallo de consistencia'}\n")

if __name__ == "__main__":
    ejecutar_prueba_idempotencia()

