# Reporte de Ingesta Batch y CDC a Bronze (Subfase 1.1)

- **Destino:** Capa Bronze (Archivos Parquet comprimidos con SNAPPY)
- **Total Registros Procesados:** 1163409

## Detalle de Registros por Archivo Fuente

| archivo_fuente                |   registros_ingeridos |
|:------------------------------|----------------------:|
| tm_estaciones.csv             |                   104 |
| tu_paradas.csv                |                   328 |
| mr_estaciones.csv             |                    22 |
| am_estaciones.csv             |                    14 |
| transurbano_transacciones.csv |                832791 |
| metroriel_viajes.jsonl        |                299100 |
| cdc_padron_usuarios.csv       |                 31050 |

---
### Fuentes Ingeridas:
- **Catálogos:** Estaciones de Transmetro, paradas de Transurbano, estaciones de MetroRiel y Aerómetro.
- **Transacciones:** Transurbano (CSV) y MetroRiel (JSONL).
- **Log CDC:** Padrón histórico de usuarios de Transmetro.
