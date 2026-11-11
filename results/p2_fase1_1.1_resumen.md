# Fase 1.1 — Auditoría de ingesta (9 fuentes) y tiempos por etapa

## Tabla 1: Conteos esperadas vs cargadas

| archivo_fuente                | dataset_bronze            | via_ingesta       |   filas_origen |   filas_bronze |   diferencia | explicacion                                | estado   |
|:------------------------------|:--------------------------|:------------------|---------------:|---------------:|-------------:|:-------------------------------------------|:---------|
| tm_estaciones.csv             | tm_estaciones             | Batch             |            104 |            104 |            0 | ok                                         | OK       |
| tu_paradas.csv                | tu_paradas                | Batch             |            328 |            328 |            0 | ok                                         | OK       |
| mr_estaciones.csv             | mr_estaciones             | Batch             |             22 |             22 |            0 | ok                                         | OK       |
| am_estaciones.csv             | am_estaciones             | Batch             |             14 |             14 |            0 | ok                                         | OK       |
| transurbano_transacciones.csv | transurbano_transacciones | Batch             |        1635788 |        1635788 |            0 | ok                                         | OK       |
| metroriel_viajes.jsonl        | metroriel_viajes          | Batch             |         590844 |         590844 |            0 | ok                                         | OK       |
| cdc_padron_usuarios.csv       | cdc_padron_usuarios       | CDC               |          77636 |          77636 |            0 | ok                                         | OK       |
| transmetro_validaciones.csv   | transmetro_validaciones   | Streaming (Kafka) |         713786 |         713785 |           -1 | faltan_1_filas_en_bronze (ingesta parcial) | REVISAR  |
| aerometro_boardings.csv       | aerometro_boardings       | Streaming (Kafka) |         387053 |         387053 |            0 | ok                                         | OK       |

- **Total filas origen:** 3,405,575
- **Total filas Bronze:** 3,405,574
- **Balance general:** -1 filas
- **Fuentes con diferencia != 0:** 1 / 9

## Tabla 2: Tiempos por etapa

| etapa          | start                     | end                       |   duration_s | estado   |
|:---------------|:--------------------------|:--------------------------|-------------:|:---------|
| ingesta_bronze | 2026-11-07T03:33:06+00:00 | 2026-11-07T03:35:03+00:00 |     117.003  | OK       |
| cargar_staging | 2026-11-07T03:35:03+00:00 | 2026-11-07T03:38:36+00:00 |     213.282  | OK       |
| ejecutar_dbt   | 2026-11-07T03:38:37+00:00 | 2026-11-07T03:39:47+00:00 |      70.7647 | OK       |
| cuarentena     | 2026-11-07T03:39:47+00:00 | 2026-11-07T03:39:49+00:00 |       1.3894 | OK       |
| medir_conteos  | 2026-11-07T03:39:49+00:00 | 2026-11-07T03:39:51+00:00 |       2.4816 | OK       |

- **Tiempo total del pipeline:** 404.92 s
