# Reporte de Ingesta Streaming (Subfase 1.1)

- **Origen:** Consumidor Kafka (Topicos Transmetro y Aerometro)
- **Destino:** Capa Bronze (Parquet SNAPPY, particionado YYYY/MM/DD)
- **Total Registros Consumidos:** 1,100,838

## Detalle de Registros por Topico

| topico_dataset          |   registros_ingeridos |
|:------------------------|----------------------:|
| transmetro_validaciones |                713785 |
| aerometro_boardings     |                387053 |
