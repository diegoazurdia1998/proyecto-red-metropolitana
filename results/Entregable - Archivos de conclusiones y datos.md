![[fase1_1.1_ingesta_batch_cdc_resumen]]

### Vía de ingesta para Transurbano

**Vía seleccionada:** Batch (micro-lote diario acumulado).

**Justificación técnica:**

1. **Naturaleza física de la recolección embarcada:** A diferencia de Transmetro y Aerómetro, que tienen torniquetes fijos con conexión de red dedicada al servidor central, las unidades de Transurbano operan con validadores embarcados en movimiento. Estos validadores almacenan las transacciones en memoria local (disco flash) debido a intermitencias de señal móvil o ausencia de enlace 4G continuo. La sincronización masiva se realiza por ráfagas WiFi o red física cuando el autobús llega al predio o patio de maniobras al finalizar el turno.

2. **Estructura tabular y desacople temporal:** El archivo no es un flujo de eventos en crudo con timestamps sincronizados, sino un volcado consolidado de bitácoras donde la fecha y la hora ya vienen separadas (`DD/MM/YYYY` y `HH:MM:SS`). Esto refleja un proceso previo de liquidación contable de las máquinas validadoras.

3. **Optimización de recursos y rendimiento:** Con un peso de ~41.13 MB y cerca de 270,000 transacciones diarias proyectadas, forzar una simulación fila por fila en Kafka satura innecesariamente las colas de streaming con eventos que en la realidad ya nacen agrupados por liquidación de turno. Procesarlo en lotes (batch) permite ingestas vectorizadas ultrarrápidas directo a formato Apache Parquet particionado en disco.

**Consecuencias de la decisión:**

- **Impacto positivo:** Reducción de latencia de carga, simplificación del monitoreo, menor sobrecarga de memoria en los brokers de Kafka y garantía de reprocesamiento determinista e idempotente en caso de reingesta por lotes.
- **Trade-off asumido:** Las métricas de abordaje de Transurbano no están disponibles en tiempo real; sufren un desfase (latencia de datos) equivalente al intervalo de descarga de los autobuses (típicamente al cierre de operación o en ventanas de varias horas), lo cual es aceptable para análisis de demanda y modelado dimensional en la Agencia Metropolitana de Transporte.

### Almacenamiento de la capa Bronze

Se decidió guardar la capa Bronze en un Data Lake (carpetas con archivos Parquet) en lugar de una base de datos o Data Warehouse porque el archivo de MetroRiel viene en formato JSON con datos anidados. Si se hubiera usado una base de datos tradicional, habría sido necesario desarmar o simplificar esa información antes de almacenarla para que encajara en tablas rígidas, lo que habría roto la regla de conservar los datos exactamente como llegaron de la calle. El Data Lake permitió guardar esos viajes completos sin perder ningún detalle original y sin riesgo de que el proceso fallara si en el futuro se agregaran campos nuevos.

Además, esta elección hizo que el sistema fuera más rápido, económico y ordenado. Guardar los datos crudos en archivos directos evitó saturar la memoria y el rendimiento de la base de datos principal, dejándola libre para las etapas donde realmente se necesitaba hacer cálculos y limpieza (Silver y Gold). Así, el Data Lake funcionó como un archivo histórico seguro donde todo quedó respaldado tal cual se recibió, dejando las separaciones y transformaciones complejas para los pasos siguientes.

![[fase1_1.2_resumen_cdc_conteos]]

![[fase1_1.3_reporte_calidad_cuarentena]]

## 1.4 Diseño Dimensional y Capa Gold

### 1. Declaración formal del grano de la tabla de hechos principal


  > _"Un evento atómico individual de abordaje a un vehículo o ingreso por torniquete a la red de transporte metropolitano, identificado por usuario, estación o parada de acceso, línea o ruta, y momento exacto de validación en tiempo local."_

Nota: Bajo este grano estándar, el viaje de MetroRiel se descompone atómicamente tomando su punto de abordaje/entrada `entry` para convivir uniformemente en la tabla de hechos general de abordajes, manteniendo opcionalmente una segunda tabla de hechos acumulada específica para viajes cerrados de MetroRiel.

![[Diagrama modelo gold.png]]

### 2. Matriz del Bus de Datos 

| Proceso de Negocio / Tabla de Hechos                     | dim_tiempo | dim_usuario | dim_estacion_parada  |
| -------------------------------------------------------- | ---------- | ----------- | -------------------- |
| **Abordajes y Validaciones (`fact_abordajes`)**          | X          | X           | X                    |
| **Viajes Completos MetroRiel (`fact_viajes_metroriel`)** | X          | X           | X (Origen / Destino) |
### 3. Clasificación formal de medidas numéricas

- **Aditivas (se pueden sumar a través de cualquier dimensión):**
  - `monto_gtq`: Tarifa cobrada por el pasaje/abordaje.
  - `cantidad_abordajes`: Contador de eventos de ingreso (1 por fila).
  - `duracion_segundos`: Duración del trayecto (en `fact_viajes_metroriel`).

- **Semi-aditivas (se pueden sumar a través de algunas dimensiones, pero no en el tiempo):**
  - Saldo remanente de tarjeta o inventario de usuarios activos (válido sumar entre zonas/estaciones en un corte, pero no a lo largo del tiempo).

- **No aditivas (no se pueden sumar directamente, requieren ratios o promedios):**
  - `tarifa_promedio_por_viaje` (∑ monto / ∑ abordajes).
  - `duracion_promedio_minutos`.

![[fase1_1.5_evidencia_idempotencia]]