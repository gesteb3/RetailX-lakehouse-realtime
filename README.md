# RetailX Lakehouse Real-Time

Arquitectura Lakehouse con procesamiento batch y streaming en tiempo real.

La solución integra Apache Spark, Delta Lake, Kafka, Python y salidas optimizadas para Power BI. El objetivo es simular una plataforma analítica empresarial para RetailX, capaz de procesar ventas históricas, eventos web y eventos GPS de camiones en tiempo real.

## 1. Arquitectura general

El proyecto usa una arquitectura Medallion compuesta por tres capas:

### Bronze

Capa de datos crudos.

Contiene los datos originales provenientes de diferentes fuentes:

- CSV: ventas simuladas del ERP/POS.
- JSON: eventos web simulados.
- XML: catálogo maestro de productos.
- Kafka: eventos GPS de camiones en tiempo real.

En esta capa se conserva trazabilidad mediante columnas técnicas como:

- `_ingestion_timestamp`
- `_source_system`
- `_source_format`
- `_batch_id`

### Silver

Capa de datos limpios y normalizados.

En esta capa se aplican:

- Conversión correcta de tipos de datos.
- Limpieza de campos.
- Eliminación de duplicados.
- Validación de fechas.
- Filtros de calidad.
- Enriquecimiento de ventas con catálogo de productos.

Tablas principales:

- `silver_ventas`
- `silver_web_events`
- `silver_productos`
- `silver_ventas_enriquecidas`
- `silver_data_quality_report`

### Gold

Capa analítica lista para negocio, dashboards y Power BI.

Incluye KPIs batch y KPIs real-time.

Tablas batch:

- `gold_kpis_generales`
- `gold_ventas_mensuales`
- `gold_cliente_mayor_volumen`
- `gold_top10_ventas`
- `gold_promedio_venta_cliente`
- `gold_pareto_80_clientes`
- `gold_resumen_pareto`
- `gold_ventas_categoria_canal`
- `gold_web_kpis`

Tablas real-time:

- `gold_rt_kpis_generales`
- `gold_rt_kpis_por_zona`
- `gold_rt_toneladas_por_hora`
- `gold_rt_estado_flota`
- `gold_rt_alertas_operativas`

## 2. Flujo de datos

### Flujo batch

```text
CSV / JSON / XML
        ↓
Bronze Delta
        ↓
Silver Delta
        ↓
Gold Delta
        ↓
Power BI CSV final
```

### Flujo streaming

```text
Python Kafka Producer
        ↓
Kafka topic gpscamiones
        ↓
Spark Structured Streaming
        ↓
Bronze Delta Streaming
        ↓
Gold Real-Time KPIs
        ↓
Power BI CSV final
```

## 3. Tecnologías utilizadas

- Python
- PySpark
- Apache Spark 3.5.1
- Delta Lake
- Apache Kafka
- Docker
- Docker Compose
- Power BI
- Git

## 4. Estructura del proyecto

```text
retailx-lakehouse-realtime/
│
├── config/
│   └── spark_config.py
│
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── powerbi/
│   └── checkpoints/
│
├── scripts/
│   ├── 01_setup_data.py
│   ├── 02_batch_bronze.py
│   ├── 03_batch_silver.py
│   ├── 04_batch_gold.py
│   ├── 05_kafka_producer_gps.py
│   ├── 06_streaming_bronze.py
│   ├── 07_streaming_gold_kpis.py
│   └── 08_export_powerbi.py
│
├── logs/
├── sql/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

## 5. Levantar el entorno

Desde CMD, entrar a la carpeta del proyecto:

```cmd
cd "C:\Users\gusta\OneDrive\Escritorio\Base de Datos II\PROGRAMAS\retailx-lakehouse-realtime"
```

Levantar contenedores:

```cmd
docker compose up -d --build
```

Verificar contenedores:

```cmd
docker ps
```

Deben aparecer:

```text
retailx-spark-master
retailx-spark-worker
retailx-kafka
retailx-kafka-ui
```

Panel de Spark:

```text
http://localhost:8080
```

Panel de Kafka UI:

```text
http://localhost:8081
```

## 6. Ejecutar pipeline batch

### 6.1 Generar datasets simulados

```cmd
docker exec -it retailx-spark-master bash -lc "cd /opt/spark/work-dir/app && /opt/spark/bin/spark-submit scripts/01_setup_data.py"
```

Genera:

```text
data/raw/csv/ventas
data/raw/json/web_events
data/raw/xml/productos.xml
data/raw/manifest.json
```

### 6.2 Generar capa Bronze

```cmd
docker exec -it retailx-spark-master bash -lc "cd /opt/spark/work-dir/app && /opt/spark/bin/spark-submit --packages io.delta:delta-spark_2.12:3.2.0 scripts/02_batch_bronze.py"
```

Genera:

```text
data/bronze/ventas
data/bronze/web_events
data/bronze/productos
data/bronze/audit_ingestion
```

### 6.3 Generar capa Silver

```cmd
docker exec -it retailx-spark-master bash -lc "cd /opt/spark/work-dir/app && /opt/spark/bin/spark-submit --packages io.delta:delta-spark_2.12:3.2.0 scripts/03_batch_silver.py"
```

Genera:

```text
data/silver/ventas
data/silver/web_events
data/silver/productos
data/silver/ventas_enriquecidas
data/silver/data_quality_report
```

### 6.4 Generar capa Gold batch

```cmd
docker exec -it retailx-spark-master bash -lc "cd /opt/spark/work-dir/app && /opt/spark/bin/spark-submit --packages io.delta:delta-spark_2.12:3.2.0 scripts/04_batch_gold.py"
```

Genera KPIs empresariales y exportaciones para Power BI.

## 7. Ejecutar pipeline streaming

### 7.1 Crear topic de Kafka

```cmd
docker exec -it retailx-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic gpscamiones --partitions 3 --replication-factor 1
```

Verificar topic:

```cmd
docker exec -it retailx-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

Debe aparecer:

```text
gpscamiones
```

### 7.2 Ejecutar Spark Streaming Bronze

En una terminal CMD:

```cmd
docker exec -it retailx-spark-master bash -lc "cd /opt/spark/work-dir/app && STREAMING_RUNTIME_SECONDS=120 /opt/spark/bin/spark-submit --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 scripts/06_streaming_bronze.py"
```

### 7.3 Ejecutar producer GPS

En otra terminal CMD:

```cmd
docker exec -it retailx-spark-master bash -lc "cd /opt/spark/work-dir/app && PRODUCER_MAX_EVENTS=300 PRODUCER_INTERVAL_SECONDS=0.3 python3 scripts/05_kafka_producer_gps.py"
```

Genera eventos GPS simulados de camiones con:

- Camión
- Zona
- Latitud
- Longitud
- Toneladas
- Velocidad
- Ocupación
- Estado de entrega
- Riesgo de retraso
- Entrega a tiempo

### 7.4 Generar Gold Real-Time KPIs

```cmd
docker exec -it retailx-spark-master bash -lc "cd /opt/spark/work-dir/app && /opt/spark/bin/spark-submit --packages io.delta:delta-spark_2.12:3.2.0 scripts/07_streaming_gold_kpis.py"
```

Genera:

```text
data/gold/rt_kpis_generales
data/gold/rt_kpis_por_zona
data/gold/rt_toneladas_por_hora
data/gold/rt_estado_flota
data/gold/rt_alertas_operativas
```

## 8. Exportar archivos finales para Power BI

```cmd
docker exec -it retailx-spark-master bash -lc "cd /opt/spark/work-dir/app && python3 scripts/08_export_powerbi.py"
```

Los CSV finales quedan en:

```text
data/powerbi/final
```

Archivos principales:

```text
kpis_generales.csv
ventas_mensuales.csv
cliente_mayor_volumen.csv
top10_ventas.csv
promedio_venta_cliente.csv
pareto_80_clientes.csv
resumen_pareto.csv
ventas_categoria_canal.csv
web_kpis.csv
rt_kpis_generales.csv
rt_kpis_por_zona.csv
rt_toneladas_por_hora.csv
rt_estado_flota.csv
rt_alertas_operativas.csv
powerbi_manifest.csv
```

## 9. KPIs implementados

### KPIs batch

- Total de ventas
- Clientes únicos
- Productos vendidos
- Ingreso total
- Ticket promedio
- Venta máxima
- Venta mínima
- Unidades vendidas
- Margen estimado
- Ventas mensuales
- Cliente con mayor volumen de compra
- Top 10 ventas de mayor importe
- Promedio de venta por cliente
- Análisis Pareto 80/20 de clientes
- Ventas por categoría y canal
- KPIs de eventos web

### KPIs real-time

- Total de eventos GPS
- Camiones activos
- Toneladas transportadas
- Velocidad promedio
- Tiempo promedio de espera
- Ocupación promedio
- Riesgo promedio de retraso
- Porcentaje de entregas a tiempo
- Alertas operativas
- Eventos de riesgo alto
- Toneladas por hora
- Estado de flota
- KPIs por zona logística

## 10. Evidencias recomendadas

Para la entrega se recomienda tomar capturas de:

1. Contenedores activos con `docker ps`.
2. Panel de Spark en `http://localhost:8080`.
3. Kafka UI en `http://localhost:8081`.
4. Topic `gpscamiones` creado.
5. Ejecución correcta de `01_setup_data.py`.
6. Ejecución correcta de Bronze.
7. Ejecución correcta de Silver.
8. Ejecución correcta de Gold.
9. Producer enviando eventos GPS.
10. Streaming Bronze ejecutándose.
11. Gold Real-Time KPIs generado.
12. Carpeta `data/powerbi/final` con CSV finales.
13. Dashboard o carga de archivos en Power BI.

## 11. Cómo consumir en Power BI

En Power BI Desktop:

1. Seleccionar Obtener datos.
2. Elegir Texto/CSV.
3. Buscar la carpeta:

```text
data/powerbi/final
```

4. Cargar los CSV principales.
5. Crear visualizaciones usando:

- `kpis_generales.csv`
- `ventas_mensuales.csv`
- `top10_ventas.csv`
- `pareto_80_clientes.csv`
- `rt_kpis_generales.csv`
- `rt_kpis_por_zona.csv`
- `rt_alertas_operativas.csv`

## 12. Comandos útiles

Ver contenedores:

```cmd
docker ps
```

Ver logs de Spark master:

```cmd
docker logs retailx-spark-master
```

Ver logs de Kafka:

```cmd
docker logs retailx-kafka
```

Ver logs del proyecto:

```cmd
dir logs
```

Detener contenedores:

```cmd
docker compose down
```

Levantar nuevamente:

```cmd
docker compose up -d
```

## 13. Conclusión

Este proyecto implementa una solución integral de ingeniería de datos para RetailX utilizando arquitectura Lakehouse, procesamiento batch, procesamiento streaming, Delta Lake, Kafka, Spark y exportaciones listas para Power BI.

La solución cubre el flujo completo desde fuentes crudas hasta indicadores ejecutivos, permitiendo análisis histórico de ventas y monitoreo operacional en tiempo real de la flota logística.
