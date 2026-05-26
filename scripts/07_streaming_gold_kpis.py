import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config.spark_config import build_spark_session, get_project_root, setup_logging


SCRIPT_NAME = "07_streaming_gold_kpis"
logger = setup_logging(SCRIPT_NAME)


def read_delta(spark, path: Path, table_name: str) -> DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"No existe la tabla requerida {table_name}: {path}")

    logger.info("Leyendo tabla %s desde %s", table_name, path)

    return spark.read.format("delta").load(str(path))


def write_delta(df: DataFrame, path: Path, table_name: str) -> None:
    logger.info("Escribiendo tabla Gold Streaming %s en %s", table_name, path)

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(path))
    )

    logger.info("Tabla %s escrita correctamente.", table_name)


def write_powerbi_csv(df: DataFrame, path: Path, dataset_name: str) -> None:
    logger.info("Exportando %s para Power BI en %s", dataset_name, path)

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .option("encoding", "UTF-8")
        .csv(str(path))
    )

    logger.info("Exportación Power BI finalizada: %s", dataset_name)


def prepare_gps_df(gps_df: DataFrame) -> DataFrame:
    return (
        gps_df
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("camion").isNotNull())
        .filter(F.col("timestamp_evento").isNotNull())
        .withColumn("fecha_evento", F.to_date("timestamp_evento"))
        .withColumn("hora_evento", F.hour("timestamp_evento"))
        .withColumn("anio_mes", F.date_format("timestamp_evento", "yyyy-MM"))
        .withColumn(
            "nivel_riesgo",
            F.when(F.col("riesgo_retraso") >= 0.75, F.lit("ALTO"))
            .when(F.col("riesgo_retraso") >= 0.50, F.lit("MEDIO"))
            .otherwise(F.lit("BAJO")),
        )
        .withColumn(
            "alerta_operativa",
            F.when(F.col("riesgo_retraso") >= 0.75, F.lit(True))
            .when(F.col("tiempo_espera_min") >= 60, F.lit(True))
            .when((F.col("velocidad") <= 10) & (F.col("estado_entrega") == "EN_RUTA"), F.lit(True))
            .otherwise(F.lit(False)),
        )
    )


def build_kpis_realtime(gps_df: DataFrame) -> DataFrame:
    return (
        gps_df.agg(
            F.count("*").alias("total_eventos_gps"),
            F.countDistinct("camion").alias("camiones_activos"),
            F.round(F.sum("toneladas"), 2).alias("toneladas_transportadas"),
            F.round(F.avg("velocidad"), 2).alias("velocidad_promedio"),
            F.round(F.avg("tiempo_espera_min"), 2).alias("tiempo_espera_promedio_min"),
            F.round(F.avg("ocupacion_pct"), 2).alias("ocupacion_promedio_pct"),
            F.round(F.avg("riesgo_retraso") * 100, 2).alias("riesgo_retraso_promedio_pct"),
            F.round(
                (F.sum(F.when(F.col("entrega_a_tiempo") == True, 1).otherwise(0)) / F.count("*")) * 100,
                2,
            ).alias("entregas_a_tiempo_pct"),
            F.sum(F.when(F.col("alerta_operativa") == True, 1).otherwise(0)).alias("total_alertas_operativas"),
            F.sum(F.when(F.col("nivel_riesgo") == "ALTO", 1).otherwise(0)).alias("eventos_riesgo_alto"),
        )
        .withColumn("generated_at", F.current_timestamp())
    )


def build_kpis_por_zona(gps_df: DataFrame) -> DataFrame:
    return (
        gps_df.groupBy("zona")
        .agg(
            F.count("*").alias("total_eventos"),
            F.countDistinct("camion").alias("camiones_activos"),
            F.round(F.sum("toneladas"), 2).alias("toneladas_transportadas"),
            F.round(F.avg("velocidad"), 2).alias("velocidad_promedio"),
            F.round(F.avg("tiempo_espera_min"), 2).alias("tiempo_espera_promedio_min"),
            F.round(F.avg("ocupacion_pct"), 2).alias("ocupacion_promedio_pct"),
            F.round(F.avg("riesgo_retraso") * 100, 2).alias("riesgo_promedio_pct"),
            F.sum(F.when(F.col("alerta_operativa") == True, 1).otherwise(0)).alias("alertas_operativas"),
            F.round(
                (F.sum(F.when(F.col("entrega_a_tiempo") == True, 1).otherwise(0)) / F.count("*")) * 100,
                2,
            ).alias("entregas_a_tiempo_pct"),
        )
        .withColumn("generated_at", F.current_timestamp())
        .orderBy(F.col("alertas_operativas").desc(), F.col("riesgo_promedio_pct").desc())
    )


def build_toneladas_por_hora(gps_df: DataFrame) -> DataFrame:
    return (
        gps_df.groupBy("fecha_evento", "hora_evento")
        .agg(
            F.count("*").alias("total_eventos"),
            F.countDistinct("camion").alias("camiones_activos"),
            F.round(F.sum("toneladas"), 2).alias("toneladas_por_hora"),
            F.round(F.avg("velocidad"), 2).alias("velocidad_promedio"),
            F.round(F.avg("ocupacion_pct"), 2).alias("ocupacion_promedio_pct"),
        )
        .withColumn("generated_at", F.current_timestamp())
        .orderBy("fecha_evento", "hora_evento")
    )


def build_estado_flota(gps_df: DataFrame) -> DataFrame:
    return (
        gps_df.groupBy("estado_entrega")
        .agg(
            F.count("*").alias("total_eventos"),
            F.countDistinct("camion").alias("camiones"),
            F.round(F.sum("toneladas"), 2).alias("toneladas"),
            F.round(F.avg("tiempo_espera_min"), 2).alias("tiempo_espera_promedio_min"),
        )
        .withColumn("generated_at", F.current_timestamp())
        .orderBy(F.col("total_eventos").desc())
    )


def build_alertas_operativas(gps_df: DataFrame) -> DataFrame:
    return (
        gps_df
        .filter(F.col("alerta_operativa") == True)
        .select(
            "event_id",
            "timestamp_evento",
            "camion",
            "zona",
            "lat",
            "lon",
            "toneladas",
            "capacidad_toneladas",
            "ocupacion_pct",
            "velocidad",
            "estado_entrega",
            "tiempo_espera_min",
            F.round(F.col("riesgo_retraso") * 100, 2).alias("riesgo_retraso_pct"),
            "nivel_riesgo",
            "entrega_a_tiempo",
        )
        .withColumn(
            "tipo_alerta",
            F.when(F.col("riesgo_retraso_pct") >= 75, F.lit("RIESGO_ALTO_RETRASO"))
            .when(F.col("tiempo_espera_min") >= 60, F.lit("ESPERA_PROLONGADA"))
            .when((F.col("velocidad") <= 10) & (F.col("estado_entrega") == "EN_RUTA"), F.lit("POSIBLE_DETENCION_EN_RUTA"))
            .otherwise(F.lit("ALERTA_OPERATIVA")),
        )
        .withColumn("generated_at", F.current_timestamp())
        .orderBy(F.col("riesgo_retraso_pct").desc(), F.col("tiempo_espera_min").desc())
    )


def build_catalogo_realtime(spark) -> DataFrame:
    rows = [
        {
            "table_name": "gold_rt_kpis_generales",
            "description": "KPIs ejecutivos de flota: camiones activos, toneladas, velocidad, ocupación y alertas.",
            "powerbi_path": "data/powerbi/rt_kpis_generales",
        },
        {
            "table_name": "gold_rt_kpis_por_zona",
            "description": "Indicadores por zona logística para detectar rutas críticas.",
            "powerbi_path": "data/powerbi/rt_kpis_por_zona",
        },
        {
            "table_name": "gold_rt_toneladas_por_hora",
            "description": "Toneladas transportadas por hora para análisis operacional.",
            "powerbi_path": "data/powerbi/rt_toneladas_por_hora",
        },
        {
            "table_name": "gold_rt_estado_flota",
            "description": "Distribución de eventos por estado de entrega.",
            "powerbi_path": "data/powerbi/rt_estado_flota",
        },
        {
            "table_name": "gold_rt_alertas_operativas",
            "description": "Alertas por riesgo alto, espera prolongada o posible detención en ruta.",
            "powerbi_path": "data/powerbi/rt_alertas_operativas",
        },
    ]

    return (
        spark.createDataFrame(rows)
        .withColumn("layer", F.lit("gold_realtime"))
        .withColumn("generated_at", F.current_timestamp())
    )


def validate_outputs(spark, root_path: Path) -> None:
    tables = {
        "rt_kpis_generales": root_path / "data/gold/rt_kpis_generales",
        "rt_kpis_por_zona": root_path / "data/gold/rt_kpis_por_zona",
        "rt_toneladas_por_hora": root_path / "data/gold/rt_toneladas_por_hora",
        "rt_estado_flota": root_path / "data/gold/rt_estado_flota",
        "rt_alertas_operativas": root_path / "data/gold/rt_alertas_operativas",
        "rt_catalogo": root_path / "data/gold/rt_catalogo",
    }

    for table_name, path in tables.items():
        if not path.exists():
            raise FileNotFoundError(f"No se generó la tabla Gold Streaming {table_name}")

        count = spark.read.format("delta").load(str(path)).count()
        logger.info("Validación Gold Streaming - %s: %s registros", table_name, count)

        if table_name != "rt_alertas_operativas" and count == 0:
            raise ValueError(f"La tabla Gold Streaming {table_name} está vacía.")


def main() -> None:
    spark = None

    try:
        root_path = get_project_root()

        logger.info("Iniciando generación de Gold Streaming KPIs.")
        logger.info("Ruta raíz del proyecto: %s", root_path)

        spark = build_spark_session(
            app_name="RetailX_07_Streaming_Gold_KPIs",
            include_delta=True,
            include_kafka=False,
            shuffle_partitions=4,
        )

        spark.sparkContext.setLogLevel("WARN")

        bronze_stream_path = root_path / "data/bronze/gps_camiones_streaming"

        gps_df = read_delta(
            spark=spark,
            path=bronze_stream_path,
            table_name="bronze_gps_camiones_streaming",
        )

        gps_df = prepare_gps_df(gps_df).cache()

        kpis_generales_df = build_kpis_realtime(gps_df)
        kpis_zona_df = build_kpis_por_zona(gps_df)
        toneladas_hora_df = build_toneladas_por_hora(gps_df)
        estado_flota_df = build_estado_flota(gps_df)
        alertas_df = build_alertas_operativas(gps_df)
        catalogo_df = build_catalogo_realtime(spark)

        outputs = [
            ("rt_kpis_generales", kpis_generales_df),
            ("rt_kpis_por_zona", kpis_zona_df),
            ("rt_toneladas_por_hora", toneladas_hora_df),
            ("rt_estado_flota", estado_flota_df),
            ("rt_alertas_operativas", alertas_df),
            ("rt_catalogo", catalogo_df),
        ]

        for table_name, df in outputs:
            write_delta(
                df=df,
                path=root_path / f"data/gold/{table_name}",
                table_name=f"gold_{table_name}",
            )

            write_powerbi_csv(
                df=df,
                path=root_path / f"data/powerbi/{table_name}",
                dataset_name=table_name,
            )

        validate_outputs(spark, root_path)

        gps_df.unpersist()

        logger.info("Gold Streaming KPIs generado correctamente.")

    except Exception as error:
        logger.exception("Error crítico generando Gold Streaming KPIs: %s", error)
        raise

    finally:
        if spark is not None:
            spark.stop()
            logger.info("SparkSession cerrada correctamente.")


if __name__ == "__main__":
    main()