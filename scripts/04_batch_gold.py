import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config.spark_config import build_spark_session, get_project_root, setup_logging


SCRIPT_NAME = "04_batch_gold"
logger = setup_logging(SCRIPT_NAME)


def read_delta(spark, path: Path, table_name: str) -> DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"No existe la tabla requerida {table_name}: {path}")

    logger.info("Leyendo tabla %s desde %s", table_name, path)

    return spark.read.format("delta").load(str(path))


def write_delta(df: DataFrame, path: Path, table_name: str) -> None:
    logger.info("Escribiendo tabla Gold %s en %s", table_name, path)

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(path))
    )

    logger.info("Tabla Gold %s escrita correctamente.", table_name)


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


def build_kpis_generales(ventas_df: DataFrame) -> DataFrame:
    return (
        ventas_df.agg(
            F.countDistinct("id_venta").alias("total_ventas"),
            F.countDistinct("id_cliente").alias("clientes_unicos"),
            F.countDistinct("id_producto").alias("productos_vendidos"),
            F.round(F.sum("monto"), 2).alias("ingreso_total"),
            F.round(F.avg("monto"), 2).alias("ticket_promedio"),
            F.round(F.max("monto"), 2).alias("venta_maxima"),
            F.round(F.min("monto"), 2).alias("venta_minima"),
            F.round(F.sum("cantidad"), 0).alias("unidades_vendidas"),
            F.round(F.avg("margen_estimado"), 2).alias("margen_promedio_estimado"),
        )
        .withColumn("generated_at", F.current_timestamp())
    )


def build_ventas_mensuales(ventas_df: DataFrame) -> DataFrame:
    return (
        ventas_df.groupBy("anio_mes")
        .agg(
            F.countDistinct("id_venta").alias("total_ventas"),
            F.countDistinct("id_cliente").alias("clientes_unicos"),
            F.round(F.sum("monto"), 2).alias("ingreso_total"),
            F.round(F.avg("monto"), 2).alias("ticket_promedio"),
            F.round(F.sum("cantidad"), 0).alias("unidades_vendidas"),
            F.round(F.sum("margen_estimado"), 2).alias("margen_estimado_total"),
        )
        .withColumn("generated_at", F.current_timestamp())
        .orderBy("anio_mes")
    )


def build_cliente_mayor_volumen(ventas_df: DataFrame) -> DataFrame:
    clientes_df = (
        ventas_df.groupBy("id_cliente")
        .agg(
            F.countDistinct("id_venta").alias("cantidad_compras"),
            F.round(F.sum("monto"), 2).alias("total_comprado"),
            F.round(F.avg("monto"), 2).alias("ticket_promedio_cliente"),
            F.round(F.sum("cantidad"), 0).alias("unidades_compradas"),
        )
    )

    window = Window.orderBy(F.col("total_comprado").desc())

    return (
        clientes_df
        .withColumn("ranking_cliente", F.row_number().over(window))
        .filter(F.col("ranking_cliente") == 1)
        .withColumn("generated_at", F.current_timestamp())
    )


def build_top10_ventas(ventas_df: DataFrame) -> DataFrame:
    window = Window.orderBy(F.col("monto").desc())

    return (
        ventas_df.select(
            "id_venta",
            "id_cliente",
            "id_producto",
            "nombre_producto",
            "categoria_final",
            "canal",
            "pais",
            "fecha_venta",
            "cantidad",
            "precio_unitario",
            "monto",
        )
        .withColumn("ranking_venta", F.row_number().over(window))
        .filter(F.col("ranking_venta") <= 10)
        .withColumn("generated_at", F.current_timestamp())
    )


def build_promedio_venta_cliente(ventas_df: DataFrame) -> DataFrame:
    return (
        ventas_df.groupBy("id_cliente")
        .agg(
            F.countDistinct("id_venta").alias("cantidad_ventas"),
            F.round(F.sum("monto"), 2).alias("total_comprado"),
            F.round(F.avg("monto"), 2).alias("monto_promedio_venta"),
            F.round(F.max("monto"), 2).alias("mayor_compra"),
            F.round(F.min("monto"), 2).alias("menor_compra"),
        )
        .withColumn("generated_at", F.current_timestamp())
        .orderBy(F.col("total_comprado").desc())
    )


def build_pareto_80_clientes(ventas_df: DataFrame) -> DataFrame:
    clientes_df = (
        ventas_df.groupBy("id_cliente")
        .agg(
            F.countDistinct("id_venta").alias("cantidad_ventas"),
            F.round(F.sum("monto"), 2).alias("total_comprado"),
        )
    )

    total_window = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    ranking_window = Window.orderBy(F.col("total_comprado").desc())

    pareto_df = (
        clientes_df
        .withColumn("ingreso_global", F.sum("total_comprado").over(total_window))
        .withColumn("ranking_cliente", F.row_number().over(ranking_window))
        .withColumn(
            "ingreso_acumulado",
            F.round(F.sum("total_comprado").over(ranking_window), 2),
        )
        .withColumn(
            "porcentaje_acumulado",
            F.round(F.col("ingreso_acumulado") / F.col("ingreso_global"), 4),
        )
        .withColumn(
            "porcentaje_acumulado_label",
            F.concat(F.round(F.col("porcentaje_acumulado") * 100, 2), F.lit("%")),
        )
        .withColumn(
            "segmento_pareto",
            F.when(F.col("porcentaje_acumulado") <= 0.80, F.lit("Clientes que concentran el 80% del ingreso"))
            .otherwise(F.lit("Resto de clientes")),
        )
        .withColumn("generated_at", F.current_timestamp())
    )

    return pareto_df


def build_resumen_pareto(pareto_df: DataFrame) -> DataFrame:
    return (
        pareto_df.groupBy("segmento_pareto")
        .agg(
            F.countDistinct("id_cliente").alias("cantidad_clientes"),
            F.round(F.sum("total_comprado"), 2).alias("ingreso_segmento"),
            F.round(F.max("porcentaje_acumulado") * 100, 2).alias("porcentaje_acumulado_maximo"),
        )
        .withColumn("generated_at", F.current_timestamp())
        .orderBy(F.col("ingreso_segmento").desc())
    )


def build_ventas_categoria_canal(ventas_df: DataFrame) -> DataFrame:
    return (
        ventas_df.groupBy("categoria_final", "canal")
        .agg(
            F.countDistinct("id_venta").alias("total_ventas"),
            F.countDistinct("id_cliente").alias("clientes_unicos"),
            F.round(F.sum("monto"), 2).alias("ingreso_total"),
            F.round(F.avg("monto"), 2).alias("ticket_promedio"),
            F.round(F.sum("cantidad"), 0).alias("unidades_vendidas"),
        )
        .withColumn("generated_at", F.current_timestamp())
        .orderBy(F.col("ingreso_total").desc())
    )


def build_web_kpis(web_df: DataFrame) -> DataFrame:
    return (
        web_df.groupBy("anio_mes", "event_type", "device")
        .agg(
            F.countDistinct("event_id").alias("total_eventos"),
            F.countDistinct("id_cliente").alias("clientes_unicos"),
            F.countDistinct("session_id").alias("sesiones_unicas"),
        )
        .withColumn("generated_at", F.current_timestamp())
        .orderBy("anio_mes", "event_type", "device")
    )


def build_gold_catalog() -> list:
    return [
        {
            "table_name": "gold_kpis_generales",
            "description": "Resumen ejecutivo: ventas, clientes, ingreso total, ticket promedio y margen estimado.",
            "powerbi_path": "data/powerbi/kpis_generales",
        },
        {
            "table_name": "gold_ventas_mensuales",
            "description": "Ingresos, ventas, clientes y margen estimado por mes.",
            "powerbi_path": "data/powerbi/ventas_mensuales",
        },
        {
            "table_name": "gold_cliente_mayor_volumen",
            "description": "Cliente con mayor volumen total de compra.",
            "powerbi_path": "data/powerbi/cliente_mayor_volumen",
        },
        {
            "table_name": "gold_top10_ventas",
            "description": "Top 10 ventas individuales de mayor importe.",
            "powerbi_path": "data/powerbi/top10_ventas",
        },
        {
            "table_name": "gold_promedio_venta_cliente",
            "description": "Monto promedio de venta por cliente y ranking de clientes.",
            "powerbi_path": "data/powerbi/promedio_venta_cliente",
        },
        {
            "table_name": "gold_pareto_80_clientes",
            "description": "Análisis Pareto para identificar clientes que concentran el 80% del ingreso.",
            "powerbi_path": "data/powerbi/pareto_80_clientes",
        },
        {
            "table_name": "gold_resumen_pareto",
            "description": "Resumen ejecutivo del análisis Pareto.",
            "powerbi_path": "data/powerbi/resumen_pareto",
        },
        {
            "table_name": "gold_ventas_categoria_canal",
            "description": "Ingresos y ventas por categoría y canal comercial.",
            "powerbi_path": "data/powerbi/ventas_categoria_canal",
        },
        {
            "table_name": "gold_web_kpis",
            "description": "Eventos web agregados por mes, tipo de evento y dispositivo.",
            "powerbi_path": "data/powerbi/web_kpis",
        },
    ]


def write_gold_catalog(spark, root_path: Path) -> None:
    catalog_df = (
        spark.createDataFrame(build_gold_catalog())
        .withColumn("layer", F.lit("gold"))
        .withColumn("generated_at", F.current_timestamp())
    )

    write_delta(
        df=catalog_df,
        path=root_path / "data/gold/catalogo_gold",
        table_name="gold_catalogo",
    )

    write_powerbi_csv(
        df=catalog_df,
        path=root_path / "data/powerbi/catalogo_gold",
        dataset_name="catalogo_gold",
    )


def validate_gold_outputs(spark, root_path: Path) -> None:
    tables = {
        "kpis_generales": root_path / "data/gold/kpis_generales",
        "ventas_mensuales": root_path / "data/gold/ventas_mensuales",
        "cliente_mayor_volumen": root_path / "data/gold/cliente_mayor_volumen",
        "top10_ventas": root_path / "data/gold/top10_ventas",
        "promedio_venta_cliente": root_path / "data/gold/promedio_venta_cliente",
        "pareto_80_clientes": root_path / "data/gold/pareto_80_clientes",
        "resumen_pareto": root_path / "data/gold/resumen_pareto",
        "ventas_categoria_canal": root_path / "data/gold/ventas_categoria_canal",
        "web_kpis": root_path / "data/gold/web_kpis",
        "catalogo_gold": root_path / "data/gold/catalogo_gold",
    }

    for table_name, path in tables.items():
        if not path.exists():
            raise FileNotFoundError(f"No se generó la tabla Gold {table_name}")

        count = spark.read.format("delta").load(str(path)).count()
        logger.info("Validación Gold - %s: %s registros", table_name, count)

        if count == 0:
            raise ValueError(f"La tabla Gold {table_name} está vacía.")


def main() -> None:
    spark = None

    try:
        root_path = get_project_root()

        logger.info("Iniciando procesamiento Gold.")
        logger.info("Ruta raíz del proyecto: %s", root_path)

        spark = build_spark_session(
            app_name="RetailX_04_Batch_Gold",
            include_delta=True,
            include_kafka=False,
            shuffle_partitions=8,
        )

        spark.sparkContext.setLogLevel("WARN")

        ventas_df = read_delta(
            spark=spark,
            path=root_path / "data/silver/ventas_enriquecidas",
            table_name="silver_ventas_enriquecidas",
        ).cache()

        web_df = read_delta(
            spark=spark,
            path=root_path / "data/silver/web_events",
            table_name="silver_web_events",
        ).cache()

        kpis_generales_df = build_kpis_generales(ventas_df)
        ventas_mensuales_df = build_ventas_mensuales(ventas_df)
        cliente_mayor_df = build_cliente_mayor_volumen(ventas_df)
        top10_ventas_df = build_top10_ventas(ventas_df)
        promedio_cliente_df = build_promedio_venta_cliente(ventas_df)
        pareto_df = build_pareto_80_clientes(ventas_df)
        resumen_pareto_df = build_resumen_pareto(pareto_df)
        ventas_categoria_canal_df = build_ventas_categoria_canal(ventas_df)
        web_kpis_df = build_web_kpis(web_df)

        gold_outputs = [
            ("kpis_generales", kpis_generales_df),
            ("ventas_mensuales", ventas_mensuales_df),
            ("cliente_mayor_volumen", cliente_mayor_df),
            ("top10_ventas", top10_ventas_df),
            ("promedio_venta_cliente", promedio_cliente_df),
            ("pareto_80_clientes", pareto_df),
            ("resumen_pareto", resumen_pareto_df),
            ("ventas_categoria_canal", ventas_categoria_canal_df),
            ("web_kpis", web_kpis_df),
        ]

        for table_name, df in gold_outputs:
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

        write_gold_catalog(spark, root_path)
        validate_gold_outputs(spark, root_path)

        ventas_df.unpersist()
        web_df.unpersist()

        logger.info("Capa Gold generada correctamente.")

    except Exception as error:
        logger.exception("Error crítico generando capa Gold: %s", error)
        raise

    finally:
        if spark is not None:
            spark.stop()
            logger.info("SparkSession cerrada correctamente.")


if __name__ == "__main__":
    main()