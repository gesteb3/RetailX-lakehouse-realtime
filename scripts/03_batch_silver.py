import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config.spark_config import build_spark_session, get_project_root, setup_logging


SCRIPT_NAME = "03_batch_silver"
logger = setup_logging(SCRIPT_NAME)


def read_delta(spark, path: Path, table_name: str):
    if not path.exists():
        raise FileNotFoundError(f"No existe la tabla {table_name}: {path}")

    logger.info("Leyendo tabla %s desde %s", table_name, path)

    return spark.read.format("delta").load(str(path))


def write_delta(df, path: Path, table_name: str, partitions=None) -> None:
    logger.info("Escribiendo tabla Silver %s en %s", table_name, path)

    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )

    if partitions:
        writer = writer.partitionBy(*partitions)

    writer.save(str(path))

    logger.info("Tabla Silver %s escrita correctamente.", table_name)


def process_silver_ventas(spark, root_path: Path):
    input_path = root_path / "data/bronze/ventas"
    output_path = root_path / "data/silver/ventas"

    df = read_delta(spark, input_path, "bronze_ventas")

    df = (
        df.select(
            F.col("id_venta").cast("long").alias("id_venta"),
            F.col("id_cliente").cast("long").alias("id_cliente"),
            F.col("id_producto").cast("long").alias("id_producto"),
            F.col("cantidad").cast("int").alias("cantidad"),
            F.col("precio_unitario").cast("double").alias("precio_unitario"),
            F.col("monto").cast("double").alias("monto"),
            F.to_date("fecha_venta").alias("fecha_venta"),
            F.trim(F.upper(F.col("canal"))).alias("canal"),
            F.trim(F.initcap(F.col("pais"))).alias("pais"),
            F.trim(F.lower(F.col("categoria"))).alias("categoria"),
            F.col("_ingestion_timestamp"),
            F.col("_source_system"),
            F.col("_source_format"),
            F.col("_batch_id"),
        )
        .filter(F.col("id_venta").isNotNull())
        .filter(F.col("id_cliente").isNotNull())
        .filter(F.col("id_producto").isNotNull())
        .filter(F.col("monto").isNotNull())
        .filter(F.col("monto") > 0)
        .filter(F.col("cantidad") > 0)
        .filter(F.col("fecha_venta").isNotNull())
    )

    window = Window.partitionBy("id_venta").orderBy(F.col("_ingestion_timestamp").desc())

    df = (
        df.withColumn("row_number", F.row_number().over(window))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
        .withColumn("anio", F.year("fecha_venta"))
        .withColumn("mes", F.month("fecha_venta"))
        .withColumn("anio_mes", F.date_format("fecha_venta", "yyyy-MM"))
        .withColumn("processed_at", F.current_timestamp())
    )

    df = df.repartition(8, "anio_mes")

    write_delta(
        df=df,
        path=output_path,
        table_name="silver_ventas",
        partitions=["anio_mes"],
    )

    return df


def process_silver_web_events(spark, root_path: Path):
    input_path = root_path / "data/bronze/web_events"
    output_path = root_path / "data/silver/web_events"

    df = read_delta(spark, input_path, "bronze_web_events")

    df = (
        df.select(
            F.col("event_id").cast("long").alias("event_id"),
            F.col("id_cliente").cast("long").alias("id_cliente"),
            F.trim(F.col("session_id")).alias("session_id"),
            F.trim(F.lower(F.col("page"))).alias("page"),
            F.trim(F.lower(F.col("event_type"))).alias("event_type"),
            F.trim(F.lower(F.col("device"))).alias("device"),
            F.to_timestamp("event_timestamp").alias("event_timestamp"),
            F.col("ingestion_source"),
            F.col("_ingestion_timestamp"),
            F.col("_source_system"),
            F.col("_source_format"),
            F.col("_batch_id"),
        )
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("id_cliente").isNotNull())
        .filter(F.col("event_timestamp").isNotNull())
    )

    window = Window.partitionBy("event_id").orderBy(F.col("_ingestion_timestamp").desc())

    df = (
        df.withColumn("row_number", F.row_number().over(window))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
        .withColumn("fecha_evento", F.to_date("event_timestamp"))
        .withColumn("anio_mes", F.date_format("event_timestamp", "yyyy-MM"))
        .withColumn("processed_at", F.current_timestamp())
    )

    df = df.repartition(4, "anio_mes")

    write_delta(
        df=df,
        path=output_path,
        table_name="silver_web_events",
        partitions=["anio_mes"],
    )

    return df


def process_silver_productos(spark, root_path: Path):
    input_path = root_path / "data/bronze/productos"
    output_path = root_path / "data/silver/productos"

    df = read_delta(spark, input_path, "bronze_productos")

    df = (
        df.select(
            F.col("id_producto").cast("long").alias("id_producto"),
            F.trim(F.col("nombre_producto")).alias("nombre_producto"),
            F.trim(F.lower(F.col("categoria"))).alias("categoria"),
            F.trim(F.col("proveedor")).alias("proveedor"),
            F.col("precio_base").cast("double").alias("precio_base"),
            F.when(F.lower(F.col("activo")) == "true", F.lit(True))
            .otherwise(F.lit(False))
            .alias("activo"),
            F.col("_ingestion_timestamp"),
            F.col("_source_system"),
            F.col("_source_format"),
            F.col("_batch_id"),
        )
        .filter(F.col("id_producto").isNotNull())
        .filter(F.col("precio_base").isNotNull())
        .filter(F.col("precio_base") > 0)
    )

    window = Window.partitionBy("id_producto").orderBy(F.col("_ingestion_timestamp").desc())

    df = (
        df.withColumn("row_number", F.row_number().over(window))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
        .withColumn("processed_at", F.current_timestamp())
    )

    df = df.coalesce(1)

    write_delta(
        df=df,
        path=output_path,
        table_name="silver_productos",
    )

    return df


def process_silver_ventas_enriquecidas(spark, root_path: Path):
    ventas_path = root_path / "data/silver/ventas"
    productos_path = root_path / "data/silver/productos"
    output_path = root_path / "data/silver/ventas_enriquecidas"

    ventas_df = read_delta(spark, ventas_path, "silver_ventas")
    productos_df = read_delta(spark, productos_path, "silver_productos")

    productos_df = productos_df.select(
        "id_producto",
        F.col("nombre_producto"),
        F.col("categoria").alias("categoria_producto"),
        F.col("proveedor"),
        F.col("precio_base"),
        F.col("activo").alias("producto_activo"),
    )

    df = (
        ventas_df.alias("v")
        .join(productos_df.alias("p"), on="id_producto", how="left")
        .withColumn(
            "categoria_final",
            F.coalesce(F.col("categoria_producto"), F.col("categoria")),
        )
        .withColumn(
            "margen_estimado",
            F.round(F.col("monto") - (F.col("precio_base") * F.col("cantidad")), 2),
        )
        .withColumn("processed_at", F.current_timestamp())
    )

    df = df.repartition(8, "anio_mes")

    write_delta(
        df=df,
        path=output_path,
        table_name="silver_ventas_enriquecidas",
        partitions=["anio_mes"],
    )

    return df


def write_quality_report(spark, root_path: Path, ventas_df, web_df, productos_df, ventas_enriquecidas_df):
    output_path = root_path / "data/silver/data_quality_report"

    report_rows = [
        {
            "layer": "silver",
            "table_name": "ventas",
            "record_count": ventas_df.count(),
            "quality_rule": "ventas limpias, monto positivo, fecha válida, sin duplicados por id_venta",
        },
        {
            "layer": "silver",
            "table_name": "web_events",
            "record_count": web_df.count(),
            "quality_rule": "eventos con id_cliente, event_id y timestamp válido",
        },
        {
            "layer": "silver",
            "table_name": "productos",
            "record_count": productos_df.count(),
            "quality_rule": "productos activos/inactivos normalizados y precio válido",
        },
        {
            "layer": "silver",
            "table_name": "ventas_enriquecidas",
            "record_count": ventas_enriquecidas_df.count(),
            "quality_rule": "ventas unidas con catálogo de productos para analítica final",
        },
    ]

    df = (
        spark.createDataFrame(report_rows)
        .withColumn("generated_at", F.current_timestamp())
    )

    (
        df.coalesce(1)
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(output_path))
    )

    logger.info("Reporte de calidad Silver escrito en %s", output_path)


def validate_outputs(spark, root_path: Path):
    tables = {
        "ventas": root_path / "data/silver/ventas",
        "web_events": root_path / "data/silver/web_events",
        "productos": root_path / "data/silver/productos",
        "ventas_enriquecidas": root_path / "data/silver/ventas_enriquecidas",
        "data_quality_report": root_path / "data/silver/data_quality_report",
    }

    for table_name, path in tables.items():
        if not path.exists():
            raise FileNotFoundError(f"No se generó la tabla Silver {table_name}")

        count = spark.read.format("delta").load(str(path)).count()
        logger.info("Validación Silver - %s: %s registros", table_name, count)

        if count == 0:
            raise ValueError(f"La tabla Silver {table_name} está vacía.")


def main() -> None:
    spark = None

    try:
        root_path = get_project_root()

        logger.info("Iniciando procesamiento Silver.")
        logger.info("Ruta raíz del proyecto: %s", root_path)

        spark = build_spark_session(
            app_name="RetailX_03_Batch_Silver",
            include_delta=True,
            include_kafka=False,
            shuffle_partitions=8,
        )

        spark.sparkContext.setLogLevel("WARN")

        ventas_df = process_silver_ventas(spark, root_path)
        web_df = process_silver_web_events(spark, root_path)
        productos_df = process_silver_productos(spark, root_path)
        ventas_enriquecidas_df = process_silver_ventas_enriquecidas(spark, root_path)

        write_quality_report(
            spark=spark,
            root_path=root_path,
            ventas_df=ventas_df,
            web_df=web_df,
            productos_df=productos_df,
            ventas_enriquecidas_df=ventas_enriquecidas_df,
        )

        validate_outputs(spark, root_path)

        logger.info("Capa Silver generada correctamente.")

    except Exception as error:
        logger.exception("Error crítico generando capa Silver: %s", error)
        raise

    finally:
        if spark is not None:
            spark.stop()
            logger.info("SparkSession cerrada correctamente.")


if __name__ == "__main__":
    main()