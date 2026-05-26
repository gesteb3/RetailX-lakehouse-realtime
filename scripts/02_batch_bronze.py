import sys
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config.spark_config import build_spark_session, get_project_root, setup_logging


SCRIPT_NAME = "02_batch_bronze"
logger = setup_logging(SCRIPT_NAME)


def add_bronze_metadata(df, source_system: str, source_format: str, batch_id: str):
    return (
        df.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_source_format", F.lit(source_format))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def write_delta_table(df, output_path: Path, table_name: str) -> None:
    logger.info("Escribiendo tabla Bronze: %s", table_name)

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(output_path))
    )

    logger.info("Tabla %s escrita correctamente en %s", table_name, output_path)


def process_sales_csv(spark, root_path: Path, batch_id: str) -> None:
    input_path = root_path / "data/raw/csv/ventas"
    output_path = root_path / "data/bronze/ventas"

    logger.info("Leyendo ventas CSV desde %s", input_path)

    if not input_path.exists():
        raise FileNotFoundError(f"No existe el dataset CSV de ventas: {input_path}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(str(input_path))
    )

    df = add_bronze_metadata(
        df=df,
        source_system="ERP_POS",
        source_format="CSV",
        batch_id=batch_id,
    )

    df = df.repartition(8)

    write_delta_table(
        df=df,
        output_path=output_path,
        table_name="bronze_ventas",
    )


def process_web_events_json(spark, root_path: Path, batch_id: str) -> None:
    input_path = root_path / "data/raw/json/web_events"
    output_path = root_path / "data/bronze/web_events"

    logger.info("Leyendo eventos web JSON desde %s", input_path)

    if not input_path.exists():
        raise FileNotFoundError(f"No existe el dataset JSON web_events: {input_path}")

    df = spark.read.json(str(input_path))

    df = add_bronze_metadata(
        df=df,
        source_system="WEB_API",
        source_format="JSON",
        batch_id=batch_id,
    )

    df = df.repartition(4)

    write_delta_table(
        df=df,
        output_path=output_path,
        table_name="bronze_web_events",
    )


def read_products_xml(root_path: Path):
    input_path = root_path / "data/raw/xml/productos.xml"

    logger.info("Leyendo productos XML desde %s", input_path)

    if not input_path.exists():
        raise FileNotFoundError(f"No existe el archivo XML de productos: {input_path}")

    tree = ET.parse(input_path)
    root = tree.getroot()

    records = []

    for product in root.findall("producto"):
        records.append(
            {
                "id_producto": product.findtext("id_producto"),
                "nombre_producto": product.findtext("nombre_producto"),
                "categoria": product.findtext("categoria"),
                "proveedor": product.findtext("proveedor"),
                "precio_base": product.findtext("precio_base"),
                "activo": product.findtext("activo"),
            }
        )

    return records


def process_products_xml(spark, root_path: Path, batch_id: str) -> None:
    output_path = root_path / "data/bronze/productos"

    records = read_products_xml(root_path)

    schema = StructType(
        [
            StructField("id_producto", StringType(), True),
            StructField("nombre_producto", StringType(), True),
            StructField("categoria", StringType(), True),
            StructField("proveedor", StringType(), True),
            StructField("precio_base", StringType(), True),
            StructField("activo", StringType(), True),
        ]
    )

    rows = [Row(**record) for record in records]
    df = spark.createDataFrame(rows, schema=schema)

    df = add_bronze_metadata(
        df=df,
        source_system="MASTER_DATA",
        source_format="XML",
        batch_id=batch_id,
    )

    df = df.coalesce(1)

    write_delta_table(
        df=df,
        output_path=output_path,
        table_name="bronze_productos",
    )


def write_bronze_audit(spark, root_path: Path, batch_id: str) -> None:
    output_path = root_path / "data/bronze/audit_ingestion"

    audit_rows = [
        {
            "batch_id": batch_id,
            "layer": "bronze",
            "dataset": "ventas",
            "source_format": "CSV",
            "target_path": "data/bronze/ventas",
        },
        {
            "batch_id": batch_id,
            "layer": "bronze",
            "dataset": "web_events",
            "source_format": "JSON",
            "target_path": "data/bronze/web_events",
        },
        {
            "batch_id": batch_id,
            "layer": "bronze",
            "dataset": "productos",
            "source_format": "XML",
            "target_path": "data/bronze/productos",
        },
    ]

    df = (
        spark.createDataFrame(audit_rows)
        .withColumn("processed_at", F.current_timestamp())
    )

    (
        df.coalesce(1)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(output_path))
    )

    logger.info("Auditoría Bronze escrita en %s", output_path)


def validate_bronze_outputs(spark, root_path: Path) -> None:
    datasets = {
        "ventas": root_path / "data/bronze/ventas",
        "web_events": root_path / "data/bronze/web_events",
        "productos": root_path / "data/bronze/productos",
        "audit_ingestion": root_path / "data/bronze/audit_ingestion",
    }

    for name, path in datasets.items():
        if not path.exists():
            raise FileNotFoundError(f"No se generó la tabla Bronze: {name}")

        count = spark.read.format("delta").load(str(path)).count()
        logger.info("Validación Bronze - %s: %s registros", name, count)

        if count == 0:
            raise ValueError(f"La tabla Bronze {name} quedó vacía.")


def main() -> None:
    spark = None

    try:
        root_path = get_project_root()
        batch_id = str(uuid.uuid4())

        logger.info("Iniciando ingesta Bronze.")
        logger.info("Ruta raíz del proyecto: %s", root_path)
        logger.info("Batch ID: %s", batch_id)

        spark = build_spark_session(
            app_name="RetailX_02_Batch_Bronze",
            include_delta=True,
            include_kafka=False,
            shuffle_partitions=8,
        )

        spark.sparkContext.setLogLevel("WARN")

        process_sales_csv(spark, root_path, batch_id)
        process_web_events_json(spark, root_path, batch_id)
        process_products_xml(spark, root_path, batch_id)
        write_bronze_audit(spark, root_path, batch_id)
        validate_bronze_outputs(spark, root_path)

        logger.info("Capa Bronze generada correctamente.")

    except Exception as error:
        logger.exception("Error crítico generando capa Bronze: %s", error)
        raise

    finally:
        if spark is not None:
            spark.stop()
            logger.info("SparkSession cerrada correctamente.")


if __name__ == "__main__":
    main()