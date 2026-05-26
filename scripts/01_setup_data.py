import json
import os
import random
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config.spark_config import build_spark_session, get_project_root, setup_logging


SCRIPT_NAME = "01_setup_data"
logger = setup_logging(SCRIPT_NAME)


def create_directories(root_path: Path) -> None:
    directories = [
        "data/raw/csv",
        "data/raw/json",
        "data/raw/xml",
        "data/bronze",
        "data/silver",
        "data/gold",
        "data/powerbi",
        "data/checkpoints",
        "logs",
    ]

    for directory in directories:
        path = root_path / directory
        path.mkdir(parents=True, exist_ok=True)

    logger.info("Directorios base verificados correctamente.")


def generate_sales_csv(spark, root_path: Path, total_rows: int) -> None:
    output_path = root_path / "data/raw/csv/ventas"

    logger.info("Generando dataset CSV de ventas con %s registros.", total_rows)

    canales = F.array(
        F.lit("POS"),
        F.lit("WEB"),
        F.lit("APP"),
        F.lit("MARKETPLACE"),
    )

    paises = F.array(
        F.lit("Guatemala"),
        F.lit("El Salvador"),
        F.lit("Honduras"),
        F.lit("Costa Rica"),
    )

    categorias = F.array(
        F.lit("electronica"),
        F.lit("hogar"),
        F.lit("moda"),
        F.lit("deportes"),
        F.lit("supermercado"),
    )

    df = (
        spark.range(1, total_rows + 1)
        .withColumnRenamed("id", "id_venta")
        .withColumn("id_cliente", (F.rand(seed=11) * 50000 + 1).cast("int"))
        .withColumn("id_producto", (F.rand(seed=12) * 2000 + 1).cast("int"))
        .withColumn("cantidad", (F.rand(seed=13) * 5 + 1).cast("int"))
        .withColumn("precio_unitario", F.round(F.rand(seed=14) * 490 + 10, 2))
        .withColumn("monto", F.round(F.col("cantidad") * F.col("precio_unitario"), 2))
        .withColumn(
            "fecha_venta",
            F.date_add(
                F.to_date(F.lit("2026-01-01")),
                (F.rand(seed=15) * 180).cast("int"),
            ),
        )
        .withColumn(
            "canal",
            F.element_at(canales, (F.rand(seed=16) * 4 + 1).cast("int")),
        )
        .withColumn(
            "pais",
            F.element_at(paises, (F.rand(seed=17) * 4 + 1).cast("int")),
        )
        .withColumn(
            "categoria",
            F.element_at(categorias, (F.rand(seed=18) * 5 + 1).cast("int")),
        )
        .withColumn("created_at", F.current_timestamp())
    )

    (
        df.repartition(8)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(str(output_path))
    )

    logger.info("Dataset CSV generado en: %s", output_path)


def generate_web_events_json(spark, root_path: Path, total_rows: int) -> None:
    event_rows = min(total_rows, 250000)
    output_path = root_path / "data/raw/json/web_events"

    logger.info("Generando dataset JSON de eventos web con %s registros.", event_rows)

    pages = F.array(
        F.lit("/home"),
        F.lit("/producto"),
        F.lit("/carrito"),
        F.lit("/checkout"),
        F.lit("/login"),
    )

    events = F.array(
        F.lit("page_view"),
        F.lit("add_to_cart"),
        F.lit("remove_from_cart"),
        F.lit("purchase_attempt"),
        F.lit("login"),
    )

    devices = F.array(
        F.lit("mobile"),
        F.lit("desktop"),
        F.lit("tablet"),
    )

    df = (
        spark.range(1, event_rows + 1)
        .withColumnRenamed("id", "event_id")
        .withColumn("id_cliente", (F.rand(seed=21) * 50000 + 1).cast("int"))
        .withColumn("session_id", F.concat(F.lit("sess_"), F.col("event_id")))
        .withColumn(
            "page",
            F.element_at(pages, (F.rand(seed=22) * 5 + 1).cast("int")),
        )
        .withColumn(
            "event_type",
            F.element_at(events, (F.rand(seed=23) * 5 + 1).cast("int")),
        )
        .withColumn(
            "device",
            F.element_at(devices, (F.rand(seed=24) * 3 + 1).cast("int")),
        )
        .withColumn(
            "event_timestamp",
            F.expr("timestampadd(MINUTE, cast(rand(25) * 250000 as int), timestamp('2026-01-01 00:00:00'))"),
        )
        .withColumn("ingestion_source", F.lit("web_api"))
    )

    (
        df.repartition(4)
        .write
        .mode("overwrite")
        .json(str(output_path))
    )

    logger.info("Dataset JSON generado en: %s", output_path)


def generate_products_xml(root_path: Path, total_products: int = 2000) -> None:
    output_path = root_path / "data/raw/xml/productos.xml"

    logger.info("Generando dataset XML de productos con %s registros.", total_products)

    categorias = ["electronica", "hogar", "moda", "deportes", "supermercado"]
    proveedores = ["Proveedor_A", "Proveedor_B", "Proveedor_C", "Proveedor_D"]

    root = ET.Element("productos")

    for product_id in range(1, total_products + 1):
        product = ET.SubElement(root, "producto")

        ET.SubElement(product, "id_producto").text = str(product_id)
        ET.SubElement(product, "nombre_producto").text = f"Producto_{product_id}"
        ET.SubElement(product, "categoria").text = random.choice(categorias)
        ET.SubElement(product, "proveedor").text = random.choice(proveedores)
        ET.SubElement(product, "precio_base").text = str(round(random.uniform(10, 500), 2))
        ET.SubElement(product, "activo").text = "true"

    tree = ET.ElementTree(root)
    tree.write(output_path, encoding="utf-8", xml_declaration=True)

    logger.info("Dataset XML generado en: %s", output_path)


def write_dataset_manifest(root_path: Path, total_rows: int) -> None:
    manifest_path = root_path / "data/raw/manifest.json"

    manifest = {
        "project": "RetailX Lakehouse Real-Time",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "datasets": [
            {
                "name": "ventas",
                "format": "csv",
                "path": "data/raw/csv/ventas",
                "rows": total_rows,
                "description": "Transacciones simuladas de ERP/POS para análisis Lakehouse.",
            },
            {
                "name": "web_events",
                "format": "json",
                "path": "data/raw/json/web_events",
                "rows": min(total_rows, 250000),
                "description": "Eventos semiestructurados de Web/API.",
            },
            {
                "name": "productos",
                "format": "xml",
                "path": "data/raw/xml/productos.xml",
                "rows": 2000,
                "description": "Catálogo maestro de productos en XML.",
            },
        ],
    }

    with open(manifest_path, "w", encoding="utf-8") as file:
        json.dump(manifest, file, indent=4, ensure_ascii=False)

    logger.info("Manifest generado en: %s", manifest_path)


def main() -> None:
    spark = None

    try:
        root_path = get_project_root()
        total_rows = int(os.getenv("RETAILX_ROWS", "1000000"))

        logger.info("Iniciando generación de datos RetailX.")
        logger.info("Ruta raíz del proyecto: %s", root_path)
        logger.info("Total de registros principales: %s", total_rows)

        create_directories(root_path)

        spark = build_spark_session(
            app_name="RetailX_01_Setup_Data",
            include_kafka=False,
            shuffle_partitions=8,
        )

        spark.sparkContext.setLogLevel("WARN")

        generate_sales_csv(spark, root_path, total_rows)
        generate_web_events_json(spark, root_path, total_rows)
        generate_products_xml(root_path)
        write_dataset_manifest(root_path, total_rows)

        logger.info("Generación de datasets finalizada correctamente.")

    except Exception as error:
        logger.exception("Error crítico generando datasets: %s", error)
        raise

    finally:
        if spark is not None:
            spark.stop()
            logger.info("SparkSession cerrada correctamente.")


if __name__ == "__main__":
    main()