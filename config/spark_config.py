import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession


DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.2.0"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"


def get_project_root() -> Path:
    return Path(os.getenv("APP_HOME", Path.cwd())).resolve()


def setup_logging(script_name: str) -> logging.Logger:
    root_path = get_project_root()
    log_dir = root_path / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(script_name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_dir / f"{script_name}.log", encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def build_spark_session(
    app_name: str,
    include_delta: bool = False,
    include_kafka: bool = False,
    shuffle_partitions: int = 8,
) -> SparkSession:
    master_url = os.getenv("SPARK_MASTER_URL", "local[*]")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master_url)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.default.parallelism", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    packages = []

    if include_delta:
        packages.append(DELTA_PACKAGE)
        builder = (
            builder
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        )

    if include_kafka:
        packages.append(KAFKA_PACKAGE)

    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))

    return builder.getOrCreate()