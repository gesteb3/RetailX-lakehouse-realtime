import os
import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config.spark_config import build_spark_session, get_project_root, setup_logging


SCRIPT_NAME = "06_streaming_bronze"
logger = setup_logging(SCRIPT_NAME)

TOPIC_NAME = os.getenv("KAFKA_TOPIC", "gpscamiones")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
STREAMING_RUNTIME_SECONDS = int(os.getenv("STREAMING_RUNTIME_SECONDS", "120"))


def get_gps_schema() -> StructType:
    return StructType(
        [
            StructField("event_id", IntegerType(), True),
            StructField("timestamp_evento", StringType(), True),
            StructField("camion", StringType(), True),
            StructField("zona", StringType(), True),
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True),
            StructField("toneladas", DoubleType(), True),
            StructField("capacidad_toneladas", DoubleType(), True),
            StructField("ocupacion_pct", DoubleType(), True),
            StructField("velocidad", DoubleType(), True),
            StructField("estado_entrega", StringType(), True),
            StructField("tiempo_espera_min", IntegerType(), True),
            StructField("riesgo_retraso", DoubleType(), True),
            StructField("entrega_a_tiempo", BooleanType(), True),
            StructField("fuente", StringType(), True),
        ]
    )


def build_streaming_dataframe(spark):
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        raw_df
        .select(
            F.col("topic").alias("_topic"),
            F.col("partition").alias("_partition"),
            F.col("offset").alias("_offset"),
            F.col("timestamp").alias("_kafka_timestamp"),
            F.col("key").cast("string").alias("_message_key"),
            F.col("value").cast("string").alias("_raw_message"),
        )
        .withColumn("json_data", F.from_json(F.col("_raw_message"), get_gps_schema()))
        .select(
            "json_data.*",
            "_topic",
            "_partition",
            "_offset",
            "_kafka_timestamp",
            "_message_key",
            "_raw_message",
        )
        .withColumn("timestamp_evento", F.to_timestamp("timestamp_evento"))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_system", F.lit("GPS_IOT_KAFKA"))
        .withColumn("_source_format", F.lit("JSON_STREAM"))
    )

    return parsed_df


def main() -> None:
    spark = None

    try:
        root_path = get_project_root()

        output_path = root_path / "data/bronze/gps_camiones_streaming"
        checkpoint_path = root_path / "data/checkpoints/streaming_bronze/gps_camiones"

        logger.info("Iniciando Spark Streaming Bronze.")
        logger.info("Kafka bootstrap servers: %s", BOOTSTRAP_SERVERS)
        logger.info("Kafka topic: %s", TOPIC_NAME)
        logger.info("Output path: %s", output_path)
        logger.info("Checkpoint path: %s", checkpoint_path)

        spark = build_spark_session(
            app_name="RetailX_06_Streaming_Bronze",
            include_delta=True,
            include_kafka=True,
            shuffle_partitions=4,
        )

        spark.sparkContext.setLogLevel("WARN")

        streaming_df = build_streaming_dataframe(spark)

        query = (
            streaming_df
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", str(checkpoint_path))
            .trigger(processingTime="10 seconds")
            .start(str(output_path))
        )

        logger.info("Streaming Bronze ejecutándose por %s segundos.", STREAMING_RUNTIME_SECONDS)

        query.awaitTermination(STREAMING_RUNTIME_SECONDS)
        query.stop()

        logger.info("Streaming Bronze finalizado correctamente.")

    except Exception as error:
        logger.exception("Error crítico en Streaming Bronze: %s", error)
        raise

    finally:
        if spark is not None:
            spark.stop()
            logger.info("SparkSession cerrada correctamente.")


if __name__ == "__main__":
    main()