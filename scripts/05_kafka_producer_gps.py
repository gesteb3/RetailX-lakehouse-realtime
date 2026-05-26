import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config.spark_config import setup_logging


SCRIPT_NAME = "05_kafka_producer_gps"
logger = setup_logging(SCRIPT_NAME)

TOPIC_NAME = os.getenv("KAFKA_TOPIC", "gpscamiones")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
PRODUCER_MAX_EVENTS = int(os.getenv("PRODUCER_MAX_EVENTS", "300"))
PRODUCER_INTERVAL_SECONDS = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "0.5"))


ZONAS = [
    {
        "zona": "Centro_Distribucion",
        "base_lat": 14.6349,
        "base_lon": -90.5069,
        "riesgo_base": 0.10,
    },
    {
        "zona": "Ruta_Atlantico",
        "base_lat": 14.7200,
        "base_lon": -90.3500,
        "riesgo_base": 0.35,
    },
    {
        "zona": "Ruta_Pacifico",
        "base_lat": 14.3000,
        "base_lon": -90.7800,
        "riesgo_base": 0.28,
    },
    {
        "zona": "Zona_Oriente",
        "base_lat": 14.9667,
        "base_lon": -89.5333,
        "riesgo_base": 0.45,
    },
    {
        "zona": "Zona_Occidente",
        "base_lat": 14.8333,
        "base_lon": -91.5167,
        "riesgo_base": 0.30,
    },
]


ESTADOS_ENTREGA = [
    "EN_RUTA",
    "EN_PATIO",
    "DESCARGANDO",
    "FINALIZADO",
]


def build_producer() -> KafkaProducer:
    attempts = 0

    while attempts < 10:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                key_serializer=lambda value: value.encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=50,
            )

            logger.info("Producer conectado a Kafka en %s", BOOTSTRAP_SERVERS)
            return producer

        except NoBrokersAvailable:
            attempts += 1
            logger.warning("Kafka no disponible. Reintento %s/10...", attempts)
            time.sleep(3)

    raise ConnectionError(f"No fue posible conectar a Kafka en {BOOTSTRAP_SERVERS}")


def generate_event(event_id: int) -> dict:
    zona_info = random.choice(ZONAS)

    camion_id = random.randint(1, 35)
    capacidad = random.choice([8, 10, 12, 15, 18, 20, 25])
    toneladas = round(random.uniform(1, capacidad), 2)
    ocupacion_pct = round((toneladas / capacidad) * 100, 2)

    velocidad = round(random.uniform(0, 95), 2)
    tiempo_espera = random.randint(0, 90)

    estado = random.choices(
        ESTADOS_ENTREGA,
        weights=[0.55, 0.20, 0.15, 0.10],
        k=1,
    )[0]

    riesgo_retraso = min(
        1.0,
        zona_info["riesgo_base"]
        + (0.25 if velocidad < 15 and estado == "EN_RUTA" else 0)
        + (0.20 if tiempo_espera > 45 else 0)
        + random.uniform(0, 0.15),
    )

    entrega_a_tiempo = riesgo_retraso < 0.55

    return {
        "event_id": event_id,
        "timestamp_evento": datetime.now(timezone.utc).isoformat(),
        "camion": f"CAM-{camion_id:03d}",
        "zona": zona_info["zona"],
        "lat": round(zona_info["base_lat"] + random.uniform(-0.05, 0.05), 6),
        "lon": round(zona_info["base_lon"] + random.uniform(-0.05, 0.05), 6),
        "toneladas": toneladas,
        "capacidad_toneladas": capacidad,
        "ocupacion_pct": ocupacion_pct,
        "velocidad": velocidad,
        "estado_entrega": estado,
        "tiempo_espera_min": tiempo_espera,
        "riesgo_retraso": round(riesgo_retraso, 4),
        "entrega_a_tiempo": entrega_a_tiempo,
        "fuente": "gps_iot_simulado",
    }


def main() -> None:
    producer = None

    try:
        logger.info("Iniciando producer GPS RetailX.")
        logger.info("Topic destino: %s", TOPIC_NAME)
        logger.info("Eventos a generar: %s", PRODUCER_MAX_EVENTS)

        producer = build_producer()

        for event_id in range(1, PRODUCER_MAX_EVENTS + 1):
            event = generate_event(event_id)
            key = event["camion"]

            producer.send(
                topic=TOPIC_NAME,
                key=key,
                value=event,
            )

            if event_id % 25 == 0:
                producer.flush()
                logger.info("Eventos enviados: %s", event_id)

            time.sleep(PRODUCER_INTERVAL_SECONDS)

        producer.flush()
        logger.info("Producer finalizado correctamente. Total eventos: %s", PRODUCER_MAX_EVENTS)

    except Exception as error:
        logger.exception("Error crítico en Kafka Producer GPS: %s", error)
        raise

    finally:
        if producer is not None:
            producer.close()
            logger.info("Producer cerrado correctamente.")


if __name__ == "__main__":
    main()