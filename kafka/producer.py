import json
import logging
from aiokafka import AIOKafkaProducer
from core.config import settings

logger = logging.getLogger(__name__)

_producer: AIOKafkaProducer | None = None

async def init_kafka_producer():
    global _producer
    _producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS
    )
    try:
        await _producer.start()
        logger.info("Kafka Producer started.")
    except Exception as e:
        logger.error(f"Failed to start Kafka Producer: {e}")
        _producer = None


async def close_kafka_producer():
    global _producer
    if _producer:
        await _producer.stop()
        logger.info("Kafka Producer stopped.")
        _producer = None

async def send_to_kafka(data: dict, topic: str = settings.KAFKA_TOPIC_TRACKING):
    if not _producer:
        logger.warning("Kafka producer not initialized. Skipping message.")
        return

    try:
        value = json.dumps(data).encode("utf-8")
        await _producer.send_and_wait(topic, value)
    except Exception as e:
        logger.error(f"Failed to send message to Kafka: {e}")
