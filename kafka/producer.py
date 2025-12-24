import json
import logging
from typing import Optional

from aiokafka import AIOKafkaProducer
from core.config import settings

logger = logging.getLogger(__name__)

_producer: Optional[AIOKafkaProducer] = None

async def init_kafka_producer():
    """
    Initialize the AIOKafkaProducer as a singleton.
    Starts the producer and sets the global instance.
    """
    global _producer
    if _producer:
        return

    logger.info("Initializing Kafka Producer...")
    try:
        _producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
        await _producer.start()
        logger.info("Kafka Producer started successfully.")
    except Exception as e:
        logger.error(f"Failed to start Kafka Producer: {e}")
        _producer = None
        raise

async def close_kafka_producer():
    """
    Stops the global AIOKafkaProducer instance.
    """
    global _producer
    if _producer:
        logger.info("Stopping Kafka Producer...")
        await _producer.stop()
        _producer = None
        logger.info("Kafka Producer stopped.")

async def send_to_kafka(
    data: dict, 
    key: Optional[str] = None, 
    topic: str = settings.KAFKA_TOPIC_TRACKING
):
    """
    Sends a dictionary message to the specified Kafka topic.
    
    Args:
        data: The dictionary payload to send.
        key: The key for partitioning (e.g., idol_id). 
             Messages with the same key go to the same partition (guaranteed ordering).
        topic: The Kafka topic to publish to.
        
    Raises:
        RuntimeError: If the producer is not initialized.
        Exception: If the send operation fails.
    """
    if not _producer:
        raise RuntimeError("Kafka producer is not initialized. Application startup failed or is incomplete.")

    try:
        await _producer.send_and_wait(topic=topic, value=data, key=key)
    except Exception as e:
        logger.error(f"Failed to send message to Kafka topic '{topic}': {e}")
        raise
