import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer
from core.config import settings
from db.redis_client import init_redis_client, close_redis_client, set_latest_position, publish_update

logger = logging.getLogger(__name__)

async def consume_tracking_events():
    """
    Kafka Consumer Loop.
    Reads sighting events from 'volunteer_tracking' topic.
    Stores latest position in Redis (with TTL) and publishes for WebSockets.
    """
    consumer = AIOKafkaConsumer(
        settings.KAFKA_TOPIC_TRACKING,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id="divyapath_backend_consumer_group_v2",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest" # Start from new messages
    )
    
    # Ensure Redis is connected (in case run standalone)
    await init_redis_client()
    
    logger.info("Starting Kafka Consumer for Tracking Events...")
    await consumer.start()
    
    try:
        async for msg in consumer:
            try:
                data = msg.value
                idol_id = data.get("idol_id")
                
                if idol_id:
                    # 1. Store in Redis with TTL (10 minutes)
                    await set_latest_position(idol_id, data, ttl=15)
                    
                    # 2. Publish to Redis Pub/Sub for WebSockets
                    # Channel: "live_updates" (matches websocket/manager.py)
                    await publish_update("live_updates", data)
                    
                    logger.info(f"Processed event for idol {idol_id}: {data}")
                else:
                    logger.warning(f"Received malformed message: {data}")
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                
    except Exception as e:
        logger.error(f"Kafka Consumer crashed: {e}")
    finally:
        logger.info("Stopping Kafka Consumer...")
        await consumer.stop()
        # await close_redis_client() # If run standalone, might want to close. If integrated, maybe not.
                                     # Careful if running in same loop as API.

if __name__ == "__main__":
    # Standalone entry point
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(consume_tracking_events())
    except KeyboardInterrupt:
        pass
