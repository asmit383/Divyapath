import json
import logging
from typing import Optional

import redis.asyncio as redis
from core.config import settings

logger = logging.getLogger(__name__)

_redis_client: Optional[redis.Redis] = None

async def init_redis_client():
    """
    Initialize the async Redis client as a singleton.
    """
    global _redis_client
    if _redis_client:
        return

    logger.info("Initializing Redis Client...")
    try:
        _redis_client = redis.from_url(
            settings.REDIS_URL, 
            encoding="utf-8", 
            decode_responses=True
        )
        # Ping to ensure connection is established
        await _redis_client.ping()
        logger.info("Redis Client connected successfully.")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        _redis_client = None
        raise

async def close_redis_client():
    """
    Closes the global Redis client connection.
    """
    global _redis_client
    if _redis_client:
        logger.info("Closing Redis Client...")
        await _redis_client.close()
        _redis_client = None
        logger.info("Redis Client closed.")

async def get_redis_client() -> redis.Redis:
    """
    Accessor for the global Redis client. Raises error if not initialized.
    """
    if not _redis_client:
        raise RuntimeError("Redis client is not initialized.")
    return _redis_client

# --- High-Level Helper Functions ---

async def set_latest_position(idol_id: str, data: dict, ttl: int = 600):
    """
    Stores the latest position of an idol in Redis.
    Uses a simple key-value pair with JSON string.
    Key format: "idol:{idol_id}:pos"
    
    Args:
        idol_id: The ID of the idol.
        data: Position data dictionary.
        ttl: Expiration time in seconds (default 10 minutes).
    """
    client = await get_redis_client()
    key = f"idol:{idol_id}:pos"
    try:
        await client.set(key, json.dumps(data), ex=ttl)
    except Exception as e:
        logger.error(f"Redis set failed for {key}: {e}")
        raise

async def get_latest_position(idol_id: str) -> Optional[dict]:
    """
    Retrieves the latest position of an idol from Redis.
    Returns None if not found.
    """
    client = await get_redis_client()
    key = f"idol:{idol_id}:pos"
    try:
        data_str = await client.get(key)
        if data_str:
            return json.loads(data_str)
        return None
    except Exception as e:
        logger.error(f"Redis get failed for {key}: {e}")
        raise

async def publish_update(channel: str, message: dict):
    """
    Publishes a message to a Redis Pub/Sub channel.
    Used for broadcasting real-time updates to connected WebSockets.
    """
    client = await get_redis_client()
    try:
        await client.publish(channel, json.dumps(message))
    except Exception as e:
        logger.error(f"Redis publish failed on channel {channel}: {e}")
        raise
