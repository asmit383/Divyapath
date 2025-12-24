import asyncio
import logging
import json
from typing import List, Optional
from fastapi import WebSocket
from db.redis_client import get_redis_client

logger = logging.getLogger(__name__)

class ConnectionManager:
    """
    Manages WebSocket connections and Redis Pub/Sub integration.
    """
    def __init__(self):
        # List of active WebSocket connections
        self.active_connections: List[WebSocket] = []
        # Background task for reading from Redis
        self.redis_task: Optional[asyncio.Task] = None
        self.must_stop_redis = False

    async def connect(self, websocket: WebSocket):
        """Accepts a new WebSocket connection."""
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"Client connected. Active connections: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        """Removes a WebSocket connection."""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"Client disconnected. Active connections: {len(self.active_connections)}")

    async def broadcast(self, message: str):
        """Sends a message to all connected clients."""
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception as e:
                logger.error(f"Error sending to client: {e}")
                # We could disconnect here, but usually disconnect happens on receive/send failure handled by endpoint

    async def start_redis_listener(self, channel: str = "live_updates"):
        """
        Starts a background task that subscribes to a Redis channel 
        and broadcasts received messages to all WebSocket clients.
        """
        self.must_stop_redis = False
        if self.redis_task:
            return

        logger.info(f"Starting Redis Pub/Sub listener on channel: {channel}")
        self.redis_task = asyncio.create_task(self._redis_listener_loop(channel))

    async def stop_redis_listener(self):
        """Stops the background Redis listener task."""
        self.must_stop_redis = True
        if self.redis_task:
            logger.info("Stopping Redis Pub/Sub listener...")
            self.redis_task.cancel()
            try:
                await self.redis_task
            except asyncio.CancelledError:
                pass
            self.redis_task = None
            logger.info("Redis Pub/Sub listener stopped.")

    async def _redis_listener_loop(self, channel: str):
        """
        Internal loop to listen for Redis messages.
        """
        try:
            redis_client = await get_redis_client()
            async with redis_client.pubsub() as pubsub:
                await pubsub.subscribe(channel)
                logger.info(f"Subscribed to Redis channel: {channel}")
                
                async for message in pubsub.listen():
                    if self.must_stop_redis:
                        break
                        
                    if message["type"] == "message":
                        data = message["data"]
                        # data is usually a string (JSON) if decode_responses=True
                        # Broadcast raw string to clients to save serialization overhead
                        await self.broadcast(data)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Redis Pub/Sub listener error: {e}")
            # In production, you might want logic to restart the listener after a delay
            
manager = ConnectionManager()
