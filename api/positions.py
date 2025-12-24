from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, status
import logging

from db.redis_client import get_redis_client
from db.mysql import get_latest_positions as fetch_from_mysql

logger = logging.getLogger(__name__)
router = APIRouter()

async def get_all_latest_from_redis() -> List[Dict[str, Any]]:
    """
    Helper to scan Redis for all 'idol:*:pos' keys and retrieve their values.
    Returns a list of dictionaries.
    """
    client = await get_redis_client()
    try:
        keys = await client.keys("idol:*:pos")
        if not keys:
            return []
        
        # MGET for efficiency
        values = await client.mget(keys)
        # Parse JSON strings to dicts, filtering out Nones
        import json
        return [json.loads(v) for v in values if v]
    except Exception as e:
        logger.error(f"Failed to fetch latest positions from Redis: {e}")
        return []

@router.get("/", response_model=List[Dict[str, Any]])
async def get_positions(limit: int = 100):
    """
    Retrieve the latest known positions for all idols.

    Strategy:
    1. **Redis Priority**: Attempts to fetch real-time data from Redis cache first.
       This is the 'Hot' path and is extremely fast.
    2. **MySQL Fallback**: If Redis is empty (e.g., after a cold restart) or fails, 
       it fetches the latest known positions from the persistent MySQL database.
    
    Returns:
        List of objects containing: {idol_id, lat, lon, timestamp, ...}
    """
    # 1. Try Redis
    realtime_data = await get_all_latest_from_redis()
    if realtime_data:
        # Sort by timestamp desc to match MySQL behavior if needed, 
        # though map clients usually just want "latest state".
        return realtime_data

    # 2. Fallback to MySQL
    logger.info("Redis cache miss or empty. Falling back to MySQL.")
    try:
        # Note: MySQL calls are synchronous here. In a high-load production env,
        # we might wrap this in run_in_executor, but for this scale it's acceptable fallback.
        historical_data = fetch_from_mysql(limit=limit)
        return historical_data
    except Exception as e:
        logger.error(f"Failed to fetch positions from MySQL fallback: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Unable to retrieve idol positions."
        )
