from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Query
import logging
import json

from db.redis_client import get_redis_client

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/", response_model=List[Dict[str, Any]])
async def get_heatmap(
    lat_min: Optional[float] = Query(None, description="Minimum Latitude"),
    lat_max: Optional[float] = Query(None, description="Maximum Latitude"),
    lon_min: Optional[float] = Query(None, description="Minimum Longitude"),
    lon_max: Optional[float] = Query(None, description="Maximum Longitude"),
):
    """
    Retrieve Crowd Density Heatmap Data.

    This endpoint returns pre-aggregated heatmap points from Redis.
    The external stream processor (e.g., Flink) is responsible for calculating density 
    and writing it to Redis keys matching 'heatmap:*'.

    Returns:
        List of objects: {lat, lon, weight/intensity, ...}
    """
    client = await get_redis_client()
    try:
        # 1. Scan for all heatmap keys
        # In a production geo-spatial setup, we would use GEOSEARCH or QuadTree keys.
        # For this version, we scan the simple 'heatmap:*' pattern.
        keys = await client.keys("heatmap:*")
        
        if not keys:
            return []

        # 2. Batch fetch values
        values = await client.mget(keys)
        
        # 3. Parse and Filter
        results = []
        for v in values:
            if not v:
                continue
            
            try:
                point = json.loads(v)
                
                # 4. Optional Bounding Box Filtering
                # If bbox params are provided, filter in-memory.
                # (Ideally, Redis GEO commands would handle this, but this works for basic JSON storage)
                if (lat_min is not None and point.get('lat', 0) < lat_min): continue
                if (lat_max is not None and point.get('lat', 0) > lat_max): continue
                if (lon_min is not None and point.get('lon', 0) < lon_min): continue
                if (lon_max is not None and point.get('lon', 0) > lon_max): continue

                results.append(point)
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON in heatmap data: {v}")
                continue
                
        return results

    except Exception as e:
        logger.error(f"Failed to fetch heatmap data: {e}")
        # Return empty list gracefully instead of 500ing the frontend
        return []
