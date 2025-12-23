from datetime import datetime
from pydantic import BaseModel

class SightingSchema(BaseModel):
    idol_id: str
    lat: float
    lon: float
    rssi: int
    timestamp: datetime
    volunteer_id: str
