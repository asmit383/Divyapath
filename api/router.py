from fastapi import APIRouter

from api.sightings import router as sightings_router
from api.positions import router as positions_router
from api.heatmap import router as heatmap_router
from api.admin import router as admin_router

api_router = APIRouter()

@api_router.get("/health")
def health_check():
    return {"status": "ok", "service": "DivyaPath Backend"}

api_router.include_router(sightings_router, prefix="/sightings", tags=["sightings"])
api_router.include_router(positions_router, prefix="/positions", tags=["positions"])
api_router.include_router(heatmap_router, prefix="/heatmap", tags=["heatmap"])
api_router.include_router(admin_router, prefix="/admin", tags=["admin"])

