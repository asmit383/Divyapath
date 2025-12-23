from fastapi import APIRouter

from api.sightings import router as sightings_router

api_router = APIRouter()

@api_router.get("/health")
def health_check():
    return {"status": "ok", "service": "DivyaPath Backend"}

api_router.include_router(sightings_router, prefix="/sightings", tags=["sightings"])

