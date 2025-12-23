from fastapi import APIRouter

router = APIRouter()

@router.post("/")
async def create_sighting():
    return {"msg": "ok"}
