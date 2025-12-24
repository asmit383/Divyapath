from fastapi import APIRouter, HTTPException, status
from schemas.sighting import SightingSchema
from kafka.producer import send_to_kafka

router = APIRouter()

@router.post("/", status_code=status.HTTP_202_ACCEPTED)
async def create_sighting(sighting: SightingSchema):
    """
    Ingest a new idol sighting from a volunteer device.

    Flow:
    1. Validates payload using SightingSchema.
    2. Asynchronously pushes event to Kafka topic 'volunteer_tracking'.
    3. Uses 'idol_id' as partition key to guarantee sequential processing order per idol.
    4. Returns 202 Accepted immediately.

    Processing happens downstream in Flink consumers. No db writes occur here.
    """
    try:
        # Publish to Kafka
        # We use idol_id as the key to ensure all updates for a specific idol 
        # land in the same Kafka partition, preserving order.
        await send_to_kafka(
            data=sighting.model_dump(),
            key=sighting.idol_id
        )
        return {"status": "queued", "msg": "Sighting received successfully"}
    except Exception as e:
        # Logged in producer, but we need to signal failure to client
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to queue sighting event"
        )
