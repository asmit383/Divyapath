from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from websocket.manager import manager

router = APIRouter()

@router.websocket("/live")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time live updates.
    
    Flow:
    1. Client connects.
    2. Connection is added to the manager.
    3. Server pushes updates received from Redis Pub/Sub (handled by manager).
    4. Client disconnect triggers cleanup.
    """
    await manager.connect(websocket)
    try:
        while True:
            # We just keep the connection open to send data.
            # Ideally verify connection health with ping/pong or expect some client heartbeat.
            # For now, we await text to detect disconnects, even if we don't process client msgs.
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        # Catch other errors (like network drop)
        manager.disconnect(websocket)
