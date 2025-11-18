from typing import Dict, List
from fastapi import WebSocket

class WebSocketManager:
    def __init__(self):
        self.connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, job_id: str, websocket: WebSocket):
        await websocket.accept()
        if job_id not in self.connections:
            self.connections[job_id] = []
        self.connections[job_id].append(websocket)

    def disconnect(self, job_id: str, websocket: WebSocket):
        if job_id in self.connections:
            self.connections[job_id].remove(websocket)
            if not self.connections[job_id]:
                del self.connections[job_id]

    async def broadcast(self, job_id: str, message: dict):
        if job_id not in self.connections:
            return
        living = []
        for ws in self.connections[job_id]:
            try:
                await ws.send_json(message)
                living.append(ws)
            except:
                pass
        self.connections[job_id] = living


ws_manager = WebSocketManager()
