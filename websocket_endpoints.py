# websocket_endpoints.py

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pathlib import Path
import uuid
import os
import base64

from websocket_manager import ws_manager
from CERFA_ANALYSE.pre_analyse_cerfa import pre_analyse_cerfa
from CERFA_ANALYSE.analyse_gemini import analyse_cerfa

router = APIRouter()

# ============================================================
# 🔵 1) WebSocket /ws/job/{job_id} — suivi logs pipeline
# ============================================================
@router.websocket("/ws/job/{job_id}")
async def ws_job(websocket: WebSocket, job_id: str):
    await ws_manager.connect(job_id, websocket)
    try:
        while True:
            await websocket.receive_text()
    except:
        ws_manager.disconnect(job_id, websocket)


# ============================================================
# 🟦 2) WebSocket /ws/pipeline — UNIQUEMENT CERFA
#    (pré-analyse → validation → analyse complète)
# ============================================================
@router.websocket("/ws/pipeline")
async def ws_pipeline(ws: WebSocket):
    await ws.accept()
    pdf_temp_path = None

    try:
        while True:
            message = await ws.receive_json()
            action = message.get("action")

            # ------------------------------
            # 1) START PRE-ANALYSE
            # ------------------------------
            if action == "start_preanalyse":
                await ws.send_json({
                    "event": "progress",
                    "step": 0,
                    "label": "Pré-analyse du CERFA…"
                })

                pdf_b64 = message["pdf"]
                pdf_raw = base64.b64decode(pdf_b64)

                pdf_temp_path = f"/tmp/preanalyse_{uuid.uuid4()}.pdf"
                with open(pdf_temp_path, "wb") as f:
                    f.write(pdf_raw)

                pre = pre_analyse_cerfa(pdf_temp_path)

                await ws.send_json({
                    "event": "preanalyse_result",
                    "pdf_path": pdf_temp_path,
                    "preanalyse": pre
                })

            # ------------------------------
            # 2) CONFIRMATION UTILISATEUR
            # ------------------------------
            elif action == "confirm_preanalyse":
                await ws.send_json({
                    "event": "progress",
                    "step": 1,
                    "label": "Analyse complète du CERFA…"
                })

                pdf_temp_path = message["pdf_path"]

                # Analyse complète
                cerfa_full = analyse_cerfa(pdf_temp_path, interactive=False)

                # Overrides
                override_insee = message.get("insee")
                if override_insee and cerfa_full.get("data"):
                    cerfa_full["data"]["commune_insee"] = override_insee

                override_parcelles = message.get("parcelles")
                if override_parcelles and cerfa_full.get("data"):
                    cerfa_full["data"]["references_cadastrales"] = override_parcelles

                # FERMETURE DU WS : le front doit appeler /pipeline/run-rest
                await ws.send_json({
                    "event": "cerfa_done",
                    "pdf_path": pdf_temp_path,
                    "cerfa": cerfa_full
                })

                await ws.close()  # Fermeture propre de la connexion WebSocket
                break  # Sortir de la boucle, fermeture propre via finally

            else:
                await ws.send_json({"event": "error", "message": f"Action inconnue : {action}"})

    except WebSocketDisconnect:
        print("🔌 Client WebSocket déconnecté")
    except Exception as e:
        print(f"❌ Erreur WebSocket pipeline: {e}")
        await ws.send_json({"event": "error", "message": str(e)})
