# websocket_endpoints.py

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import uuid
import os
import base64

from websocket_manager import ws_manager
from CERFA_ANALYSE.pre_analyse_cerfa import pre_analyse_cerfa
from CERFA_ANALYSE.analyse_gemini import analyse_cerfa

router = APIRouter()

# -----------------------------------------------------------
# 🔵 1) WebSocket : suivi des jobs existants
# -----------------------------------------------------------
@router.websocket("/ws/job/{job_id}")
async def ws_job(websocket: WebSocket, job_id: str):
    await ws_manager.connect(job_id, websocket)
    try:
        while True:
            await websocket.receive_text()
    except:
        ws_manager.disconnect(job_id, websocket)


# -----------------------------------------------------------
# 🟦 2) WebSocket Pipeline CERFA (pré-analyse → validation)
# -----------------------------------------------------------
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
                await ws.send_json({"event": "progress", "step": 0, "label": "Pré-analyse du CERFA…"})

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

                cerfa_full = analyse_cerfa(pdf_temp_path, interactive=False)

                # OVERRIDE INSEE
                override_insee = message.get("insee")
                if override_insee and cerfa_full.get("data"):
                    cerfa_full["data"]["commune_insee"] = override_insee
                    cerfa_full["data"]["_insee_override"] = True

                # OVERRIDE PARCELLES
                override_parcelles = message.get("parcelles")
                if override_parcelles and cerfa_full.get("data"):
                    cerfa_full["data"]["references_cadastrales"] = [
                        {
                            "section": p.get("section"),
                            "numero": p.get("numero"),
                            "surface_m2": p.get("surface_m2")
                        }
                        for p in override_parcelles
                    ]
                    cerfa_full["data"]["_parcelles_override"] = True

                await ws.send_json({"event": "cerfa_done", "cerfa": cerfa_full})

            else:
                await ws.send_json({"event": "error", "message": f"Action inconnue : {action}"})

    except WebSocketDisconnect:
        print("🔌 Client WebSocket déconnecté")
    except Exception as e:
        print(f"❌ Erreur WebSocket pipeline: {e}")
        await ws.send_json({"event": "error", "message": str(e)})
    finally:
        if pdf_temp_path and os.path.exists(pdf_temp_path):
            try:
                os.unlink(pdf_temp_path)
            except:
                pass
