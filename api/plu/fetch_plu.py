from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
import requests
import xml.etree.ElementTree as ET
import zipfile
import io
from supabase import create_client
import os
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/plu", tags=["PLU"])

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SERVICE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

BUCKET_NAME = "plu-reglements-cached"
MAX_CACHE_SIZE = 49 * 1024 * 1024  # 49 Mo
ATOM_BASE = "https://www.geoportail-urbanisme.gouv.fr/atom/dataset-feed"
NS = {"atom": "http://www.w3.org/2005/Atom"}

def fetch_atom_xml(insee: str) -> str:
    url = f"{ATOM_BASE}/DU_{insee}.xml"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.text

def extract_zip_url(atom_xml: str) -> str:
    root = ET.fromstring(atom_xml)
    for entry in root.findall("atom:entry", NS):
        for link in entry.findall("atom:link", NS):
            href = link.attrib.get("href", "")
            if href.endswith(".zip") and "/api/document/" in href:
                return href
    raise RuntimeError("Lien ZIP introuvable")

def get_plu_code(insee: str) -> dict:
    """Retourne le code PLU/PLUI à utiliser + infos EPCI"""
    result = supabase.table("plu_epci_mapping")\
        .select("epci_code, epci_name, document_type")\
        .eq("insee", insee)\
        .execute()
    
    if result.data:
        return {
            "code": result.data[0]["epci_code"],
            "name": result.data[0]["epci_name"],
            "type": result.data[0]["document_type"]
        }
    return {"code": insee, "name": None, "type": "PLU"}


def get_cached_plu(path: str) -> str | None:
    """path peut être '243300316' ou '243300316/UP27'"""
    try:
        file_path = f"reglements/{path}.pdf"
        result = supabase.storage.from_(BUCKET_NAME).create_signed_url(
            file_path, 
            expires_in=3600
        )
        return result.get("signedURL") if result else None
    except:
        return None



def cache_plu(insee: str, pdf_bytes: bytes) -> bool:
    size_mb = len(pdf_bytes) / (1024 * 1024)
    logger.info(f"Tentative cache PLU {insee}: {size_mb:.2f} Mo")
    
    if len(pdf_bytes) > MAX_CACHE_SIZE:
        return False
    
    try:
        file_path = f"reglements/{insee}.pdf"
        supabase.storage.from_(BUCKET_NAME).upload(
            file_path,
            pdf_bytes,
            file_options={
                "content-type": "application/pdf",
                "x-upsert": "true"  # ✅ String, pas bool
            }
        )
        logger.info(f"✅ PLU {insee} mis en cache")
        return True
    except Exception as e:
        logger.error(f"❌ Échec cache PLU {insee}: {e}")
        return False

@router.get("/check/{insee}")
async def check_plu_availability(insee: str, zone: str = Query(None)):
    plu_info = get_plu_code(insee)
    plu_code = plu_info["code"]
    
    # Vérifier cache d'abord
    cached = get_cached_plu(plu_code)
    if cached:
        result = {
            "available": True, 
            "insee": insee, 
            "plu_code": plu_code,
            "type": plu_info["type"],
            "epci_name": plu_info["name"],
            "cached": True
        }
        if zone:
            result["zone"] = zone
        return result
    
    # Sinon vérifier GPU
    try:
        atom_xml = fetch_atom_xml(plu_code)
        zip_url = extract_zip_url(atom_xml)
        result = {
            "available": True, 
            "insee": insee,
            "plu_code": plu_code,
            "type": plu_info["type"],
            "epci_name": plu_info["name"],
            "cached": False
        }
        if zone:
            result["zone"] = zone
        return result
    except Exception as e:
        return {"available": False, "insee": insee, "error": str(e)}

@router.get("/reglement/{insee}")
async def get_reglement_plu(insee: str):
    plu_info = get_plu_code(insee)
    plu_code = plu_info["code"]
    
    # Vérifier cache
    cached_url = get_cached_plu(plu_code)
    if cached_url:
        return JSONResponse({
            "url": cached_url, 
            "cached": True,
            "type": plu_info["type"]
        })
    
    # Télécharger
    try:
        atom_xml = fetch_atom_xml(plu_code)
        zip_url = extract_zip_url(atom_xml)
        
        zip_bytes = requests.get(zip_url, timeout=300).content
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
        
        # Filtrer : dossier 3_Reglement + nom contient "reglement" + exclure graphique/prescription
        pdfs = [
            n for n in zf.namelist()
            if n.lower().endswith(".pdf")
            and "3_reglement" in n.lower()
            and "reglement" in n.lower()
            and "graphique" not in n.lower()
            and "prescription" not in n.lower()
        ]
        
        if not pdfs:
            raise HTTPException(404, "Règlement textuel non trouvé")
        
        # Prioriser le fichier le plus court (souvent juste "reglement.pdf")
        pdfs.sort(key=len)
        pdf_name = pdfs[0]
        pdf_bytes = zf.read(pdf_name)
        
        # Tenter mise en cache
        cached = cache_plu(plu_code, pdf_bytes)
        
        if cached:
            cached_url = get_cached_plu(plu_code)
            return JSONResponse({
                "url": cached_url, 
                "cached": False,
                "type": plu_info["type"]
            })
        
        # Si trop gros : retourner les bytes directement
        from fastapi.responses import Response
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": f"inline; filename=reglement_{plu_code}.pdf"}
        )
        
    except requests.HTTPError:
        raise HTTPException(404, f"PLU non trouvé pour {insee}")
    except Exception as e:
        raise HTTPException(500, str(e))

@router.get("/zonage/{insee}")
async def get_zonage_at_point(
    insee: str,
    lon: float = Query(..., description="Longitude"),
    lat: float = Query(..., description="Latitude")
):
    plu_info = get_plu_code(insee)
    plu_code = plu_info["code"]
    
    # Si pas de zonage découpé (PLU simple), retourner vide
    if plu_info["type"] != "PLUI":
        return {"zones": []}
    
    result = supabase.rpc("get_zonage_at_point", {
        "code_siren": plu_code,
        "lon": lon,
        "lat": lat
    }).execute()
    
    zones = [z["libelle"] for z in result.data] if result.data else []
    return {"zones": zones}


@router.get("/reglement/{insee}/zone/{zone}")
async def get_reglement_zone(insee: str, zone: str):
    plu_info = get_plu_code(insee)
    plu_code = plu_info["code"]
    
    path = f"{plu_code}/{zone}"
    logger.info(f"Recherche: {path}")
    
    cached_url = get_cached_plu(path)
    logger.info(f"Résultat: {cached_url}")
    
    if cached_url:
        return JSONResponse({"url": cached_url, "zone": zone})
    
    raise HTTPException(404, f"Zone {zone} non disponible")