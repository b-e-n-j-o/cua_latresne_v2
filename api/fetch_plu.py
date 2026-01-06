from fastapi import APIRouter, HTTPException
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

def get_cached_plu(insee: str) -> str | None:
    """Retourne l'URL signée si en cache"""
    try:
        file_path = f"reglements/{insee}.pdf"
        files = supabase.storage.from_(BUCKET_NAME).list("reglements")
        
        if not any(f["name"] == f"{insee}.pdf" for f in files):
            return None
        
        url = supabase.storage.from_(BUCKET_NAME).create_signed_url(
            file_path, 
            expires_in=3600
        )
        return url["signedURL"]
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
async def check_plu_availability(insee: str):
    # Vérifier cache d'abord
    cached = get_cached_plu(insee)
    if cached:
        return {"available": True, "insee": insee, "cached": True}
    
    # Sinon vérifier GPU
    try:
        atom_xml = fetch_atom_xml(insee)
        zip_url = extract_zip_url(atom_xml)
        return {"available": True, "insee": insee, "url": zip_url, "cached": False}
    except Exception as e:
        return {"available": False, "insee": insee, "error": str(e)}

@router.get("/reglement/{insee}")
async def get_reglement_plu(insee: str):
    # Vérifier cache
    cached_url = get_cached_plu(insee)
    if cached_url:
        return JSONResponse({"url": cached_url, "cached": True})
    
    # Télécharger
    try:
        atom_xml = fetch_atom_xml(insee)
        zip_url = extract_zip_url(atom_xml)
        
        zip_bytes = requests.get(zip_url, timeout=300).content
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
        
        pdfs = [n for n in zf.namelist() 
                if n.lower().endswith(".pdf") and "reglement" in n.lower()]
        
        if not pdfs:
            raise HTTPException(404, "Règlement non trouvé")
        
        pdf_bytes = zf.read(pdfs[0])
        
        # Tenter mise en cache
        cached = cache_plu(insee, pdf_bytes)
        
        if cached:
            cached_url = get_cached_plu(insee)
            return JSONResponse({"url": cached_url, "cached": False})
        
        # Si trop gros : retourner les bytes directement
        from fastapi.responses import Response
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": f"inline; filename=reglement_{insee}.pdf"}
        )
        
    except requests.HTTPError:
        raise HTTPException(404, f"PLU non trouvé pour {insee}")
    except Exception as e:
        raise HTTPException(500, str(e))