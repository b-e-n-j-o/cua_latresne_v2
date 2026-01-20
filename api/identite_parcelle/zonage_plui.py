"""
Endpoint pour récupérer le zonage PLUi d'une parcelle cadastrale
via intersection géométrique entre WFS IGN et la table carto.plui_bordeaux_zonage
hébergée sur Supabase.
"""
from fastapi import APIRouter, HTTPException, Body
from pydantic import BaseModel
from typing import List, Optional
import os
import json
import psycopg2
import requests
from dotenv import load_dotenv

router = APIRouter()

# Charger les variables d'environnement (.env)
load_dotenv()


def get_db_connection():
    """
    Crée une connexion PostgreSQL vers Supabase en utilisant les variables d'environnement.
    """
    return psycopg2.connect(
        host=os.getenv("SUPABASE_HOST"),
        dbname=os.getenv("SUPABASE_DB"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
        port=int(os.getenv("SUPABASE_PORT", "5432")),
    )


@router.get("/zonage-plui/{insee}/{section}/{numero}")
async def get_zonage_plui(insee: str, section: str, numero: str):
    """
    Récupère le zonage PLUi d'une parcelle par intersection géométrique.
    
    Process:
    1. Fetch géométrie parcelle depuis WFS IGN (EPSG:2154)
    2. Intersection avec carto.plui_bordeaux_zonage
    3. Retourne typezone + etiquette
    """
    
    # ============================================================
    # 1. Fetch géométrie parcelle depuis WFS IGN (EPSG:2154)
    #    Logique alignée sur le script de test fonctionnel
    # ============================================================
    wfs_url = "https://data.geopf.fr/wfs"
    print(
        f"[zonage_plui] Requête WFS IGN pour parcelle "
        f"insee={insee}, section={section}, numero={numero}"
    )
    section_norm = section.upper().strip()
    numero_norm = numero.zfill(4)

    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": "CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle",
        "outputFormat": "application/json",
        "srsName": "EPSG:2154",
        "cql_filter": (
            f"code_insee='{insee}' AND "
            f"section='{section_norm}' AND "
            f"numero='{numero_norm}'"
        ),
    }

    try:
        print(f"[zonage_plui] Appel WFS URL={wfs_url}")
        print(f"[zonage_plui] Params WFS={params}")
        resp = requests.get(wfs_url, params=params, timeout=30)
        resp.raise_for_status()
        geojson = resp.json()
        print(
            f"[zonage_plui] WFS OK, nb features="
            f"{len(geojson.get('features', []))}"
        )
    except Exception as e:
        print(f"[zonage_plui] Erreur WFS IGN: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Erreur WFS IGN: {str(e)}"
        )

    if not geojson.get("features"):
        print(
            "[zonage_plui] Aucune feature retournée par le WFS pour "
            f"{insee} {section_norm} {numero_norm}"
        )
        raise HTTPException(
            status_code=404,
            detail=f"Parcelle introuvable au cadastre pour {insee} {section_norm} {numero_norm}"
        )

    parcelle_geom = geojson["features"][0]["geometry"]
    
    # ============================================================
    # 2. Intersection avec PLUi en base (carto.plui_bordeaux_zonage)
    # ============================================================
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # On force le SRID 2154 sur la géométrie GeoJSON issue de l'IGN
        # (le WFS renvoie des coordonnées en 2154 mais sans SRID explicite en GeoJSON)
        sql = """
            SELECT 
                typezone,
                etiquette,
                libelle,
                libelong,
                ST_Area(ST_Intersection(
                    geom_2154,
                    ST_SetSRID(
                        ST_GeomFromGeoJSON(%s),
                        2154
                    )
                )) as intersection_area
            FROM carto.plui_bordeaux_zonage
            WHERE ST_Intersects(
                geom_2154,
                ST_SetSRID(
                    ST_GeomFromGeoJSON(%s),
                    2154
                )
            )
            ORDER BY intersection_area DESC
            LIMIT 1
        """
        geom_str = json.dumps(parcelle_geom)
        print("[zonage_plui] Lancement requête SQL intersection PLUi…")
        cur.execute(sql, (geom_str, geom_str))

        row = cur.fetchone()

        if not row:
            print("[zonage_plui] Aucune zone PLUi intersectée")
            return {
                "typezone": None,
                "etiquette": None,
                "libelle": None,
                "libelong": None,
                "message": "Parcelle hors zonage PLUi"
            }

        print(
            "[zonage_plui] Intersection trouvée : "
            f"typezone={row[0]!r}, etiquette={row[1]!r}"
        )

        return {
            "typezone": row[0],
            "etiquette": row[1],
            "libelle": row[2],
            "libelong": row[3],
            "intersection_area": float(row[4]) if row[4] else 0
        }

    finally:
        cur.close()
        conn.close()


# ============================================================
# Modèles Pydantic pour les unités foncières
# ============================================================

class UFParcelle(BaseModel):
    section: str
    numero: str

class UFRequest(BaseModel):
    insee: str
    parcelles: List[UFParcelle]


@router.post("/zonage-plui/uf")
async def get_zonage_plui_uf(request: UFRequest):
    """
    Récupère les zonages PLUi pour une unité foncière (plusieurs parcelles).
    
    Retourne un dictionnaire avec les zonages de chaque parcelle.
    Chaque parcelle peut avoir un zonage différent.
    
    Process:
    1. Pour chaque parcelle, fetch géométrie depuis WFS IGN
    2. Intersection avec carto.plui_bordeaux_zonage
    3. Retourne tous les zonages trouvés
    """
    insee = request.insee
    parcelles = request.parcelles
    
    if not parcelles:
        raise HTTPException(
            status_code=400,
            detail="Aucune parcelle fournie pour l'unité foncière"
        )
    
    if len(parcelles) > 5:
        raise HTTPException(
            status_code=400,
            detail="Une unité foncière ne peut pas dépasser 5 parcelles"
        )
    
    wfs_url = "https://data.geopf.fr/wfs"
    results = {}
    
    # Pour chaque parcelle, récupérer son zonage
    for parcelle in parcelles:
        section = parcelle.section.upper().strip()
        numero = parcelle.numero.zfill(4)
        parcelle_key = f"{section}-{numero}"
        
        print(
            f"[zonage_plui_uf] Traitement parcelle "
            f"insee={insee}, section={section}, numero={numero}"
        )
        
        # 1. Fetch géométrie depuis WFS IGN
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": "CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle",
            "outputFormat": "application/json",
            "srsName": "EPSG:2154",
            "cql_filter": (
                f"code_insee='{insee}' AND "
                f"section='{section}' AND "
                f"numero='{numero}'"
            ),
        }
        
        try:
            resp = requests.get(wfs_url, params=params, timeout=30)
            resp.raise_for_status()
            geojson = resp.json()
            
            if not geojson.get("features"):
                print(
                    f"[zonage_plui_uf] Parcelle introuvable: {insee} {section} {numero}"
                )
                results[parcelle_key] = {
                    "section": section,
                    "numero": numero,
                    "typezone": None,
                    "etiquette": None,
                    "libelle": None,
                    "libelong": None,
                    "message": "Parcelle introuvable au cadastre"
                }
                continue
                
            parcelle_geom = geojson["features"][0]["geometry"]
            
            # 2. Intersection avec PLUi
            conn = get_db_connection()
            cur = conn.cursor()
            
            try:
                sql = """
                    SELECT 
                        typezone,
                        etiquette,
                        libelle,
                        libelong,
                        ST_Area(ST_Intersection(
                            geom_2154,
                            ST_SetSRID(
                                ST_GeomFromGeoJSON(%s),
                                2154
                            )
                        )) as intersection_area
                    FROM carto.plui_bordeaux_zonage
                    WHERE ST_Intersects(
                        geom_2154,
                        ST_SetSRID(
                            ST_GeomFromGeoJSON(%s),
                            2154
                        )
                    )
                    ORDER BY intersection_area DESC
                    LIMIT 1
                """
                geom_str = json.dumps(parcelle_geom)
                cur.execute(sql, (geom_str, geom_str))
                
                row = cur.fetchone()
                
                if not row:
                    results[parcelle_key] = {
                        "section": section,
                        "numero": numero,
                        "typezone": None,
                        "etiquette": None,
                        "libelle": None,
                        "libelong": None,
                        "message": "Parcelle hors zonage PLUi"
                    }
                else:
                    results[parcelle_key] = {
                        "section": section,
                        "numero": numero,
                        "typezone": row[0],
                        "etiquette": row[1],
                        "libelle": row[2],
                        "libelong": row[3],
                        "intersection_area": float(row[4]) if row[4] else 0
                    }
                    
            finally:
                cur.close()
                conn.close()
                
        except Exception as e:
            print(f"[zonage_plui_uf] Erreur pour parcelle {parcelle_key}: {e}")
            results[parcelle_key] = {
                "section": section,
                "numero": numero,
                "typezone": None,
                "etiquette": None,
                "libelle": None,
                "libelong": None,
                "message": f"Erreur lors de la récupération: {str(e)}"
            }
    
    # Résumé : compter les zonages uniques
    unique_zonages = set()
    for result in results.values():
        if result.get("etiquette"):
            unique_zonages.add(result["etiquette"])
    
    return {
        "insee": insee,
        "parcelles": list(results.values()),
        "summary": {
            "total_parcelles": len(parcelles),
            "parcelles_avec_zonage": sum(1 for r in results.values() if r.get("etiquette")),
            "zonages_uniques": list(unique_zonages),
            "zonages_multiples": len(unique_zonages) > 1
        }
    }