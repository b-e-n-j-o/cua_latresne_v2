# -*- coding: utf-8 -*-
"""
Intersections PARCELLE (schema latresne) → Supabase/PostGIS

- Les variables de connexion sont chargées depuis le fichier .env
- Le chemin de sortie HTML et le chemin du JSON catalogue sont définis en dur.
- Le catalogue des couches est chargé depuis un fichier externe JSON (catalogue_couches.json).
"""

import os, json, logging
from typing import Dict, Any, List
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from jinja2 import Template

# ======================= CONFIGURATION =======================
CATALOGUE_PATH = "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/DATA_FLAVIO/CATALOGUE/catalogue_couches.json"
HTML_OUT_PATH = "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTIONS/rapport_parcelle.html"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("intersections")

# ======================= CONNEXION DB =======================

def get_engine() -> Engine:
    load_dotenv()
    host = os.getenv("SUPABASE_HOST")
    user = os.getenv("SUPABASE_USER")
    pwd = os.getenv("SUPABASE_PASSWORD")
    db = os.getenv("SUPABASE_DB")
    port = os.getenv("SUPABASE_PORT", "5432")
    dsn = f"postgresql+psycopg2://{user}:{quote_plus(pwd)}@{host}:{port}/{db}?sslmode=verify-full"
    eng = create_engine(dsn, pool_pre_ping=True, pool_recycle=300, connect_args={"connect_timeout": 20})
    with eng.begin() as con:
        con.execute(text("select 1"))
    return eng

# ======================= CATALOGUE =======================

def load_catalogue(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    layers = []
    for rubrique, tables in raw.items():
        for table, conf in tables.items():
            layers.append({
                "table": table,
                "label": conf.get("nom", table),
                "geom_col": conf.get("geom", "geom_2154"),
                "keep": conf.get("keep", [])
            })
    return layers

# ======================= SQL =======================
SQL_GET_PARCEL = """
SELECT section, numero, geom_2154, ST_Area(geom_2154) AS area, ST_NumInteriorRings(geom_2154) AS holes
FROM latresne.parcelles_latresne
WHERE section = :section AND numero = :numero;
"""

SQL_CARVE = """
WITH raw AS (SELECT ST_MakeValid(:g) AS g),
     dump AS (SELECT (ST_Dump(ST_CollectionExtract(g, 3))).geom AS g FROM raw),
     outer_only AS (SELECT ST_BuildArea(ST_ExteriorRing(g)) AS g FROM dump)
SELECT ST_UnaryUnion(ST_Collect(g)) AS g_carved, ST_Area(ST_UnaryUnion(ST_Collect(g))) AS area_carved;
"""

SQL_INTERSECT = """
SET LOCAL statement_timeout = '45s';
WITH p AS (SELECT :g_parcel::geometry AS g, :parcel_area::double precision AS a)
SELECT SUM(ST_Area(ST_CollectionExtract(ST_Intersection(t.{geom_col}, p.g), 3))) AS inter_area_m2,
       ARRAY_AGG(DISTINCT t.reglementation) FILTER (WHERE t.reglementation IS NOT NULL) AS regs
FROM latresne.{table} t, p
WHERE t.{geom_col} IS NOT NULL AND t.{geom_col} && ST_Envelope(p.g) AND ST_Intersects(t.{geom_col}, p.g);
"""

SQL_EXISTS = "SELECT to_regclass(:fqname) IS NOT NULL;"

# ======================= HTML TEMPLATE =======================
HTML_TPL = Template("""
<!doctype html><html lang='fr'><head><meta charset='utf-8'><title>Intersections parcelle {{ section }} {{ numero }}</title>
<style>
body {font-family: sans-serif; margin: 20px;}
table {border-collapse: collapse; width: 100%;}
th, td {border: 1px solid #ccc; padding: 8px;}
</style></head><body>
<h2>Intersections — Parcelle {{ section }} {{ numero }}</h2>
<p>Surface parcelle: {{ area|round(2) }} m²</p>
<table>
<tr><th>Couche</th><th>Surface intersectée (m²)</th><th>%</th><th>Réglementation</th></tr>
{% for r in rows %}
<tr>
<td>{{ r.label }}</td>
<td>{{ r.area|round(2) }}</td>
<td>{{ (r.pct*100)|round(2) }}%</td>
<td>{% if r.regs %}{{ r.regs|join(', ') }}{% else %}-{% endif %}</td>
</tr>
{% endfor %}
</table>
</body></html>
""")

# ======================= FONCTIONS =======================

def table_exists(eng, fqname: str) -> bool:
    with eng.begin() as con:
        return bool(con.execute(text(SQL_EXISTS), {"fqname": fqname}).scalar())

def carve_geom(eng, g):
    with eng.begin() as con:
        r = con.execute(text(SQL_CARVE), {"g": g}).first()
    return r[0], float(r[1] or 0.0)

def intersect_layer(eng, layer, g, area):
    fq = f"latresne.{layer['table']}"
    if not table_exists(eng, fq):
        logger.warning(f"⚠️ Table absente: {fq}")
        return {"label": layer['label'], "area": 0, "pct": 0, "regs": []}
    sql = SQL_INTERSECT.format(table=layer['table'], geom_col=layer['geom_col'])
    with eng.begin() as con:
        row = con.execute(text(sql), {"g_parcel": g, "parcel_area": area}).mappings().first()
    inter_area = float(row['inter_area_m2'] or 0)
    pct = inter_area / area if area > 0 else 0
    regs = row.get('regs') or []
    return {"label": layer['label'], "area": inter_area, "pct": pct, "regs": regs}

# ======================= MAIN =======================

def main(section: str = None, numero: str = None):
    eng = get_engine()
    layers = load_catalogue(CATALOGUE_PATH)

    if not section or not numero:
        section = input("Section: ").upper().strip()
        numero = input("Numéro: ").zfill(4)

    with eng.begin() as con:
        parcelle = con.execute(text(SQL_GET_PARCEL), {"section": section, "numero": numero}).mappings().first()
    if not parcelle:
        raise SystemExit(f"Parcelle {section} {numero} introuvable")

    geom = parcelle['geom_2154']
    area_raw = parcelle['area']
    holes = parcelle['holes'] > 0

    g_carved, area_carved = carve_geom(eng, geom)

    rows = []
    for layer in layers:
        r = intersect_layer(eng, layer, g_carved, area_carved)
        if r['area'] > 0:
            logger.info(f"→ {layer['table']}: {r['area']:.2f} m² ({r['pct']*100:.2f}%)")
        rows.append(r)

    html = HTML_TPL.render(section=section, numero=numero, area=area_carved, rows=rows)
    with open(HTML_OUT_PATH, 'w', encoding='utf-8') as f:
        f.write(html)
    logger.info(f"✅ Rapport généré: {HTML_OUT_PATH}")
    return HTML_OUT_PATH

if __name__ == '__main__':
    main()
