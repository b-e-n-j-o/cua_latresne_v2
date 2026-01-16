from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
import requests
import geopandas as gpd
from shapely.geometry import Point
import io
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.lib.enums import TA_CENTER
from datetime import datetime
import html

router = APIRouter()


def nettoyer_texte(texte):
    """Nettoie les problèmes d'encodage"""
    if not texte or texte == 'N/A':
        return texte
    
    corrections = {
        'â€™': "'", 'â€œ': '"', 'â€': '"', 'Ã©': 'é', 'Ã¨': 'è',
        'Ãª': 'ê', 'Ã ': 'à', 'Ã§': 'ç', 'Ã´': 'ô', 'Ã®': 'î',
        'Ã»': 'û', 'Ã¹': 'ù', 'Ã«': 'ë', 'Ã¯': 'ï', 'Ã¼': 'ü',
        'â€': ' ', 'dâ€™': "d'", 'lâ€™': "l'"
    }
    
    texte_clean = str(texte)
    for mauvais, bon in corrections.items():
        texte_clean = texte_clean.replace(mauvais, bon)
    
    try:
        texte_clean = html.unescape(texte_clean)
    except:
        pass
    
    return texte_clean


def get_parcelle_geometry(code_insee: str, section: str, numero: str):
    """Récupère géométrie parcelle via WFS IGN"""
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": "CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle",
        "srsName": "EPSG:4326",
        "outputFormat": "application/json",
        "CQL_FILTER": f"code_insee='{code_insee}' AND section='{section}' AND numero='{numero}'"
    }
    
    r = requests.get("https://data.geopf.fr/wfs/ows", params=params, timeout=15)
    r.raise_for_status()
    
    gdf = gpd.read_file(io.BytesIO(r.content))
    if len(gdf) == 0:
        raise ValueError("Parcelle introuvable")
    
    return gdf.iloc[0].geometry, gdf.iloc[0].get('contenance', 'N/A')


def fetch_dpe_commune(code_insee: str):
    """Récupère tous les DPE de la commune"""
    params = {'q': f'code_insee_ban:{code_insee}', 'size': 1000}
    r = requests.get(
        "https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines",
        params=params, timeout=15
    )
    r.raise_for_status()
    return r.json().get('results', [])


def spatial_intersection(dpe_list, parcelle_geom):
    """Intersection spatiale DPE x Parcelle"""
    parcelle_buffer = parcelle_geom.buffer(0.0001)
    dpe_in_parcelle = []
    
    for dpe in dpe_list:
        geopoint = dpe.get('_geopoint')
        if not geopoint:
            continue
        try:
            lat, lon = map(float, geopoint.split(','))
            if parcelle_buffer.contains(Point(lon, lat)):
                dpe_in_parcelle.append(dpe)
        except:
            continue
    
    return dpe_in_parcelle


def generer_rapport_pdf_exhaustif(dpe_data, section, numero, code_insee, surface_parcelle):
    """Génère le PDF exhaustif en mémoire"""
    if not dpe_data:
        raise ValueError("Aucune donnée DPE")
    
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4,
                           rightMargin=2*cm, leftMargin=2*cm,
                           topMargin=2*cm, bottomMargin=2*cm)
    
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'CustomTitle', parent=styles['Heading1'], fontSize=18,
        textColor=colors.HexColor('#1a5490'), spaceAfter=30, alignment=TA_CENTER
    )
    heading1_style = ParagraphStyle(
        'CustomHeading1', parent=styles['Heading1'], fontSize=14,
        textColor=colors.HexColor('#1a5490'), spaceAfter=12, spaceBefore=16
    )
    heading2_style = ParagraphStyle(
        'CustomHeading2', parent=styles['Heading2'], fontSize=11,
        textColor=colors.HexColor('#2c5f8d'), spaceAfter=8, spaceBefore=12
    )
    
    story = []
    dpe = dpe_data[0]
    
    # Nettoyer textes
    dpe_clean = {}
    for key, value in dpe.items():
        dpe_clean[key] = nettoyer_texte(value) if isinstance(value, str) else value
    dpe = dpe_clean
    
    # Calculs
    surface_logement = dpe.get('surface_habitable_logement', 0)
    cout_total = dpe.get('cout_total_5_usages', 0)
    cout_par_m2 = round(cout_total / surface_logement, 2) if surface_logement else 0
    
    conso_chauffage_ep = dpe.get('conso_chauffage_ep', 0)
    conso_ecs_ep = dpe.get('conso_ecs_ep', 0)
    conso_eclairage_ep = dpe.get('conso_eclairage_ep', 0)
    conso_total_ep = dpe.get('conso_5_usages_ep', 1)
    
    part_chauffage = round((conso_chauffage_ep / conso_total_ep * 100), 0) if conso_total_ep else 0
    part_ecs = round((conso_ecs_ep / conso_total_ep * 100), 0) if conso_total_ep else 0
    part_eclairage = round((conso_eclairage_ep / conso_total_ep * 100), 0) if conso_total_ep else 0
    
    # === TITRE ===
    story.append(Paragraph("RAPPORT DE DIAGNOSTIC DE PERFORMANCE ÉNERGÉTIQUE", title_style))
    story.append(Spacer(1, 0.5*cm))
    
    # === PARCELLE ===
    story.append(Paragraph("IDENTIFICATION DE LA PARCELLE", heading1_style))
    table = Table([
        ['Section cadastrale', section],
        ['Numéro de parcelle', numero],
        ['Code INSEE', code_insee],
        ['Surface de la parcelle', f"{surface_parcelle} m²"],
        ['Nombre de logements', str(len(dpe_data))]
    ], colWidths=[8*cm, 8*cm])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#e8f4f8')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey)
    ]))
    story.append(table)
    story.append(Spacer(1, 0.5*cm))
    
    # === LOGEMENT ===
    story.append(Paragraph("IDENTIFICATION DU LOGEMENT", heading1_style))
    table = Table([
        ['Adresse', dpe.get('adresse_ban', 'N/A')],
        ['Type de bien', dpe.get('type_batiment', 'N/A').capitalize()],
        ['Surface habitable', f"{dpe.get('surface_habitable_logement', 'N/A')} m²"],
        ['Année de construction', f"{dpe.get('annee_construction', 'N/A')} ({dpe.get('periode_construction', 'N/A')})"],
        ['Nombre de niveaux', str(dpe.get('nombre_niveau_logement', 'N/A'))],
        ['Hauteur sous plafond', f"{dpe.get('hauteur_sous_plafond', 'N/A')} m"],
        ['Zone climatique', dpe.get('zone_climatique', 'N/A')],
        ['Altitude', dpe.get('classe_altitude', 'N/A')],
        ['Coordonnées GPS', dpe.get('_geopoint', 'N/A')]
    ], colWidths=[8*cm, 8*cm])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#e8f4f8')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey)
    ]))
    story.append(table)
    story.append(Spacer(1, 0.5*cm))
    
    # === PERFORMANCE ===
    story.append(Paragraph("PERFORMANCE ÉNERGÉTIQUE GLOBALE", heading1_style))
    story.append(Paragraph("Étiquettes", heading2_style))
    
    table = Table([
        ['Indicateur', 'Valeur', 'Classe'],
        ['DPE', f"{dpe.get('conso_5_usages_par_m2_ep', 'N/A')} kWh/m²/an", dpe.get('etiquette_dpe', 'N/A')],
        ['Émissions GES', f"{dpe.get('emission_ges_5_usages_par_m2', 'N/A')} kg CO2/m²/an", dpe.get('etiquette_ges', 'N/A')]
    ], colWidths=[5*cm, 6*cm, 5*cm])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('BACKGROUND', (2, 1), (2, -1), colors.HexColor('#fff3cd')),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTNAME', (2, 1), (2, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey)
    ]))
    story.append(table)
    story.append(Spacer(1, 0.5*cm))
    
    # === CONSOMMATIONS ===
    story.append(Paragraph("Consommations annuelles", heading2_style))
    table = Table([
        ['Usage', 'Énergie Primaire (kWh)', 'Énergie Finale (kWh)', 'Part'],
        ['Chauffage', str(dpe.get('conso_chauffage_ep', 'N/A')), 
         str(dpe.get('conso_chauffage_ef', 'N/A')), f"{part_chauffage}%"],
        ['Eau chaude sanitaire', str(dpe.get('conso_ecs_ep', 'N/A')), 
         str(dpe.get('conso_ecs_ef', 'N/A')), f"{part_ecs}%"],
        ['Éclairage', str(dpe.get('conso_eclairage_ep', 'N/A')), 
         str(dpe.get('conso_eclairage_ef', 'N/A')), f"{part_eclairage}%"],
        ['Auxiliaires', str(dpe.get('conso_auxiliaires_ep', 'N/A')), 
         str(dpe.get('conso_auxiliaires_ef', 'N/A')), '<1%'],
        ['Refroidissement', str(dpe.get('conso_refroidissement_ep', 'N/A')), 
         str(dpe.get('conso_refroidissement_ef', 'N/A')), '0%'],
        ['TOTAL', str(dpe.get('conso_5_usages_ep', 'N/A')), 
         str(dpe.get('conso_5_usages_ef', 'N/A')), '100%']
    ], colWidths=[4.5*cm, 4*cm, 4*cm, 3.5*cm])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#e8f4f8')),
        ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('PADDING', (0, 0), (-1, -1), 6),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey)
    ]))
    story.append(table)
    story.append(Spacer(1, 0.3*cm))
    story.append(Paragraph(
        f"<b>Consommation par m²</b> : {dpe.get('conso_5_usages_par_m2_ep', 'N/A')} kWh EP/m²/an",
        styles['Normal']
    ))
    story.append(Spacer(1, 0.5*cm))
    
    # === COÛTS ===
    story.append(Paragraph("Coûts énergétiques annuels estimés", heading2_style))
    table = Table([
        ['Poste', 'Coût annuel'],
        ['Chauffage', f"{dpe.get('cout_chauffage', 'N/A')} €"],
        ['Eau chaude sanitaire', f"{dpe.get('cout_ecs', 'N/A')} €"],
        ['Éclairage', f"{dpe.get('cout_eclairage', 'N/A')} €"],
        ['Auxiliaires', f"{dpe.get('cout_auxiliaires', 'N/A')} €"],
        ['TOTAL', f"{dpe.get('cout_total_5_usages', 'N/A')} €/an"]
    ], colWidths=[10*cm, 6*cm])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#e8f4f8')),
        ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey)
    ]))
    story.append(table)
    story.append(Spacer(1, 0.3*cm))
    story.append(Paragraph(f"<b>Soit {cout_par_m2} €/m²/an</b>", styles['Normal']))
    story.append(Spacer(1, 0.5*cm))
    
    # === GES ===
    story.append(Paragraph("Émissions de gaz à effet de serre", heading2_style))
    table = Table([
        ['Poste', 'Émissions (kg CO2/an)'],
        ['Chauffage', str(dpe.get('emission_ges_chauffage', 'N/A'))],
        ['Eau chaude sanitaire', str(dpe.get('emission_ges_ecs', 'N/A'))],
        ['Éclairage', str(dpe.get('emission_ges_eclairage', 'N/A'))],
        ['TOTAL', str(dpe.get('emission_ges_5_usages', 'N/A'))]
    ], colWidths=[10*cm, 6*cm])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#e8f4f8')),
        ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey)
    ]))
    story.append(table)
    story.append(Spacer(1, 0.3*cm))
    story.append(Paragraph(
        f"<b>Soit {dpe.get('emission_ges_5_usages_par_m2', 'N/A')} kg CO2/m²/an</b>",
        styles['Normal']
    ))
    
    # === PAGE 2 : BÂTI ===
    story.append(PageBreak())
    story.append(Paragraph("QUALITÉ DU BÂTI", heading1_style))
    story.append(Paragraph("Isolation thermique", heading2_style))
    
    table = Table([
        ['Élément', 'Qualité'],
        ['Enveloppe globale', dpe.get('qualite_isolation_enveloppe', 'N/A').capitalize()],
        ['Murs', dpe.get('qualite_isolation_murs', 'N/A').capitalize()],
        ['Menuiseries', dpe.get('qualite_isolation_menuiseries', 'N/A').capitalize()],
        ['Plancher bas', dpe.get('qualite_isolation_plancher_bas', 'N/A').capitalize()],
        ['Combles aménagés', dpe.get('qualite_isolation_plancher_haut_comble_amenage', 'N/A').capitalize()]
    ], colWidths=[10*cm, 6*cm])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('BACKGROUND', (0, 1), (0, -1), colors.HexColor('#e8f4f8')),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey)
    ]))
    story.append(table)
    story.append(Spacer(1, 0.3*cm))
    story.append(Paragraph(
        f"<b>Coefficient Ubat</b> : {dpe.get('ubat_w_par_m2_k', 'N/A')} W/m²/K",
        styles['Normal']
    ))
    story.append(Spacer(1, 0.5*cm))
    
    # === DÉPERDITIONS ===
    story.append(Paragraph("Déperditions thermiques (en W/K)", heading2_style))
    table = Table([
        ['Élément', 'Déperdition'],
        ['Enveloppe totale', str(dpe.get('deperditions_enveloppe', 'N/A'))],
        ['Murs', str(dpe.get('deperditions_murs', 'N/A'))],
        ['Planchers bas', str(dpe.get('deperditions_planchers_bas', 'N/A'))],
        ['Planchers hauts', str(dpe.get('deperditions_planchers_hauts', 'N/A'))],
        ['Baies vitrées', str(dpe.get('deperditions_baies_vitrees', 'N/A'))],
        ['Portes', str(dpe.get('deperditions_portes', 'N/A'))],
        ['Ponts thermiques', str(dpe.get('deperditions_ponts_thermiques', 'N/A'))],
        ['Renouvellement d\'air', str(dpe.get('deperditions_renouvellement_air', 'N/A'))]
    ], colWidths=[10*cm, 6*cm])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('BACKGROUND', (0, 1), (0, -1), colors.HexColor('#e8f4f8')),
        ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey)
    ]))
    story.append(table)
    story.append(Spacer(1, 0.5*cm))
    
    # === INERTIE ===
    story.append(Paragraph("Inertie et confort", heading2_style))
    table = Table([
        ['Critère', 'Valeur'],
        ['Classe d\'inertie', dpe.get('classe_inertie_batiment', 'N/A')],
        ['Ventilation post-2012', 'Oui' if dpe.get('ventilation_posterieure_2012') else 'Non'],
        ['Apports solaires (hiver)', f"{dpe.get('apport_solaire_saison_chauffe', 'N/A')} kWh"],
        ['Apports internes (hiver)', f"{dpe.get('apport_interne_saison_chauffe', 'N/A')} kWh"]
    ], colWidths=[10*cm, 6*cm])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#e8f4f8')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey)
    ]))
    story.append(table)
    
    # === PAGE 3 : CHAUFFAGE ===
    story.append(PageBreak())
    story.append(Paragraph("SYSTÈME DE CHAUFFAGE", heading1_style))
    story.append(Paragraph("Installation n°1 (principale)", heading2_style))
    
    table = Table([
        ['Caractéristique', 'Description'],
        ['Type d\'installation', dpe.get('type_installation_chauffage_n1', 'N/A').capitalize()],
        ['Configuration', dpe.get('configuration_installation_chauffage_n1', 'N/A')],
        ['Générateur principal', dpe.get('type_generateur_chauffage_principal', 'N/A')],
        ['Énergie', dpe.get('type_energie_principale_chauffage', 'N/A')],
        ['Émetteur', dpe.get('type_emetteur_installation_chauffage_n1', 'N/A')[:60]],
        ['Surface chauffée', f"{dpe.get('surface_chauffee_installation_chauffage_n1', 'N/A')} m²"],
        ['Consommation', f"{dpe.get('conso_chauffage_ef', 'N/A')} kWh/an"],
        ['Usage', dpe.get('usage_generateur_n1_installation_n1', 'N/A').capitalize()]
    ], colWidths=[8*cm, 8*cm])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#e8f4f8')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('VALIGN', (0, 0), (-1, -1), 'TOP')
    ]))
    story.append(table)
    story.append(Spacer(1, 0.3*cm))
    
    desc_chauffage = dpe.get('description_installation_chauffage_n1', 'N/A')
    story.append(Paragraph(f"<b>Description détaillée :</b> {desc_chauffage}", styles['Normal']))
    story.append(Spacer(1, 0.5*cm))
    
    story.append(Paragraph("Besoins théoriques", heading2_style))
    table = Table([
        ['Besoin', 'Valeur'],
        ['Besoin de chauffage', f"{dpe.get('besoin_chauffage', 'N/A')} kWh/an"],
        ['Besoin de refroidissement', f"{dpe.get('besoin_refroidissement', 'N/A')} kWh/an"]
    ], colWidths=[10*cm, 6*cm])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#e8f4f8')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey)
    ]))
    story.append(table)
    
    # === ECS ===
    story.append(Spacer(1, 0.5*cm))
    story.append(Paragraph("SYSTÈME EAU CHAUDE SANITAIRE (ECS)", heading1_style))
    story.append(Paragraph("Installation n°1", heading2_style))
    
    table = Table([
        ['Caractéristique', 'Description'],
        ['Type d\'installation', dpe.get('type_installation_ecs_n1', 'N/A').capitalize()],
        ['Configuration', dpe.get('configuration_installation_ecs_n1', 'N/A')],
        ['Générateur', dpe.get('type_generateur_n1_ecs_n1', 'N/A')],
        ['Énergie', dpe.get('type_energie_principale_ecs', 'N/A')],
        ['Volume de stockage', f"{dpe.get('volume_stockage_generateur_n1_ecs_n1', 'N/A')} litres"],
        ['Surface desservie', f"{dpe.get('surface_habitable_desservie_par_installation_ecs_n1', 'N/A')} m²"],
        ['Consommation', f"{dpe.get('conso_ecs_ef', 'N/A')} kWh/an"],
        ['Besoin théorique', f"{dpe.get('besoin_ecs', 'N/A')} kWh/an"]
    ], colWidths=[8*cm, 8*cm])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#e8f4f8')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('VALIGN', (0, 0), (-1, -1), 'TOP')
    ]))
    story.append(table)
    story.append(Spacer(1, 0.3*cm))
    
    desc_ecs = dpe.get('description_installation_ecs_n1', 'N/A')
    story.append(Paragraph(f"<b>Description détaillée :</b> {desc_ecs}", styles['Normal']))
    
    # === PAGE 4 : ENR & ADMIN ===
    story.append(PageBreak())
    story.append(Paragraph("ÉNERGIES RENOUVELABLES & CONFORT", heading1_style))
    
    table = Table([
        ['Critère', 'Valeur'],
        ['Production photovoltaïque', f"{dpe.get('production_electricite_pv_kwhep_par_an', 0)} kWh/an"],
        ['Type d\'installation solaire', dpe.get('type_installation_solaire_n1', 'N/A')]
    ], colWidths=[10*cm, 6*cm])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#e8f4f8')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey)
    ]))
    story.append(table)
    
    story.append(Spacer(1, 0.5*cm))
    story.append(Paragraph("INFORMATIONS ADMINISTRATIVES", heading1_style))
    
    table = Table([
        ['Information', 'Valeur'],
        ['Numéro DPE', dpe.get('numero_dpe', 'N/A')],
        ['Date de visite', dpe.get('date_visite_diagnostiqueur', 'N/A')],
        ['Date d\'établissement', dpe.get('date_etablissement_dpe', 'N/A')],
        ['Date de réception', dpe.get('date_reception_dpe', 'N/A')],
        ['Date de validité', dpe.get('date_fin_validite_dpe', 'N/A')],
        ['Dernière modification', dpe.get('date_derniere_modification_dpe', 'N/A')],
        ['Version DPE', str(dpe.get('version_dpe', 'N/A'))],
        ['Modèle', dpe.get('modele_dpe', 'N/A')],
        ['Méthode', dpe.get('methode_application_dpe', 'N/A').capitalize()]
    ], colWidths=[8*cm, 8*cm])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#e8f4f8')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey)
    ]))
    story.append(table)
    
    story.append(Spacer(1, 0.5*cm))
    story.append(Paragraph("DONNÉES TECHNIQUES COMPLÉMENTAIRES", heading1_style))
    story.append(Paragraph("Géocodage BAN", heading2_style))
    
    table = Table([
        ['Critère', 'Valeur'],
        ['Identifiant BAN', dpe.get('identifiant_ban', 'N/A')],
        ['Statut', dpe.get('statut_geocodage', 'N/A')],
        ['Score BAN', str(dpe.get('score_ban', 'N/A'))],
        ['Coordonnées Lambert 93', f"X: {dpe.get('coordonnee_cartographique_x_ban', 'N/A')}, "
                                    f"Y: {dpe.get('coordonnee_cartographique_y_ban', 'N/A')}"]
    ], colWidths=[8*cm, 8*cm])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#e8f4f8')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey)
    ]))
    story.append(table)
    story.append(Spacer(1, 0.5*cm))
    
    story.append(Paragraph("Classification", heading2_style))
    table = Table([
        ['Critère', 'Valeur'],
        ['Département', dpe.get('code_departement_ban', 'N/A')],
        ['Région', dpe.get('code_region_ban', 'N/A')],
        ['Code postal', dpe.get('code_postal_ban', 'N/A')]
    ], colWidths=[8*cm, 8*cm])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#e8f4f8')),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('PADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey)
    ]))
    story.append(table)
    
    # === FOOTER ===
    story.append(Spacer(1, 1*cm))
    story.append(Paragraph(
        f"Rapport généré automatiquement le {datetime.now().strftime('%d/%m/%Y à %H:%M')}",
        styles['Normal']
    ))
    story.append(Paragraph(
        f"<i>Ce diagnostic de performance énergétique est valable jusqu'au {dpe.get('date_fin_validite_dpe', 'N/A')}</i>",
        styles['Italic']
    ))
    
    doc.build(story)
    buffer.seek(0)
    return buffer


@router.get("/rapport-dpe/exists/{code_insee}/{section}/{numero}")
async def check_dpe_exists(code_insee: str, section: str, numero: str):
    """
    Vérifie si un DPE existe pour une parcelle donnée.
    Retourne un booléen sans générer le PDF.
    """
    try:
        parcelle_geom, _ = get_parcelle_geometry(code_insee, section, numero)
        dpe_list = fetch_dpe_commune(code_insee)
        dpe_in_parcelle = spatial_intersection(dpe_list, parcelle_geom)
        
        return {
            "exists": len(dpe_in_parcelle) > 0,
            "count": len(dpe_in_parcelle),
            "code_insee": code_insee,
            "section": section,
            "numero": numero
        }
        
    except Exception as e:
        # En cas d'erreur, on retourne False plutôt que de lever une exception
        return {
            "exists": False,
            "count": 0,
            "code_insee": code_insee,
            "section": section,
            "numero": numero,
            "error": str(e)
        }


@router.post("/rapport-dpe")
async def generer_rapport_dpe(data: dict):
    """Génère rapport DPE PDF exhaustif pour une parcelle"""
    try:
        code_insee = data["code_insee"]
        section = data["section"]
        numero = data["numero"]
        
        parcelle_geom, surface = get_parcelle_geometry(code_insee, section, numero)
        dpe_list = fetch_dpe_commune(code_insee)
        dpe_in_parcelle = spatial_intersection(dpe_list, parcelle_geom)
        
        if not dpe_in_parcelle:
            raise HTTPException(status_code=404, detail="Aucun DPE trouvé pour cette parcelle")
        
        pdf_buffer = generer_rapport_pdf_exhaustif(
            dpe_in_parcelle, section, numero, code_insee, surface
        )
        
        return Response(
            content=pdf_buffer.getvalue(),
            media_type="application/pdf",
            headers={"Content-Disposition": f"attachment; filename=DPE_{section}_{numero}.pdf"}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))