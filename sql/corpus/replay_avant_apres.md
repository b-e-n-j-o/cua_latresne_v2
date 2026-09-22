# Replay avant/après — règlement réellement injecté (lot 0.6)

## « parcelle AI 287 AI 299 »
session `7a8213ff-0da7-47ba-ac16-08610bb6d4fe` — refs `{"section": "AI", "numero": "287", "idu": null, "parcelles": [{"section": "AI", "numero": "287"}, {"section": "AI", "numero": "299"}], "idus": []}`
Zones PLU : ancien 1 / nouveau 1 — ['UA']
#### PLU UA (99.9%)
identique — 74c5fe604ff88d5e — 27236 caractères

Codes PPRI extra : —
get_reglement_ppri DG found=True texte_id=d077d520-339e-482e-986a-f4fce2a77188 chars=80014
#### PPRI DG
identique — 54db4dcf989d716e — 80014 caractères

Codes PPRMVT extra : —
get_reglement_pprmvt DG1 found=True texte_id=a2175480-f9c4-41c5-94f0-0128b09d7d2d chars=79589
#### PPRMVT DG1
identique — 12442b156ddd1af2 — 79589 caractères
get_reglement_pprmvt DG2 found=True texte_id=a0c1c621-f250-4c9d-970d-a5da655f7aac chars=30300
#### PPRMVT DG2
identique — ee08ae7ef03fa359 — 30300 caractères
get_reglement_pprmvt DG3 found=True texte_id=f34ce0b0-de51-4f36-9963-961deacd60e1 chars=38937
#### PPRMVT DG3
identique — 5c204a4f8b7a041e — 38937 caractères

## « zone agricole »
session `768f7286-f313-4775-90c3-34267bb50ca5` — refs `{"section": null, "numero": null, "idu": null, "parcelles": null, "idus": null}`
Pas de parcelle en session : pas de préchargement à comparer.

## « si je fais 4 terrains de paddle en interieur »
session `507dca68-4230-4b1a-a307-a02d4bc7548b` — refs `{"section": "AI", "numero": "359", "idu": null, "parcelles": [{"section": "AI", "numero": "359"}, {"section": "AI", "numero": "360"}, {"section": "AI", "numero": "361"}], "idus": []}`
Zones PLU : ancien 1 / nouveau 1 — ['UX']
#### PLU UX (100.0%)
identique — 5a18fe821d6156f8 — 18514 caractères

Codes PPRI extra : —
get_reglement_ppri DG found=True texte_id=d077d520-339e-482e-986a-f4fce2a77188 chars=80014
#### PPRI DG
identique — 54db4dcf989d716e — 80014 caractères

Codes PPRMVT extra : —
get_reglement_pprmvt DG1 found=True texte_id=a2175480-f9c4-41c5-94f0-0128b09d7d2d chars=79589
#### PPRMVT DG1
identique — 12442b156ddd1af2 — 79589 caractères
get_reglement_pprmvt DG2 found=True texte_id=a0c1c621-f250-4c9d-970d-a5da655f7aac chars=30300
#### PPRMVT DG2
identique — ee08ae7ef03fa359 — 30300 caractères
get_reglement_pprmvt DG3 found=True texte_id=f34ce0b0-de51-4f36-9963-961deacd60e1 chars=38937
#### PPRMVT DG3
identique — 5c204a4f8b7a041e — 38937 caractères

## Complément — question 9 (zone agricole, sans parcelle)
get_reglement_zone('A') ancien 22105 `8cc7b3153c680a86` / nouveau 22105 `8cc7b3153c680a86` texte_id=653ffebd-80c4-4e02-8cf2-ae2b00151691
identique

## Complément — PPRI zones intersectées (codes canoniques, comme le tool les attendait déjà)
### AI 287/299 codes=['ROUGE_CENTRE', 'ROUGE_NON_URBA']
DG found=True chars=80014
DG vs table : identique
- ROUGE_CENTRE found=True chars=16552 identique texte_id=04d43a9a-192b-44ed-be74-f337ab37bcbd
- ROUGE_NON_URBA found=True chars=21021 identique texte_id=38df0a24-caff-4286-b410-570ec0174d1c
### AI 359-361 (paddle) codes=['BLEUE', 'GRENAT', 'ROUGE_URBA']
DG found=True chars=80014
DG vs table : identique
- BLEUE found=True chars=16963 identique texte_id=198e3445-6590-4a9e-9cd0-856916804f0c
- GRENAT found=True chars=17396 identique texte_id=0ef65cbc-fa57-40fb-a43f-10d74cd2f0b9
- ROUGE_URBA found=True chars=20921 identique texte_id=d44b781b-22ee-4249-a3d0-683f9928bad3
