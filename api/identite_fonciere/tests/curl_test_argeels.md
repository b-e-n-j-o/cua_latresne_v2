curl -sS -X POST "http://localhost:8000/api/identite-fonciere/rapport" \
  -H "Content-Type: application/json" \
  -d '{
    "commune": "Argelès-sur-Mer",
    "insee": "66008",
    "idu": "66008000AB0016",
    "db_schema": "argeles",
    "parcelles_cadastrales": [{"section": "AB", "numero": "16"}]
  }' \
  -o rapport_test.pdf