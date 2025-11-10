#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_auth.py — Vérification des droits INSEE utilisateurs Supabase
"""

import os
from dotenv import load_dotenv

# ✅ Charger l'environnement en premier
load_dotenv()

# 🔧 Facultatif : affichage de contrôle
print("🔧 SUPABASE_URL =", os.getenv("SUPABASE_URL"))
print("🔧 SERVICE_KEY (tronqué) =", os.getenv("SERVICE_KEY", "")[:12] + "...")

# ✅ Maintenant on peut importer (le client Supabase pourra se créer)
from auth_utils import get_user_insee_list, is_authorized_for_insee

# 🧍 ID utilisateur à tester
USER_ID = "55c68f76-419b-4951-ba5c-6c9bfa202899"

def main():
    rights = get_user_insee_list(USER_ID)
    print(f"\n🔎 Droits INSEE de l'utilisateur {USER_ID} : {rights or 'Aucune restriction'}")

    tests = ["33234", "33531", "33063"]
    for code in tests:
        authorized = is_authorized_for_insee(USER_ID, code)
        print(f"🧩 Commune {code} → {'✅ autorisé' if authorized else '⛔ refusé'}")

if __name__ == "__main__":
    main()
