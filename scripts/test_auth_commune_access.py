#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test bout-en-bout auth Kerelia CUA — login Supabase → JWT → endpoints protégés.

Usage :
    cd BACKEND_PRINCIPAL/LATRESNE/cua_latresne_v4
    python scripts/test_auth_commune_access.py \\
        --email agent@mairie.fr \\
        --password '***'

    # Contre la prod Render :
    python scripts/test_auth_commune_access.py --prod --email ... --password '***'

    # Équivalent pytest (recommandé) :
    cp .env.test.example .env.test.local   # puis remplir AUTH_TEST_*
    pytest tests/smoke -v
    pytest tests/smoke --prod -v

Config :
    .env              — app (SUPABASE_URL, clés…)
    .env.test.local   — identifiants tests uniquement (gitignoré)
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tests.test_env import load_all_test_env  # noqa: E402

load_all_test_env()

from tests.smoke.auth_e2e import (  # noqa: E402
    DEFAULT_API_BASE,
    PROD_API_BASE,
    AuthE2EError,
    run_checks,
    supabase_sign_in,
)


def _print_results(session: dict, api_base: str, results) -> int:
    print()
    print("=== Session Supabase ===")
    print(f"  API base     : {api_base}")
    print(f"  user_id      : {session['user_id']}")
    print(f"  email        : {session.get('email')}")
    print(f"  expires_in   : {session.get('expires_in')}s")
    print()
    print("=== Résultats ===")
    failed = 0
    for r in results:
        mark = "OK" if r.ok else "FAIL"
        print(f"  [{mark:4}] {r.name}")
        print(f"         {r.detail}")
        if not r.ok:
            failed += 1
    print()
    if failed:
        print(f"ÉCHEC — {failed} test(s) en erreur.")
        return 1
    print("SUCCÈS — auth bout-en-bout OK.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Test auth Kerelia : login Supabase + endpoints protégés."
    )
    parser.add_argument("--email", default=os.getenv("AUTH_TEST_EMAIL", "").strip())
    parser.add_argument("--password", default=os.getenv("AUTH_TEST_PASSWORD", "").strip())
    parser.add_argument("--prod", action="store_true", help=f"Cible {PROD_API_BASE}")
    parser.add_argument("--api-base", default=None)
    parser.add_argument("--expect-superadmin", action="store_true")
    parser.add_argument(
        "--commune-slug",
        default=os.getenv("AUTH_TEST_COMMUNE_SLUG", "").strip() or None,
    )
    args = parser.parse_args()

    if not args.email or not args.password:
        parser.error("--email et --password requis (ou AUTH_TEST_EMAIL / AUTH_TEST_PASSWORD)")
    if args.api_base and args.prod:
        parser.error("--prod et --api-base sont mutuellement exclusifs.")

    if args.prod:
        api_base = PROD_API_BASE
    elif args.api_base:
        api_base = args.api_base.rstrip("/")
    else:
        api_base = DEFAULT_API_BASE

    expect_superadmin = args.expect_superadmin or (
        os.getenv("AUTH_TEST_EXPECT_SUPERADMIN", "").strip().lower()
        in ("1", "true", "yes", "on")
    )

    try:
        print(f"Connexion Supabase ({args.email})…")
        session = supabase_sign_in(args.email, args.password)
    except AuthE2EError as e:
        print(f"Erreur : {e}", file=sys.stderr)
        return 1

    print("Login OK, lancement des checks API…")
    results = run_checks(
        api_base,
        session,
        expect_superadmin=expect_superadmin,
        commune_slug=args.commune_slug,
    )
    return _print_results(session, api_base, results)


if __name__ == "__main__":
    sys.exit(main())
