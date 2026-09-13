#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Compatibilité : la logique vit dans api.veille_sig.cadastre.sync_or_add_parcelles.

    PYTHONPATH=. python -m api.veille_sig.cadastre.sync_or_add_parcelles --insee 66008 --schema argeles
"""

from api.veille_sig.cadastre.sync_or_add_parcelles import main

if __name__ == "__main__":
    main()
