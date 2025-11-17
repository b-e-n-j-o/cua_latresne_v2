# -*- coding: utf-8 -*-
"""
generate_qr_with_logo.py — Génère un QR code PNG avec logo Kerelia au centre,
exactement comme dans les CUA.
"""

import os
import io
import qrcode
from PIL import Image

# ---------------------------
# 🔗 1. URL du CUA à encoder
# ---------------------------
URL = "https://www.kerelia.fr/cua?t=eyJkb2N4IjogInZpc3VhbGlzYXRpb24vN2ZuaU1zZHRjVlVGSzZLTmFzSlRzV21BMk4vQ1VBX3VuaXRlX2ZvbmNpZXJlLmRvY3gifQ=="

# ---------------------------
# 🖼️ 2. Chemin vers le logo Kerelia
# ---------------------------
LOGO_PATH = "./logos/logo_kerelia.png"

# ---------------------------
# 🟩 3. Fonction QR avec logo (repris de ton pipeline)
# ---------------------------
def make_qr_with_logo(text: str, logo_path: str, output_file: str):
    # QR haute correction pour supporter un logo
    qr = qrcode.QRCode(
        version=4,
        error_correction=qrcode.constants.ERROR_CORRECT_H,
        box_size=12,
        border=4,
    )
    qr.add_data(text)
    qr.make(fit=True)

    qr_img = qr.make_image(fill_color="black", back_color="white").convert("RGB")
    qr_width, qr_height = qr_img.size

    # Logo
    if logo_path and os.path.exists(logo_path):
        logo = Image.open(logo_path)
        if logo.mode != "RGBA":
            logo = logo.convert("RGBA")

        # Taille max = 35% du QR
        max_logo_size = int(qr_width * 0.35)
        logo_w, logo_h = logo.size

        # Redimension proportionnel
        if logo_w > logo_h:
            new_w = max_logo_size
            new_h = int(max_logo_size * (logo_h / logo_w))
        else:
            new_h = max_logo_size
            new_w = int(max_logo_size * (logo_w / logo_h))

        logo = logo.resize((new_w, new_h), Image.LANCZOS)

        # Fond blanc carré
        bg_size = max(new_w, new_h) + 8  # petite marge blanche
        background = Image.new("RGB", (bg_size, bg_size), "white")
        bg_x = (bg_size - new_w) // 2
        bg_y = (bg_size - new_h) // 2
        background.paste(logo, (bg_x, bg_y), logo)

        # Position centrale
        pos_x = (qr_width - bg_size) // 2
        pos_y = (qr_height - bg_size) // 2

        qr_img.paste(background, (pos_x, pos_y))

    # Export PNG
    qr_img.save(output_file)
    print(f"✔️ QR généré : {output_file}")


# ---------------------------
# ▶️ 4. Génération finale
# ---------------------------
if __name__ == "__main__":
    make_qr_with_logo(
        text=URL,
        logo_path=LOGO_PATH,
        output_file="qr_cua_kerelia.png"
    )
