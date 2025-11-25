# email_utils.py
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")

INTERNAL_EMAIL = os.getenv("INTERNAL_NOTIFICATION_EMAIL")  # ex: contact@kerelia.fr
FROM_EMAIL = os.getenv("FROM_EMAIL")  # ex: hello@kerelia.fr


# ======================================================
# 📩 EMAIL 1 : NOTIFICATION INTERNE (déjà existant)
# ======================================================
def send_internal_email(lead: dict):
    """
    Envoie un email interne pour notifier un lead.
    Fonction appelée depuis l'endpoint /lead.
    """

    if not SENDGRID_API_KEY:
        print("⚠️ Aucun SENDGRID_API_KEY défini — email non envoyé.")
        return

    subject = f"🔥 Nouveau lead Kerelia — {lead.get('profile', 'Profil inconnu')}"
    content = f"""
    Nouveau lead Kerelia reçu :

    • Profil : {lead.get('profile')}
    • Email : {lead.get('email')}
    • Commune : {lead.get('commune')}
    • Parcelle : {lead.get('parcelle', 'Aucune')}
    • Message : {lead.get('message', 'Aucun message')}

    Enregistré automatiquement depuis la landing page.
    """

    message = Mail(
        from_email=INTERNAL_EMAIL,
        to_emails=INTERNAL_EMAIL,
        subject=subject,
        plain_text_content=content
    )

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        sg.send(message)
        print("📨 Email interne envoyé avec succès.")
    except Exception as e:
        print("❌ Erreur SendGrid :", e)



# ======================================================
# 📩 EMAIL 2 : RESET PASSWORD (NOUVEAU)
# ======================================================
def send_password_reset_email(to_email: str, reset_url: str):
    """
    Envoie un email de réinitialisation de mot de passe Kerelia.
    """

    if not SENDGRID_API_KEY:
        print("⚠️ Aucun SENDGRID_API_KEY défini — email non envoyé.")
        return

    if not FROM_EMAIL:
        print("⚠️ Aucun FROM_EMAIL défini — email non envoyé.")
        return

    html = f"""
    <!DOCTYPE html>
    <html lang="fr">
    <head>
      <meta charset="UTF-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      <title>Réinitialisation de votre mot de passe Kerelia</title>
    </head>
    <body style="margin:0; padding:0; background:#D5E1E3; font-family:Arial, sans-serif;">
      <table width="100%" cellpadding="0" cellspacing="0" style="padding:32px 0;">
        <tr>
          <td align="center">
            <table width="600" cellpadding="0" cellspacing="0"
                   style="background:#FFFFFF; border-radius:18px; overflow:hidden; padding:40px;">
              <tr>
                <td align="center" style="padding-bottom:20px; background:#0B131F;">
                  <img 
                    src="https://kerelia.fr/kerelia_logo_gris_fond_bleu_fonce.png"
                    width="140"
                    alt="Kerelia"
                    style="display:block; border:0; margin:auto;"
                  />
                </td>
              </tr>

              <tr>
                <td align="center" style="
                  font-size:24px;
                  font-weight:700;
                  color:#0B131F;
                  padding-bottom:10px;">
                  Réinitialiser votre mot de passe
                </td>
              </tr>

              <tr>
                <td style="font-size:15px; color:#1A2B42; line-height:1.6; padding-bottom:24px;">
                  Vous avez demandé à réinitialiser votre mot de passe Kerelia.
                  <br/>
                  Cliquez sur le bouton ci-dessous pour définir un nouveau mot de passe.
                </td>
              </tr>

              <tr>
                <td align="center" style="padding-bottom:32px;">
                  <a href="{reset_url}"
                     style="
                      background:#0B131F;
                      color:#D5E1E3;
                      padding:14px 22px;
                      font-size:16px;
                      border-radius:10px;
                      text-decoration:none;
                      display:inline-block;
                      font-weight:600;">
                    Réinitialiser mon mot de passe
                  </a>
                </td>
              </tr>

              <tr>
                <td style="font-size:13px; color:#444; padding-top:12px;">
                  Si le bouton ne fonctionne pas, copiez-collez ce lien dans votre navigateur :
                  <br>
                  <span style="word-break:break-all; color:#0B131F;">
                    {reset_url}
                  </span>
                </td>
              </tr>

              <tr>
                <td align="center" style="
                  padding-top:32px;
                  font-size:13px;
                  color:#666;
                  border-top:1px solid #E0E4E5;">
                  L’équipe Kerelia<br/>
                  <span style="font-size:12px;">Modernisation & automatisation des documents d’urbanisme</span>
                </td>
              </tr>

            </table>
          </td>
        </tr>
      </table>
    </body>
    </html>
    """

    message = Mail(
        from_email=Email(FROM_EMAIL, "Kerelia"),
        to_emails=to_email,
        subject="Réinitialisation de votre mot de passe Kerelia",
        html_content=html,
    )

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        sg.send(message)
        print(f"📨 Email reset envoyé → {to_email}")
    except Exception as e:
        print("❌ Erreur SendGrid (reset password) :", e)
