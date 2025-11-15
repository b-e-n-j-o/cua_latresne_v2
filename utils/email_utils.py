# email_utils.py
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
INTERNAL_EMAIL = os.getenv("INTERNAL_NOTIFICATION_EMAIL")  # ex: contact@kerelia.fr


def send_internal_email(lead: dict):
    """
    Envoie un email interne pour notifier un lead.
    Fonction appelée depuis les endpoints.
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
