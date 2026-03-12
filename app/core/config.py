import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


@dataclass
class Settings:
    app_name: str = os.getenv("APP_NAME", "Timeclock MVP")
    database_url: str = os.getenv("DATABASE_URL", "sqlite:///./timeclock.db")
    admin_pin: str = os.getenv("ADMIN_PIN", "1234")
    kiosk_device_id: str = os.getenv("KIOSK_DEVICE_ID", "ipad-kiosk-1")

    smtp_host: str = os.getenv("SMTP_HOST", "")
    smtp_port: int = int(os.getenv("SMTP_PORT", "587"))
    smtp_user: str = os.getenv("SMTP_USER", "")
    smtp_password: str = os.getenv("SMTP_PASSWORD", "")
    smtp_use_tls: bool = os.getenv("SMTP_USE_TLS", "true").lower() == "true"
    mail_from: str = os.getenv("MAIL_FROM", "")
    mail_bcc: str = os.getenv("MAIL_BCC", "")


settings = Settings()
