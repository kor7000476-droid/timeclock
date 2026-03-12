from __future__ import annotations

import html
import smtplib
from email.message import EmailMessage
from typing import Sequence, Tuple

from app.core.config import settings


class MailerError(RuntimeError):
    pass


_OUTGOING_NOTICE_KO = "본 메일은 발송전용입니다. 회신하지 마십시오."
_OUTGOING_NOTICE_EN = "This is an outgoing-only email. Please do not reply."


def _validate_smtp() -> None:
    if not (settings.smtp_host and settings.smtp_user and settings.smtp_password and settings.mail_from):
        raise MailerError("SMTP settings are not configured")
    if settings.smtp_host == "smtp.example.com" or settings.smtp_password == "change_me":
        raise MailerError("SMTP settings are placeholder values. Update .env before sending email.")


def _build_message(to_email: str, subject: str, body: str) -> tuple[EmailMessage, list[str]]:
    base_text = (body or "").rstrip()
    footer_text = f"\n\n---\n{_OUTGOING_NOTICE_KO}\n{_OUTGOING_NOTICE_EN}\n"
    full_text = f"{base_text}{footer_text}" if base_text else f"{_OUTGOING_NOTICE_KO}\n{_OUTGOING_NOTICE_EN}\n"

    base_html = html.escape(body or "").replace("\n", "<br/>").strip("<br/>")
    footer_html = (
        f"<p style=\"margin:16px 0 0; color:#dc2626; font-weight:700;\">"
        f"{html.escape(_OUTGOING_NOTICE_KO)}<br/>{html.escape(_OUTGOING_NOTICE_EN)}"
        f"</p>"
    )
    content_html = (
        "<html><body>"
        "<div style=\"font-family:'Segoe UI',Arial,sans-serif; font-size:14px; color:#111827;\">"
        f"{base_html}{footer_html}"
        "</div>"
        "</body></html>"
    )

    msg = EmailMessage()
    msg["From"] = settings.mail_from
    msg["To"] = to_email
    bcc_list = []
    if settings.mail_bcc.strip():
        bcc_list = [v.strip() for v in settings.mail_bcc.split(",") if v.strip()]
    if bcc_list:
        msg["Bcc"] = ", ".join(bcc_list)
    msg["Subject"] = subject
    msg.set_content(full_text)
    msg.add_alternative(content_html, subtype="html")

    recipients = [to_email] + bcc_list
    return msg, recipients


def send_email_with_attachments(
    to_email: str,
    subject: str,
    body: str,
    attachments: Sequence[Tuple[str, bytes, str, str]],
) -> None:
    _validate_smtp()
    msg, recipients = _build_message(to_email=to_email, subject=subject, body=body)
    for filename, content, maintype, subtype in attachments:
        msg.add_attachment(content, maintype=maintype, subtype=subtype, filename=filename)

    with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=20) as smtp:
        smtp.ehlo()
        if settings.smtp_use_tls:
            smtp.starttls()
            smtp.ehlo()
        smtp.login(settings.smtp_user, settings.smtp_password)
        rejected = smtp.sendmail(settings.mail_from, recipients, msg.as_string())
        if rejected:
            raise MailerError(f"SMTP rejected recipients: {rejected}")


def send_csv(to_email: str, subject: str, body: str, filename: str, content: str) -> None:
    send_email_with_attachments(
        to_email=to_email,
        subject=subject,
        body=body,
        attachments=[(filename, content.encode("utf-8"), "text", "csv")],
    )
