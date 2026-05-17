## Shalehaven Tech Scripts, Including emails, SQL queries, and API calls

"""

    Outbound email via Microsoft Graph (app-only / client credentials).

    The Shalehaven dev mailbox (SHALEHAVEN_DEV_EMAIL) has MFA enabled, so basic
    SMTP AUTH no longer works. IT registered an Entra ID application with the
    Mail.Send application permission (admin-consented) for that mailbox.

    Required env vars (in .env at repo root):
        SHALEHAVEN_DEV_EMAIL       sender mailbox (e.g. development@shalehaven.com)
        SHALEHAVEN_DEV_TENANT_ID   Entra tenant ID
        SHALEHAVEN_DEV_APP_ID      Entra app (client) ID
        SHALEHAVEN_DEV_VALUE       client secret VALUE (the actual secret string)
        SHALEHAVEN_DEV_SECRET      client secret ID (UUID shown next to the value)
        SHALEHAVEN_DEV_PASSWORD    mailbox password (not used by app-only flow,
                                   loaded for reference / future ROPC fallback)

    Usage from another script:
        from shalehavenscripts.tech import sendEmail
        sendEmail(
            to="someone@example.com",
            subject="Monthly LOS",
            body="See attached.",
            attachments=[r"D:\\out\\los_2026_04.xlsx", r"D:\\out\\summary.pdf"],
        )

"""

import base64
import mimetypes
import os
import time
from pathlib import Path

import requests
from dotenv import load_dotenv


GRAPH_BASE = "https://graph.microsoft.com/v1.0"
LOGIN_BASE = "https://login.microsoftonline.com"
GRAPH_SCOPE = "https://graph.microsoft.com/.default"

# Graph sendMail JSON payload (including base64 attachments) caps at ~4MB total.
SEND_MAIL_PAYLOAD_LIMIT_BYTES = 4 * 1024 * 1024


class GraphMailer:
    """Send mail from the Shalehaven dev mailbox via Microsoft Graph."""

    def __init__(self, sender, tenant_id, client_id, client_secret):
        self.sender = sender
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self._token = None
        self._token_expires_at = 0.0

    @classmethod
    def from_env(cls):
        load_dotenv(dotenv_path=_find_env_file())
        sender = _require_env("SHALEHAVEN_DEV_EMAIL")
        tenant_id = _require_env("SHALEHAVEN_DEV_TENANT_ID")
        client_id = _require_env("SHALEHAVEN_DEV_APP_ID")
        # Azure portal exposes a secret "Value" (the actual secret string) and
        # a secret "ID" (a UUID). The VALUE is what we send to the token endpoint.
        client_secret = _require_env("SHALEHAVEN_DEV_VALUE")
        return cls(sender, tenant_id, client_id, client_secret)

    def _get_token(self):
        if self._token and time.time() < self._token_expires_at - 60:
            return self._token
        url = f"{LOGIN_BASE}/{self.tenant_id}/oauth2/v2.0/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": GRAPH_SCOPE,
        }
        resp = requests.post(url, data=data, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Graph token request failed ({resp.status_code}): {resp.text}"
            )
        payload = resp.json()
        self._token = payload["access_token"]
        self._token_expires_at = time.time() + int(payload.get("expires_in", 3600))
        return self._token

    def send(self, to, subject, body, attachments=None, cc=None, bcc=None, html=False):
        """
        Send a message from self.sender.

        to / cc / bcc : str or list[str]
        attachments    : list of file paths (PDF, xlsx, xls, csv, ... any file)
        html           : if True, body is treated as HTML
        """
        message = {
            "subject": subject,
            "body": {
                "contentType": "HTML" if html else "Text",
                "content": body,
            },
            "toRecipients": _to_recipients(to),
        }
        if cc:
            message["ccRecipients"] = _to_recipients(cc)
        if bcc:
            message["bccRecipients"] = _to_recipients(bcc)
        if attachments:
            message["attachments"] = [_build_attachment(p) for p in attachments]
            total = sum(len(a["contentBytes"]) for a in message["attachments"])
            if total > SEND_MAIL_PAYLOAD_LIMIT_BYTES:
                raise ValueError(
                    f"Attachment payload ({total} bytes base64) exceeds Graph "
                    f"sendMail inline limit (~4MB). Use an upload session or "
                    f"send a shorter list."
                )

        token = self._get_token()
        url = f"{GRAPH_BASE}/users/{self.sender}/sendMail"
        resp = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={"message": message, "saveToSentItems": True},
            timeout=60,
        )
        if resp.status_code not in (202, 200):
            raise RuntimeError(
                f"Graph sendMail failed ({resp.status_code}): {resp.text}"
            )
        return True


def sendEmail(to, subject, body, attachments=None, cc=None, bcc=None, html=False):
    """Module-level convenience wrapper — builds a GraphMailer from env on each call."""
    mailer = GraphMailer.from_env()
    return mailer.send(
        to=to,
        subject=subject,
        body=body,
        attachments=attachments,
        cc=cc,
        bcc=bcc,
        html=html,
    )


def _to_recipients(addrs):
    if isinstance(addrs, str):
        addrs = [addrs]
    return [{"emailAddress": {"address": a}} for a in addrs]


def _build_attachment(path):
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"Attachment not found: {path}")
    content_type, _ = mimetypes.guess_type(str(p))
    if content_type is None:
        suffix = p.suffix.lower()
        content_type = {
            ".pdf": "application/pdf",
            ".csv": "text/csv",
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".xls": "application/vnd.ms-excel",
        }.get(suffix, "application/octet-stream")
    with p.open("rb") as fh:
        b64 = base64.b64encode(fh.read()).decode("ascii")
    return {
        "@odata.type": "#microsoft.graph.fileAttachment",
        "name": p.name,
        "contentType": content_type,
        "contentBytes": b64,
    }


def _require_env(name):
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val.strip()


def _find_env_file():
    """Walk up from this file looking for a .env (repo root or code/ parent)."""
    here = Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        candidate = parent / ".env"
        if candidate.is_file():
            return candidate
    return None


if __name__ == "__main__":
    sendEmail(
        to="mtanner@shalehaven.com",
        subject="Graph mailer smoke test",
        body="Test message from shalehavenscripts/tech.py via Microsoft Graph.",
    )
    print("sent")
