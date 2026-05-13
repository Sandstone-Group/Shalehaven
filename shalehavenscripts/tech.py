## Shalehaven tech utilities — email sending, etc.
## Designed to be imported by main_* pipelines (e.g. main_model.py) to ship outputs
## (PDFs, Excels, HTML) to stakeholders without leaving the script.

import os
import ssl
import smtplib
import mimetypes
from email.message import EmailMessage
from pathlib import Path


## Send an email from an O365 mailbox via SMTP (smtp.office365.com:587, STARTTLS).
## Built for a service mailbox WITHOUT 2FA — uses basic email + password auth. If 2FA
## is ever enabled on the sender account, this stops working and we'd switch to Graph + MSAL.
##
## Credentials default to environment variables so callers don't pass them every call:
##   SHALEHAVEN_SMTP_USER      development@shalehaven.com (Shalehaven service mailbox, no 2FA)
##   SHALEHAVEN_SMTP_PASSWORD  the mailbox password
## Both can be overridden via kwargs for one-off sends.
##
## toAddresses / cc / bcc accept a single string or a list of strings.
## attachments: list of file paths (PDF, Excel, anything). Missing paths warn and skip
## rather than failing the whole send.
## bodyIsHtml=True sends an HTML body with a plain-text fallback.
def sendEmail(toAddresses, subject, body, attachments=None,
              fromAddress=None, smtpUser=None, smtpPassword=None,
              smtpHost="smtp.office365.com", smtpPort=587,
              cc=None, bcc=None, bodyIsHtml=False):
    smtpUser = smtpUser or os.environ.get("SHALEHAVEN_SMTP_USER")
    smtpPassword = smtpPassword or os.environ.get("SHALEHAVEN_SMTP_PASSWORD")
    if not smtpUser or not smtpPassword:
        raise RuntimeError(
            "SMTP credentials missing — set SHALEHAVEN_SMTP_USER and SHALEHAVEN_SMTP_PASSWORD "
            "in the environment, or pass smtpUser=/smtpPassword= explicitly."
        )

    fromAddress = fromAddress or smtpUser
    if isinstance(toAddresses, str):
        toAddresses = [toAddresses]
    if isinstance(cc, str):
        cc = [cc]
    if isinstance(bcc, str):
        bcc = [bcc]
    if not toAddresses:
        raise ValueError("sendEmail: toAddresses is empty")

    msg = EmailMessage()
    msg["From"] = fromAddress
    msg["To"] = ", ".join(toAddresses)
    if cc:
        msg["Cc"] = ", ".join(cc)
    msg["Subject"] = subject
    if bodyIsHtml:
        msg.set_content("This message contains HTML. View it in an HTML-capable mail client.")
        msg.add_alternative(body, subtype="html")
    else:
        msg.set_content(body)

    attached_count = 0
    for path in (attachments or []):
        p = Path(path)
        if not p.exists():
            print(f"  WARNING: attachment not found, skipping: {path}")
            continue
        ctype, encoding = mimetypes.guess_type(str(p))
        if ctype is None or encoding is not None:
            ctype = "application/octet-stream"
        maintype, subtype = ctype.split("/", 1)
        with open(p, "rb") as fh:
            msg.add_attachment(fh.read(), maintype=maintype, subtype=subtype, filename=p.name)
        attached_count += 1

    all_recipients = list(toAddresses) + list(cc or []) + list(bcc or [])
    context = ssl.create_default_context()
    with smtplib.SMTP(smtpHost, smtpPort) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(smtpUser, smtpPassword)
        server.send_message(msg, from_addr=fromAddress, to_addrs=all_recipients)

    print(f"Sent email to {', '.join(all_recipients)} "
          f"(subject: {subject!r}, {attached_count} attachment(s))")
