import hashlib
import re
from email.message import Message
from email.utils import parsedate_to_datetime

from app.parser_sales import decode_mime_header, extract_text_body


def _extract_amount(body: str, label: str) -> float | None:
    pattern = rf"{re.escape(label)}:\s*€\s*([0-9]+[.,][0-9]{{2}})"
    match = re.search(pattern, body, re.IGNORECASE)
    if not match:
        return None
    return float(match.group(1).replace(",", "."))


def _extract_invoice_number(body: str) -> str | None:
    patterns = [
        r"Número da fatura:\s*([A-Z0-9._/-]+)",
        r"Fatura n[.ºo°]*\s*:?\s*([A-Z0-9._/-]+)",
        r"Invoice number:\s*([A-Z0-9._/-]+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, body, re.IGNORECASE)
        if match:
            return match.group(1).strip()

    return None


def _extract_period(body: str) -> str | None:
    patterns = [
        r"Período:\s*(.+)",
        r"Periodo:\s*(.+)",
        r"Billing period:\s*(.+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, body, re.IGNORECASE)
        if match:
            return re.sub(r"\s+", " ", match.group(1)).strip()

    return None


def _extract_total_amount(body: str) -> float | None:
    labels = [
        "Total",
        "Montante total",
        "Valor total",
        "Total da fatura",
    ]

    for label in labels:
        value = _extract_amount(body, label)
        if value is not None:
            return value

    match = re.search(r"€\s*([0-9]+[.,][0-9]{2})", body, re.IGNORECASE)
    if match:
        return float(match.group(1).replace(",", "."))

    return None


def _extract_vat_amount(body: str) -> float | None:
    labels = [
        "IVA",
        "VAT",
        "Imposto",
    ]

    for label in labels:
        value = _extract_amount(body, label)
        if value is not None:
            return value

    return None


def _extract_currency(body: str) -> str:
    if "€" in body:
        return "EUR"
    return "EUR"


def _is_expense_email(subject: str, body: str) -> bool:
    subject_l = (subject or "").lower()
    body_l = (body or "").lower()

    return (
        "destaques de artigos - a tua fatura" in subject_l
        or "a tua fatura" in subject_l
        or "invoice" in subject_l
        or "fatura" in body_l
    )


def parse_expense_email(msg: Message, gmail_message_id: str) -> dict | None:
    body = extract_text_body(msg)
    subject = decode_mime_header(msg.get("Subject", "")) or ""
    date_hdr = msg.get("Date")
    received_at = parsedate_to_datetime(date_hdr).isoformat() if date_hdr else None

    if not _is_expense_email(subject, body):
        return None

    expense_id = hashlib.sha1(f"EXPENSE:{gmail_message_id}".encode()).hexdigest()

    total_amount = _extract_total_amount(body)

    return {
        "expenseId": expense_id,
        "expenseEmailId": gmail_message_id,
        "subject": subject,
        "invoiceNumber": _extract_invoice_number(body),
        "billingPeriodRaw": _extract_period(body),
        "totalAmount": total_amount,
        "vatAmount": _extract_vat_amount(body),
        "currency": _extract_currency(body),
        "status": "RECEIVED",
        "receivedAt": received_at,
        "rawBodyPreview": body[:4000],
    }