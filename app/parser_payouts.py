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


def _extract_any_money(body: str) -> float | None:
    match = re.search(r"€\s*([0-9]+[.,][0-9]{2})", body, re.IGNORECASE)
    if not match:
        return None
    return float(match.group(1).replace(",", "."))


def _extract_transfer_reference(body: str) -> str | None:
    patterns = [
        r"Referência:\s*([A-Z0-9._/-]+)",
        r"Reference:\s*([A-Z0-9._/-]+)",
        r"ID da transferência:\s*([A-Z0-9._/-]+)",
        r"Transfer ID:\s*([A-Z0-9._/-]+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, body, re.IGNORECASE)
        if match:
            return match.group(1).strip()

    return None


def _extract_payout_method(body: str) -> str | None:
    patterns = [
        r"Método de pagamento:\s*(.+)",
        r"Payment method:\s*(.+)",
        r"Enviado para:\s*(.+)",
        r"Sent to:\s*(.+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, body, re.IGNORECASE)
        if match:
            return re.sub(r"\s+", " ", match.group(1)).strip()

    return "BANK_TRANSFER"


def _extract_arrival_window(body: str) -> str | None:
    patterns = [
        r"deverá chegar.*?em\s*(.+)",
        r"should arrive.*?in\s*(.+)",
        r"prazo estimado:\s*(.+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, body, re.IGNORECASE)
        if match:
            return re.sub(r"\s+", " ", match.group(1)).strip()

    return None


def _is_payout_email(subject: str, body: str) -> bool:
    subject_l = (subject or "").lower()
    body_l = (body or "").lower()

    return (
        "o teu pagamento está a ser enviado para o banco" in subject_l
        or "pagamento está a ser enviado para o banco" in subject_l
        or "payment is being sent to your bank" in subject_l
        or ("enviado para o banco" in body_l and "saldo vinted" in body_l)
    )


def parse_payout_email(msg: Message, gmail_message_id: str) -> dict | None:
    body = extract_text_body(msg)
    subject = decode_mime_header(msg.get("Subject", "")) or ""
    date_hdr = msg.get("Date")
    received_at = parsedate_to_datetime(date_hdr).isoformat() if date_hdr else None

    if not _is_payout_email(subject, body):
        return None

    payout_id = hashlib.sha1(f"PAYOUT:{gmail_message_id}".encode()).hexdigest()

    amount = (
        _extract_amount(body, "Montante")
        or _extract_amount(body, "Valor")
        or _extract_amount(body, "Amount")
        or _extract_any_money(body)
    )

    return {
        "payoutId": payout_id,
        "payoutEmailId": gmail_message_id,
        "subject": subject,
        "amount": amount,
        "currency": "EUR",
        "transferReference": _extract_transfer_reference(body),
        "method": _extract_payout_method(body),
        "estimatedArrivalRaw": _extract_arrival_window(body),
        "status": "SENT",
        "receivedAt": received_at,
        "rawBodyPreview": body[:4000],
    }