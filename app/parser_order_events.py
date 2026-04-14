import hashlib
import re
from email.message import Message
from email.utils import parsedate_to_datetime

from app.parser_sales import decode_mime_header, extract_text_body, normalize_title


MONTH_MAP_PT = {
    "jan": 1,
    "fev": 2,
    "mar": 3,
    "abr": 4,
    "mai": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "set": 9,
    "out": 10,
    "nov": 11,
    "dez": 12,
}


def _extract_subject_article_title(subject: str) -> str | None:
    if not subject:
        return None

    match = re.search(r"atualização do pedido para\s+(.+)$", subject, re.IGNORECASE)
    if match:
        return re.sub(r"\s+", " ", match.group(1)).strip()

    return None


def _extract_first_sku(text: str | None) -> str | None:
    if not text:
        return None

    match = re.search(r"\[(SKU\d+)\]", text, re.IGNORECASE)
    if not match:
        return None

    return match.group(1).upper()


def _extract_order_id(body: str) -> str | None:
    match = re.search(r"ID do pedido:\s*#?(\d+)", body, re.IGNORECASE)
    if not match:
        return None
    return match.group(1).strip()


def _extract_amount(body: str, label: str) -> float | None:
    pattern = rf"{re.escape(label)}:\s*€\s*([0-9]+[.,][0-9]{{2}})"
    match = re.search(pattern, body, re.IGNORECASE)
    if not match:
        return None
    return float(match.group(1).replace(",", "."))


def _extract_completed_article_title(body: str) -> str | None:
    match = re.search(
        r'A venda do artigo\s+"(.+?)"\s+foi concluída com sucesso',
        body,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None

    return re.sub(r"\s+", " ", match.group(1)).strip()


def _extract_completed_at_raw(body: str) -> str | None:
    match = re.search(r"Data:\s*([0-9]{2}/[0-9]{2}/[0-9]{4},\s*[0-9]{1,2}h[0-9]{2})", body, re.IGNORECASE)
    if not match:
        return None
    return match.group(1).strip()


def _extract_delivery_range_raw(body: str) -> tuple[str | None, str | None]:
    match = re.search(
        r"Entrega prevista a:\s*([A-Za-zÀ-ÿ]{3})\s*([0-9]{1,2})\s*-\s*([A-Za-zÀ-ÿ]{3})\s*([0-9]{1,2})",
        body,
        re.IGNORECASE,
    )
    if not match:
        return None, None

    start_raw = f"{match.group(1)} {match.group(2)}"
    end_raw = f"{match.group(3)} {match.group(4)}"
    return start_raw, end_raw


def _extract_bundle_item_count(body: str) -> int | None:
    match = re.search(r"conjunto de\s+(\d+)\s+artigos?", body, re.IGNORECASE)
    if not match:
        return None

    try:
        return int(match.group(1))
    except Exception:
        return None


def _is_shipped_email(subject: str, body: str) -> bool:
    return (
        "atualização do pedido para" in (subject or "").lower()
        and "está a caminho do comprador" in (body or "").lower()
    )


def _is_completed_email(subject: str, body: str) -> bool:
    subject_l = (subject or "").lower()
    body_l = (body or "").lower()

    return (
        "este pedido está concluído" in subject_l
        or (
            "a tua venda foi concretizada" in body_l
            and "foi concluída com sucesso" in body_l
        )
    )


def parse_order_event_email(msg: Message, gmail_message_id: str) -> dict | None:
    body = extract_text_body(msg)
    subject = decode_mime_header(msg.get("Subject", "")) or ""
    date_hdr = msg.get("Date")
    received_at = parsedate_to_datetime(date_hdr).isoformat() if date_hdr else None

    if _is_shipped_email(subject, body):
        article_title = _extract_subject_article_title(subject)
        bundle_item_count = _extract_bundle_item_count(body)
        is_bundle = bundle_item_count is not None and bundle_item_count > 1
        delivery_start_raw, delivery_end_raw = _extract_delivery_range_raw(body)

        event_id = hashlib.sha1(f"ORDER_SHIPPED:{gmail_message_id}".encode()).hexdigest()

        return {
            "eventEmailId": gmail_message_id,
            "eventId": event_id,
            "eventType": "ORDER_SHIPPED",
            "subject": subject,
            "receivedAt": received_at,
            "orderId": _extract_order_id(body),
            "articleTitle": article_title,
            "articleTitleNormalized": normalize_title(article_title),
            "sku": _extract_first_sku(article_title),
            "isBundle": is_bundle,
            "bundleItemCount": bundle_item_count,
            "estimatedDeliveryStartRaw": delivery_start_raw,
            "estimatedDeliveryEndRaw": delivery_end_raw,
            "rawBodyPreview": body[:4000],
        }

    if _is_completed_email(subject, body):
        article_title = _extract_completed_article_title(body)

        event_id = hashlib.sha1(f"ORDER_COMPLETED:{gmail_message_id}".encode()).hexdigest()

        return {
            "eventEmailId": gmail_message_id,
            "eventId": event_id,
            "eventType": "ORDER_COMPLETED",
            "subject": subject,
            "receivedAt": received_at,
            "orderId": _extract_order_id(body),
            "completedAtRaw": _extract_completed_at_raw(body),
            "articleTitle": article_title,
            "articleTitleNormalized": normalize_title(article_title),
            "sku": _extract_first_sku(article_title),
            "itemAmount": _extract_amount(body, "Recebido pelo artigo"),
            "shippingAmount": _extract_amount(body, "Recebido pelo envio"),
            "transferredToVintedBalance": _extract_amount(body, "Transferido para o teu Saldo Vinted"),
            "currency": "EUR",
            "rawBodyPreview": body[:4000],
        }

    return None