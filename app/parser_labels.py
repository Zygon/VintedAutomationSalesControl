import hashlib
import re
from email.message import Message
from email.utils import parsedate_to_datetime

from app.parser_sales import decode_mime_header, extract_text_body, normalize_title


def extract_field(body: str, field_name: str) -> str | None:
    patterns = [
        rf"{re.escape(field_name)}:\s*(.+)",
        rf"{re.escape(field_name)}\s*\n\s*(.+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, body, re.IGNORECASE)
        if match:
            value = match.group(1).strip()
            value = re.sub(r"\s+", " ", value)
            return value

    return None


def extract_skus(body: str) -> list[str]:
    matches = re.findall(r"\[(SKU\d+)\]", body, re.IGNORECASE)
    normalized = []
    for m in matches:
        sku = m.upper().strip()
        if sku not in normalized:
            normalized.append(sku)
    return normalized


def extract_bundle_articles(body: str) -> list[str]:
    """
    Extrai linhas de artigos a partir da secção 'Artigo:'.
    Funciona para:
    - 1 artigo
    - bundles com várias linhas
    """
    lines = [line.strip() for line in body.splitlines()]

    start_index = None
    for i, line in enumerate(lines):
        if line.lower().startswith("artigo:"):
            start_index = i
            break

    if start_index is None:
        return []

    collected = []

    first_line = lines[start_index]
    after_colon = first_line.split(":", 1)[1].strip() if ":" in first_line else ""
    if after_colon:
        collected.append(after_colon)

    stop_prefixes = (
        "tamanho da embalagem:",
        "código de envio:",
        "prazo de envio:",
        "id do pedido:",
        "aqui estão as próximas etapas",
    )

    for line in lines[start_index + 1:]:
        clean = line.strip()
        if not clean:
            continue

        lower = clean.lower()
        if lower.startswith(stop_prefixes):
            break

        if len(clean) < 3:
            continue

        collected.append(clean)

    normalized = []
    for item in collected:
        item = re.sub(r"\s+", " ", item).strip()
        if item and item not in normalized:
            normalized.append(item)

    return normalized


def parse_label_email(msg: Message, gmail_message_id: str) -> dict:
    body = extract_text_body(msg)
    subject = decode_mime_header(msg.get("Subject", ""))
    date_hdr = msg.get("Date")
    received_at = parsedate_to_datetime(date_hdr).isoformat() if date_hdr else None

    article = extract_field(body, "Artigo")
    shipping_code = extract_field(body, "Código de envio")
    shipping_deadline = extract_field(body, "Prazo de envio")
    order_id = extract_field(body, "ID do pedido")

    bundle_articles = extract_bundle_articles(body)
    skus = extract_skus(body)

    is_bundle = len(bundle_articles) > 1

    if not article and len(bundle_articles) == 1:
        article = bundle_articles[0]

    label_id = hashlib.sha1(gmail_message_id.encode()).hexdigest()

    return {
        "labelId": label_id,
        "labelEmailId": gmail_message_id,
        "subject": subject,
        "articleTitle": article,
        "articleTitleNormalized": normalize_title(article),
        "bundleArticles": bundle_articles,
        "shippingCode": shipping_code,
        "shippingDeadlineRaw": shipping_deadline,
        "orderId": order_id,
        "skus": skus,
        "isBundle": is_bundle,
        "status": "LABEL_RECEIVED",
        "receivedAt": received_at,
        "rawBodyPreview": body[:4000],
    }