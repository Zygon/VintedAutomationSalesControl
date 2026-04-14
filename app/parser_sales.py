import hashlib
import re
from email.header import decode_header, make_header
from email.message import Message
from email.utils import parsedate_to_datetime
from bs4 import BeautifulSoup


def decode_mime_header(value: str | None) -> str | None:
    if not value:
        return value
    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return value


def clean_text(text: str) -> str:
    text = text.replace("\xa0", " ")
    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{2,}", "\n\n", text)
    return text.strip()


def html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    for br in soup.find_all("br"):
        br.replace_with("\n")

    for tag in soup.find_all(["p", "div", "li", "tr", "table"]):
        tag.append("\n")

    text = soup.get_text(separator=" ")
    return clean_text(text)


def extract_text_body(msg: Message) -> str:
    plain_parts = []
    html_parts = []

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition = str(part.get("Content-Disposition", ""))

            if "attachment" in disposition.lower():
                continue

            payload = part.get_payload(decode=True)
            if not payload:
                continue

            charset = part.get_content_charset() or "utf-8"
            try:
                decoded = payload.decode(charset, errors="replace")
            except Exception:
                decoded = payload.decode("utf-8", errors="replace")

            if content_type == "text/plain":
                plain_parts.append(decoded)
            elif content_type == "text/html":
                html_parts.append(decoded)
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            try:
                decoded = payload.decode(charset, errors="replace")
            except Exception:
                decoded = payload.decode("utf-8", errors="replace")

            if msg.get_content_type() == "text/html":
                html_parts.append(decoded)
            else:
                plain_parts.append(decoded)

    if plain_parts:
        return clean_text("\n".join(plain_parts))

    if html_parts:
        return html_to_text("\n".join(html_parts))

    return ""


def normalize_title(title: str | None) -> str | None:
    if not title:
        return None
    title = title.lower().strip()
    title = re.sub(r"[–—-]", " ", title)
    title = re.sub(r"[^\w\s]", " ", title)
    title = re.sub(r"\s+", " ", title)
    return title


def _extract_buyer(lines: list[str], body: str) -> str | None:
    for line in lines:
        match = re.match(r"^\s*(.+?)\s+comprou\s*$", line, re.IGNORECASE)
        if match:
            buyer = match.group(1).strip()
            if buyer and not buyer.lower().startswith("olá"):
                return buyer

    for i, line in enumerate(lines):
        if line.strip().lower() == "comprou" and i > 0:
            buyer = lines[i - 1].strip()
            if buyer and not buyer.lower().startswith("olá"):
                return buyer

    match = re.search(r"\n\s*([^\n]+?)\s+comprou\s*(?:\n|$)", body, re.IGNORECASE)
    if match:
        buyer = match.group(1).strip()
        if buyer and not buyer.lower().startswith("olá"):
            return buyer

    return None


def _extract_article_title(body: str) -> str | None:
    value_match = re.search(r"€\s*([0-9]+[.,][0-9]{2})", body)
    if not value_match:
        return None

    before_value = body[:value_match.start()]
    before_lines = [line.strip() for line in before_value.splitlines() if line.strip()]

    ignore_patterns = [
        r"^olá[,!]",
        r"^comprou$",
        r"^iremos transferir",
        r"^envia este pedido",
        r"^saldo vinted",
    ]

    candidates = []
    for line in before_lines:
        line_l = line.lower()
        if any(re.search(pat, line_l) for pat in ignore_patterns):
            continue
        if re.search(r".+\s+comprou\s*$", line_l):
            continue
        if len(line) < 4:
            continue
        candidates.append(line)

    if not candidates:
        return None

    return candidates[-1]


def _extract_value(body: str) -> float | None:
    value_match = re.search(r"€\s*([0-9]+[.,][0-9]{2})", body)
    if not value_match:
        return None
    return float(value_match.group(1).replace(",", "."))


def _extract_sku(text: str | None) -> str | None:
    if not text:
        return None
    match = re.search(r"\[(SKU\d+)\]", text, re.IGNORECASE)
    if not match:
        return None
    return match.group(1).upper()


def _extract_bundle_item_count(article_title: str | None, body: str) -> int | None:
    sources = [
        article_title or "",
        body or "",
    ]

    patterns = [
        r"agrupar\s+(\d+)\s+artigos?",
        r"conjunto de\s+(\d+)\s+artigos?",
    ]

    for source in sources:
        for pattern in patterns:
            match = re.search(pattern, source, re.IGNORECASE)
            if match:
                try:
                    return int(match.group(1))
                except Exception:
                    return None

    return None


def parse_sale_email(msg: Message, gmail_message_id: str) -> dict:
    body = extract_text_body(msg)
    subject = decode_mime_header(msg.get("Subject", ""))
    date_hdr = msg.get("Date")
    sold_at = parsedate_to_datetime(date_hdr).isoformat() if date_hdr else None

    lines = [line.strip() for line in body.splitlines() if line.strip()]

    buyer = _extract_buyer(lines, body)
    value = _extract_value(body)
    article_title = _extract_article_title(body)
    sku = _extract_sku(article_title)
    bundle_item_count = _extract_bundle_item_count(article_title, body)
    is_bundle_sale = bundle_item_count is not None and bundle_item_count > 1

    sale_id = hashlib.sha1(gmail_message_id.encode()).hexdigest()

    return {
        "saleId": sale_id,
        "saleEmailId": gmail_message_id,
        "subject": subject,
        "buyerUsername": buyer,
        "articleTitle": article_title,
        "articleTitleNormalized": normalize_title(article_title),
        "sku": None if is_bundle_sale else sku,
        "value": value,
        "currency": "EUR",
        "soldAt": sold_at,
        "status": "WAITING_LABEL",
        "isBundleSale": is_bundle_sale,
        "bundleItemCount": bundle_item_count,
        "rawBodyPreview": body[:4000],
    }