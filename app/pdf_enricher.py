from io import BytesIO
from pathlib import Path
import textwrap

from pypdf import PdfReader, PdfWriter
from reportlab.pdfgen import canvas


HEADER_MARGIN = 10
LINE_HEIGHT = 14
FONT_SIZE = 10
MAX_TEXT_WIDTH_CHARS = 78


def _wrap_text(text: str, width: int = MAX_TEXT_WIDTH_CHARS) -> list[str]:
    if not text:
        return ["-"]

    wrapped = []
    for raw_line in text.splitlines():
        raw_line = raw_line.strip()
        if not raw_line:
            continue

        parts = textwrap.wrap(
            raw_line,
            width=width,
            break_long_words=False,
            break_on_hyphens=False,
        )
        if parts:
            wrapped.extend(parts)
        else:
            wrapped.append(raw_line)

    return wrapped or ["-"]


def _build_overlay_lines(
    order_id: str | None,
    sku: str | None,
    buyer_username: str | None,
    value: float | None,
    article_title: str | None,
) -> list[str]:
    lines = [
        f"PEDIDO: {order_id or '-'}",
        f"SKU: {sku or '-'}",
        f"CLIENTE: {buyer_username or '-'}",
        f"VALOR: {f'{value:.2f} EUR' if value is not None else '-'}",
    ]

    article_title = (article_title or "-").strip()

    # Bundle / múltiplos artigos: cada artigo numa linha
    if "\n" in article_title:
        lines.append("ARTIGOS:")
        article_lines = [line.strip() for line in article_title.splitlines() if line.strip()]

        for article in article_lines:
            wrapped = _wrap_text(article, width=72)
            if wrapped:
                lines.append(f"- {wrapped[0]}")
                for continuation in wrapped[1:]:
                    lines.append(f"  {continuation}")
    else:
        wrapped = _wrap_text(article_title, width=72)
        if wrapped:
            lines.append(f"ARTIGO: {wrapped[0]}")
            for continuation in wrapped[1:]:
                lines.append(f"        {continuation}")
        else:
            lines.append("ARTIGO: -")

    return lines


def _calculate_header_height(lines: list[str]) -> float:
    # margem superior + linhas + margem inferior
    return max(95, 24 + (len(lines) * LINE_HEIGHT) + 16)


def _build_header_overlay(lines: list[str], page_width: float, page_height: float, header_height: float) -> PdfReader:
    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=(page_width, page_height))

    box_x = HEADER_MARGIN
    box_y = page_height - header_height
    box_width = page_width - (HEADER_MARGIN * 2)
    box_height = header_height - HEADER_MARGIN

    c.setFillColorRGB(1, 1, 1)
    c.rect(box_x, box_y, box_width, box_height, fill=1, stroke=1)

    c.setFillColorRGB(0, 0, 0)
    c.setFont("Helvetica-Bold", FONT_SIZE)

    text_y = box_y + box_height - 18
    for line in lines:
        c.drawString(box_x + 10, text_y, line)
        text_y -= LINE_HEIGHT

    c.save()
    buffer.seek(0)
    return PdfReader(buffer)


def enrich_label_pdf(
    input_pdf_path: str | Path,
    output_pdf_path: str | Path,
    order_id: str | None,
    sku: str | None,
    buyer_username: str | None,
    value: float | None,
    article_title: str | None,
):
    input_pdf_path = Path(input_pdf_path)
    output_pdf_path = Path(output_pdf_path)
    output_pdf_path.parent.mkdir(parents=True, exist_ok=True)

    reader = PdfReader(str(input_pdf_path))
    writer = PdfWriter()

    if not reader.pages:
        raise ValueError(f"PDF sem páginas: {input_pdf_path}")

    original_first_page = reader.pages[0]
    original_width = float(original_first_page.mediabox.width)
    original_height = float(original_first_page.mediabox.height)

    overlay_lines = _build_overlay_lines(
        order_id=order_id,
        sku=sku,
        buyer_username=buyer_username,
        value=value,
        article_title=article_title,
    )

    header_height = _calculate_header_height(overlay_lines)
    total_height = original_height + header_height

    # nova primeira página mais alta
    new_first_page = writer.add_blank_page(width=original_width, height=total_height)

    try:
        new_first_page.mergeTranslatedPage(original_first_page, 0, 0, expand=False)
    except AttributeError:
        from pypdf import Transformation
        new_first_page.merge_transformed_page(
            original_first_page,
            Transformation().translate(tx=0, ty=0)
        )

    header_overlay = _build_header_overlay(
        lines=overlay_lines,
        page_width=original_width,
        page_height=total_height,
        header_height=header_height,
    )
    new_first_page.merge_page(header_overlay.pages[0])

    for page in reader.pages[1:]:
        writer.add_page(page)

    with open(output_pdf_path, "wb") as f:
        writer.write(f)