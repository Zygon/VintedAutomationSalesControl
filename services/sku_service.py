from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from services.firestore_queries import reserve_next_sku, upsert_product_row

DEFAULT_PREFIX = "SKU"
DEFAULT_PADDING = 3


class SKUGenerationError(RuntimeError):
    pass


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_product_id(account_id: str, normalized_name: str) -> str:
    return f"{account_id}__{normalized_name}"


def generate_next_sku(
    account_id: str,
    prefix: str = DEFAULT_PREFIX,
    padding: int = DEFAULT_PADDING,
) -> dict[str, Any]:
    if not account_id:
        raise SKUGenerationError("Seleciona uma conta específica antes de gerar SKU.")

    return reserve_next_sku(
        account_id=account_id,
        prefix=prefix,
        padding=padding,
        generated_by="streamlit-local-ui",
    )


def upsert_product(
    account_id: str,
    name: str,
    category: str,
    latest_sku: str | None,
    is_active: bool,
    description_template: str | None = None,
) -> str:
    if not account_id:
        raise SKUGenerationError("account_id é obrigatório")
    if not name.strip():
        raise SKUGenerationError("Nome do produto é obrigatório")

    normalized_name = " ".join(name.lower().strip().split())
    product_id = _build_product_id(account_id, normalized_name.replace(" ", "_"))
    now_iso = _utc_now_iso()

    upsert_product_row(product_id, {
        "productId": product_id,
        "accountId": account_id,
        "name": name.strip(),
        "normalizedName": normalized_name,
        "category": category.strip() or None,
        "latestSku": (latest_sku or "").strip() or None,
        "descriptionTemplate": (description_template or "").strip() or None,
        "isActive": bool(is_active),
        "updatedAt": now_iso,
        "createdAt": now_iso,
    })

    return product_id