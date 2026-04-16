from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from google.cloud import firestore

from services.firestore_queries import get_db

DEFAULT_PREFIX = "SKU"
DEFAULT_PADDING = 3


class SKUGenerationError(RuntimeError):
    pass


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_counter_id(account_id: str, prefix: str) -> str:
    return f"{account_id}__{prefix}"


def _build_generated_sku_id(account_id: str, sku: str) -> str:
    return f"{account_id}__{sku}"


def _build_product_id(account_id: str, normalized_name: str) -> str:
    return f"{account_id}__{normalized_name}"


def generate_next_sku(account_id: str, prefix: str = DEFAULT_PREFIX, padding: int = DEFAULT_PADDING) -> dict[str, Any]:
    if not account_id:
        raise SKUGenerationError("Seleciona uma conta específica antes de gerar SKU.")

    db = get_db()
    counter_id = _build_counter_id(account_id, prefix)
    counter_ref = db.collection("skuCounters").document(counter_id)

    @firestore.transactional
    def _tx(transaction: firestore.Transaction):
        snap = counter_ref.get(transaction=transaction)
        current_value = 0
        if snap.exists:
            current_value = int((snap.to_dict() or {}).get("currentValue") or 0)

        next_value = current_value + 1
        next_sku = f"{prefix}{str(next_value).zfill(padding)}"
        now_iso = _utc_now_iso()

        transaction.set(
            counter_ref,
            {
                "counterId": counter_id,
                "accountId": account_id,
                "prefix": prefix,
                "currentValue": next_value,
                "padding": padding,
                "latestSku": next_sku,
                "updatedAt": now_iso,
                "createdAt": now_iso if not snap.exists else firestore.DELETE_FIELD,
            },
            merge=True,
        )

        generated_ref = db.collection("generatedSkus").document(_build_generated_sku_id(account_id, next_sku))
        transaction.set(
            generated_ref,
            {
                "generatedSkuId": _build_generated_sku_id(account_id, next_sku),
                "accountId": account_id,
                "sku": next_sku,
                "numericValue": next_value,
                "prefix": prefix,
                "isAssigned": False,
                "assignedProductId": None,
                "generatedFor": "LISTING_HELPER",
                "generatedBy": "streamlit-local-ui",
                "createdAt": now_iso,
                "updatedAt": now_iso,
            },
            merge=True,
        )

        return {
            "sku": next_sku,
            "numericValue": next_value,
            "accountId": account_id,
            "prefix": prefix,
            "padding": padding,
        }

    transaction = db.transaction()
    return _tx(transaction)


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

    db = get_db()
    doc_ref = db.collection("products").document(product_id)
    doc_ref.set(
        {
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
        },
        merge=True,
    )
    return product_id
