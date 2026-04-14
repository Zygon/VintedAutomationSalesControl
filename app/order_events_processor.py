import hashlib
import re
from datetime import datetime, timezone

from app.config import build_account_id, normalize_email_address
from app.firestore_client import FirebaseClient
from app.gmail_client import GmailClient
from app.parser_order_events import parse_order_event_email


class OrderEventsProcessor:
    def __init__(self, account_id: str, gmail: GmailClient | None = None):
        normalized_account_id = normalize_email_address(account_id)
        if not normalized_account_id:
            raise ValueError("account_id é obrigatório")

        self.account_id = build_account_id(normalized_account_id)
        self.gmail = gmail
        self.firebase = FirebaseClient()

    def process_message_ids(self, message_ids: list[str]) -> dict:
        if not self.gmail:
            raise ValueError("GmailClient é obrigatório para processar mensagens")

        summary = {
            "processed": 0,
            "skipped_unclassified": 0,
            "shipped_updated": 0,
            "shipped_review": 0,
            "completed_updated": 0,
            "completed_reconstructed": 0,
            "sale_items_created": 0,
            "marked_as_read": 0,
            "errors": 0,
        }

        for message_id in message_ids:
            try:
                result = self.process_message(message_id)
                summary["processed"] += 1

                if result == "SKIPPED_UNCLASSIFIED":
                    summary["skipped_unclassified"] += 1
                elif result == "ORDER_SHIPPED_UPDATED":
                    summary["shipped_updated"] += 1
                    summary["marked_as_read"] += 1
                elif result == "ORDER_SHIPPED_REVIEW":
                    summary["shipped_review"] += 1
                elif result == "ORDER_COMPLETED_UPDATED":
                    summary["completed_updated"] += 1
                    summary["marked_as_read"] += 1
                elif result == "ORDER_COMPLETED_RECONSTRUCTED":
                    summary["completed_reconstructed"] += 1
                    summary["marked_as_read"] += 1
                elif result == "ORDER_COMPLETED_UPDATED_WITH_SALE_ITEM":
                    summary["completed_updated"] += 1
                    summary["sale_items_created"] += 1
                    summary["marked_as_read"] += 1
                elif result == "ORDER_COMPLETED_RECONSTRUCTED_WITH_SALE_ITEM":
                    summary["completed_reconstructed"] += 1
                    summary["sale_items_created"] += 1
                    summary["marked_as_read"] += 1
            except Exception as e:
                summary["errors"] += 1
                print(f"[ORDER EVENT ERROR][{self.account_id}] {message_id} -> {e}")

        return summary

    def process_message(self, message_id: str) -> str:
        if not self.gmail:
            raise ValueError("GmailClient é obrigatório para processar mensagens")

        msg, _ = self.gmail.get_email_message(message_id)
        parsed = parse_order_event_email(msg, message_id)

        if not parsed:
            subject = msg.get("Subject", "")
            print(f"[ORDER EVENT SKIP][{self.account_id}] {message_id} subject={subject}")
            return "SKIPPED_UNCLASSIFIED"

        event_type = parsed["eventType"]
        print(f"[ORDER EVENT][{self.account_id}] {message_id} -> {event_type}")

        if event_type == "ORDER_SHIPPED":
            result = self._handle_order_shipped(parsed)
            if result == "ORDER_SHIPPED_UPDATED":
                self.gmail.mark_as_read(message_id)
            return result

        if event_type == "ORDER_COMPLETED":
            result = self._handle_order_completed(parsed)
            if result in {
                "ORDER_COMPLETED_UPDATED",
                "ORDER_COMPLETED_RECONSTRUCTED",
                "ORDER_COMPLETED_UPDATED_WITH_SALE_ITEM",
                "ORDER_COMPLETED_RECONSTRUCTED_WITH_SALE_ITEM",
            }:
                self.gmail.mark_as_read(message_id)
            return result

        return "SKIPPED_UNCLASSIFIED"

    def _handle_order_shipped(self, data: dict) -> str:
        sale_doc = self._find_sale_for_shipped(data)

        if not sale_doc:
            self._log_event(
                event_type="ORDER_SHIPPED_REVIEW",
                entity_id=data["eventEmailId"],
                payload={
                    "reason": "SALE_NOT_FOUND",
                    "orderId": data.get("orderId"),
                    "sku": data.get("sku"),
                    "title": data.get("articleTitle"),
                    "bundleItemCount": data.get("bundleItemCount"),
                },
            )
            print(
                f"[ORDER SHIPPED REVIEW][{self.account_id}] "
                f"orderId={data.get('orderId')} sku={data.get('sku')} title={data.get('articleTitle')}"
            )
            return "ORDER_SHIPPED_REVIEW"

        sale_id = sale_doc.id
        sale = sale_doc.to_dict() or {}

        self.firebase.update_sale(sale_id, {
            "accountId": self.account_id,
            "emailAddress": sale.get("emailAddress"),
            "status": "SHIPPED",
            "shippedAt": data.get("receivedAt"),
            "estimatedDeliveryStartRaw": data.get("estimatedDeliveryStartRaw"),
            "estimatedDeliveryEndRaw": data.get("estimatedDeliveryEndRaw"),
            "updatedAt": self._utc_now_iso(),
        })

        matched_label_id = sale.get("matchedLabelId")
        if matched_label_id:
            self.firebase.update_label(matched_label_id, {
                "status": "SHIPPED",
                "updatedAt": self._utc_now_iso(),
            })

        self._log_event(
            event_type="ORDER_SHIPPED",
            entity_id=sale_id,
            payload={
                "orderId": data.get("orderId"),
                "sku": data.get("sku"),
                "articleTitle": data.get("articleTitle"),
                "estimatedDeliveryStartRaw": data.get("estimatedDeliveryStartRaw"),
                "estimatedDeliveryEndRaw": data.get("estimatedDeliveryEndRaw"),
            },
        )

        print(f"[ORDER SHIPPED OK][{self.account_id}] sale={sale_id}")
        return "ORDER_SHIPPED_UPDATED"

    def _handle_order_completed(self, data: dict) -> str:
        sale_doc = self._find_sale_for_completed(data)

        if sale_doc:
            sale_id = sale_doc.id
            sale = sale_doc.to_dict() or {}

            self.firebase.update_sale(sale_id, {
                "accountId": self.account_id,
                "emailAddress": sale.get("emailAddress") or self.account_id,
                "status": "COMPLETED",
                "orderId": data.get("orderId") or sale.get("orderId"),
                "completedAtRaw": data.get("completedAtRaw"),
                "completedAt": data.get("receivedAt"),
                "completedItemAmount": data.get("itemAmount"),
                "completedShippingAmount": data.get("shippingAmount"),
                "transferredToVintedBalance": data.get("transferredToVintedBalance"),
                "currency": data.get("currency", "EUR"),
                "completionSource": "EMAIL",
                "updatedAt": self._utc_now_iso(),
            })

            matched_label_id = sale.get("matchedLabelId")
            if matched_label_id:
                self.firebase.update_label(matched_label_id, {
                    "status": "COMPLETED",
                    "updatedAt": self._utc_now_iso(),
                })

            sale_item_created = self._ensure_completed_sale_item(
                sale_id=sale_id,
                sale={
                    **sale,
                    "orderId": data.get("orderId") or sale.get("orderId"),
                    "articleTitle": sale.get("articleTitle") or data.get("articleTitle"),
                    "articleTitleNormalized": sale.get("articleTitleNormalized") or data.get("articleTitleNormalized"),
                    "sku": sale.get("sku") or data.get("sku"),
                    "value": sale.get("value") if sale.get("value") is not None else data.get("itemAmount"),
                    "currency": sale.get("currency") or data.get("currency", "EUR"),
                },
                completion_data=data,
                source="EXISTING_SALE_COMPLETION",
            )

            self._log_event(
                event_type="ORDER_COMPLETED",
                entity_id=sale_id,
                payload={
                    "orderId": data.get("orderId"),
                    "itemAmount": data.get("itemAmount"),
                    "shippingAmount": data.get("shippingAmount"),
                    "transferredToVintedBalance": data.get("transferredToVintedBalance"),
                    "saleItemCreated": sale_item_created,
                },
            )

            print(f"[ORDER COMPLETED OK][{self.account_id}] sale={sale_id} saleItemCreated={sale_item_created}")

            if sale_item_created:
                return "ORDER_COMPLETED_UPDATED_WITH_SALE_ITEM"
            return "ORDER_COMPLETED_UPDATED"

        reconstructed_sale_id = self._build_reconstructed_completed_sale_id(
            order_id=data.get("orderId"),
            gmail_message_id=data["eventEmailId"],
        )

        reconstructed_sale = {
            "saleId": reconstructed_sale_id,
            "accountId": self.account_id,
            "emailAddress": self.account_id,
            "orderId": data.get("orderId"),
            "articleTitle": data.get("articleTitle"),
            "articleTitleNormalized": data.get("articleTitleNormalized"),
            "sku": data.get("sku"),
            "value": data.get("itemAmount"),
            "currency": data.get("currency", "EUR"),
            "status": "COMPLETED",
            "completedAtRaw": data.get("completedAtRaw"),
            "completedAt": data.get("receivedAt"),
            "completedItemAmount": data.get("itemAmount"),
            "completedShippingAmount": data.get("shippingAmount"),
            "transferredToVintedBalance": data.get("transferredToVintedBalance"),
            "completionSource": "EMAIL_RECONSTRUCTION",
            "createdFrom": "COMPLETION_EMAIL_RECONSTRUCTION",
            "isReconstructed": True,
            "updatedAt": self._utc_now_iso(),
        }

        self.firebase.upsert_sale(reconstructed_sale_id, reconstructed_sale)

        sale_item_created = self._ensure_completed_sale_item(
            sale_id=reconstructed_sale_id,
            sale=reconstructed_sale,
            completion_data=data,
            source="RECONSTRUCTED_FROM_COMPLETION_EMAIL",
        )

        self._log_event(
            event_type="ORDER_COMPLETED_RECONSTRUCTED",
            entity_id=reconstructed_sale_id,
            payload={
                "orderId": data.get("orderId"),
                "itemAmount": data.get("itemAmount"),
                "shippingAmount": data.get("shippingAmount"),
                "transferredToVintedBalance": data.get("transferredToVintedBalance"),
                "saleItemCreated": sale_item_created,
            },
        )

        print(
            f"[ORDER COMPLETED RECONSTRUCTED][{self.account_id}] "
            f"sale={reconstructed_sale_id} saleItemCreated={sale_item_created}"
        )

        if sale_item_created:
            return "ORDER_COMPLETED_RECONSTRUCTED_WITH_SALE_ITEM"
        return "ORDER_COMPLETED_RECONSTRUCTED"

    def _ensure_completed_sale_item(
        self,
        sale_id: str,
        sale: dict,
        completion_data: dict,
        source: str,
    ) -> bool:
        sale_item_id = f"{sale_id}__1"

        existing_doc = self.firebase.db.collection("saleItems").document(sale_item_id).get()
        if existing_doc.exists:
            return False

        article_title = sale.get("articleTitle") or completion_data.get("articleTitle") or "SEM_ARTIGO"
        article_title_normalized = (
            sale.get("articleTitleNormalized")
            or completion_data.get("articleTitleNormalized")
        )
        sku = sale.get("sku") or completion_data.get("sku")
        order_id = sale.get("orderId") or completion_data.get("orderId")
        buyer_username = sale.get("buyerUsername")
        currency = sale.get("currency") or completion_data.get("currency", "EUR")

        allocated_value = completion_data.get("itemAmount")
        if allocated_value is None:
            allocated_value = sale.get("value")

        self.firebase.upsert_sale_item(sale_item_id, {
            "saleItemId": sale_item_id,
            "accountId": self.account_id,
            "emailAddress": sale.get("emailAddress") or self.account_id,
            "saleId": sale_id,
            "orderId": order_id,
            "shippingCode": sale.get("shippingCode"),
            "buyerUsername": buyer_username,
            "isBundle": False,
            "bundleIndex": 1,
            "articleTitle": article_title,
            "articleTitleNormalized": article_title_normalized,
            "sku": sku,
            "allocatedValue": allocated_value,
            "currency": currency,
            "createdAt": self._utc_now_iso(),
            "source": source,
            "isReconstructed": source != "EXISTING_SALE_COMPLETION",
        })

        return True

    def _find_sale_for_shipped(self, data: dict):
        allowed_statuses = [
            "READY_TO_PRINT",
            "PRINTING",
            "PRINTED",
            "PRINT_DISPATCHED",
            "SHIPPED",
        ]

        if data.get("orderId"):
            docs = self.firebase.find_sales_by_order_id(self.account_id, data["orderId"], allowed_statuses)
            if len(docs) == 1:
                return docs[0]
            if len(docs) > 1:
                return None

        if data.get("isBundle"):
            docs = self.firebase.find_bundle_sales_by_count(
                account_id=self.account_id,
                bundle_item_count=data.get("bundleItemCount"),
                allowed_statuses=allowed_statuses,
            )
            if len(docs) == 1:
                return docs[0]
            return None

        if data.get("sku"):
            docs = self.firebase.find_sales_by_sku(self.account_id, data["sku"], allowed_statuses)
            if len(docs) == 1:
                return docs[0]
            if len(docs) > 1:
                return None

        title_norm = data.get("articleTitleNormalized")
        if title_norm:
            docs = self.firebase.find_sales_by_title(self.account_id, title_norm, allowed_statuses)
            if len(docs) == 1:
                return docs[0]

        return None

    def _find_sale_for_completed(self, data: dict):
        allowed_statuses = [
            "READY_TO_PRINT",
            "PRINTING",
            "PRINTED",
            "PRINT_DISPATCHED",
            "SHIPPED",
            "COMPLETED",
        ]

        if data.get("orderId"):
            docs = self.firebase.find_sales_by_order_id(self.account_id, data["orderId"], allowed_statuses)
            if len(docs) == 1:
                return docs[0]
            if len(docs) > 1:
                return None

        if data.get("sku"):
            docs = self.firebase.find_sales_by_sku(self.account_id, data["sku"], allowed_statuses)
            if len(docs) == 1:
                return docs[0]
            if len(docs) > 1:
                return None

        title_norm = data.get("articleTitleNormalized")
        if title_norm:
            docs = self.firebase.find_sales_by_title(self.account_id, title_norm, allowed_statuses)
            if len(docs) == 1:
                return docs[0]

        return None

    def _build_reconstructed_completed_sale_id(self, order_id: str | None, gmail_message_id: str) -> str:
        raw = f"{self.account_id}:{order_id or 'NO_ORDER'}:{gmail_message_id}:COMPLETED_RECONSTRUCTED"
        return hashlib.sha1(raw.encode()).hexdigest()

    def _log_event(self, event_type: str, entity_id: str, payload: dict):
        event_id = f"{self._utc_now_compact()}__{self._safe_event_part(self.account_id)}__{event_type}__{entity_id}"
        self.firebase.add_event(event_id, {
            "eventId": event_id,
            "accountId": self.account_id,
            "emailAddress": self.account_id,
            "type": event_type,
            "entityId": entity_id,
            "payload": payload,
            "createdAt": self._utc_now_iso(),
        })

    @staticmethod
    def _safe_event_part(value: str) -> str:
        return re.sub(r"[^a-zA-Z0-9._-]+", "_", value)

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _utc_now_compact() -> str:
        return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")