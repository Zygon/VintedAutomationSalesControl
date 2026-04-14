from datetime import datetime, timezone
import re

from app.config import build_account_id, normalize_email_address
from app.firestore_client import FirebaseClient
from app.gmail_client import GmailClient
from app.parser_payouts import parse_payout_email


class PayoutsProcessor:
    def __init__(self, account_id: str, gmail: GmailClient | None = None):
        normalized_account_id = normalize_email_address(account_id)
        if not normalized_account_id:
            raise ValueError("account_id é obrigatório")

        self.account_id = build_account_id(normalized_account_id)
        self.gmail = gmail
        self.firebase = FirebaseClient()

        if self.gmail:
            self.gmail.ensure_vinted_labels()

    def process_message_ids(self, message_ids: list[str]) -> dict:
        if not self.gmail:
            raise ValueError("GmailClient é obrigatório para processar mensagens")

        summary = {
            "processed": 0,
            "created": 0,
            "updated": 0,
            "skipped_unclassified": 0,
            "moved_to_folder": 0,
            "errors": 0,
        }

        for message_id in message_ids:
            try:
                result = self.process_message(message_id)
                summary["processed"] += 1

                if result == "SKIPPED_UNCLASSIFIED":
                    summary["skipped_unclassified"] += 1
                elif result == "PAYOUT_CREATED":
                    summary["created"] += 1
                    summary["moved_to_folder"] += 1
                elif result == "PAYOUT_UPDATED":
                    summary["updated"] += 1
                    summary["moved_to_folder"] += 1
            except Exception as e:
                summary["errors"] += 1
                print(f"[PAYOUT ERROR][{self.account_id}] {message_id} -> {e}")

        return summary

    def process_message(self, message_id: str) -> str:
        if not self.gmail:
            raise ValueError("GmailClient é obrigatório para processar mensagens")

        msg, _ = self.gmail.get_email_message(message_id)
        parsed = parse_payout_email(msg, message_id)

        if not parsed:
            subject = msg.get("Subject", "")
            print(f"[PAYOUT SKIP][{self.account_id}] {message_id} subject={subject}")
            return "SKIPPED_UNCLASSIFIED"

        payout_id = parsed["payoutId"]
        existing = self.firebase.get_payout(payout_id)

        payload = {
            **{k: v for k, v in parsed.items() if k != "rawBodyPreview"},
            "accountId": self.account_id,
            "emailAddress": self.account_id,
            "updatedAt": self._utc_now_iso(),
        }

        if existing.exists:
            self.firebase.update_payout(payout_id, payload)
            self._log_event("PAYOUT_UPDATED", payout_id, {
                "amount": parsed.get("amount"),
                "transferReference": parsed.get("transferReference"),
            })
            self.gmail.move_to_label_folder(message_id, "Vinted/Payouts")
            print(f"[PAYOUT UPDATED][{self.account_id}] {payout_id} amount={parsed.get('amount')}")
            return "PAYOUT_UPDATED"

        payload["createdAt"] = self._utc_now_iso()
        self.firebase.upsert_payout(payout_id, payload)

        self._log_event("PAYOUT_CREATED", payout_id, {
            "amount": parsed.get("amount"),
            "transferReference": parsed.get("transferReference"),
        })
        self.gmail.move_to_label_folder(message_id, "Vinted/Payouts")
        print(f"[PAYOUT CREATED][{self.account_id}] {payout_id} amount={parsed.get('amount')}")
        return "PAYOUT_CREATED"

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