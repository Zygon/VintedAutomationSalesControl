from pathlib import Path
import hashlib
import re
from datetime import datetime, timezone

from app.config import (
    GMAIL_LABELS_QUERY,
    GMAIL_SALES_QUERY,
    build_account_id,
    get_account_dirs,
    get_gmail_token_file,
    normalize_email_address,
)
from app.firestore_client import FirebaseClient
from app.gmail_client import GmailClient
from app.parser_labels import parse_label_email
from app.parser_sales import parse_sale_email
from app.pdf_enricher import enrich_label_pdf


FINAL_LABEL_STATUSES = {
    "READY_TO_PRINT",
    "PRINTING",
    "PRINTED",
    "PRINT_DISPATCHED",
    "ARCHIVED",
}


class VintedWorker:
    def __init__(self, account_id: str):
        normalized_account_id = normalize_email_address(account_id)
        if not normalized_account_id:
            raise ValueError("account_id é obrigatório")

        self.account_id = build_account_id(normalized_account_id)
        self.token_file = get_gmail_token_file(self.account_id)
        self.gmail = GmailClient(token_file=self.token_file)

        authenticated_email = normalize_email_address(self.gmail.get_authenticated_email_address())
        if authenticated_email != self.account_id:
            raise ValueError(
                f"Token inválido para a conta pedida. "
                f"Esperado={self.account_id} | autenticado={authenticated_email}"
            )

        self.email_address = authenticated_email
        self.firebase = FirebaseClient()
        self.dirs = get_account_dirs(self.account_id)

        self.gmail.ensure_vinted_labels()

    @classmethod
    def bootstrap_account_via_login(cls) -> dict:
        temp_token_file = get_gmail_token_file("__bootstrap__temp__")
        gmail = GmailClient(token_file=temp_token_file)

        email_address = normalize_email_address(gmail.get_authenticated_email_address())
        if not email_address:
            raise ValueError("Não foi possível obter o email autenticado do Gmail")

        account_id = build_account_id(email_address)
        final_token_file = get_gmail_token_file(account_id)

        if temp_token_file != final_token_file:
            final_token_file.parent.mkdir(parents=True, exist_ok=True)
            temp_token_file.replace(final_token_file)

        firebase = FirebaseClient()
        now_iso = cls._utc_now_iso()

        firebase.ensure_account(account_id, {
            "accountId": account_id,
            "emailAddress": email_address,
            "status": "ACTIVE",
            "authProvider": "google_oauth",
            "createdAt": now_iso,
            "updatedAt": now_iso,
        })

        get_account_dirs(account_id)
        gmail.ensure_vinted_labels()

        return {
            "accountId": account_id,
            "emailAddress": email_address,
            "tokenFile": str(final_token_file.resolve()),
        }

    def run_once(self):
        self.ingest_sales()
        self.ingest_labels()
        self.process_labels_for_enrichment()
        self.firebase.touch_account_last_run(self.account_id, self._utc_now_iso())

    def ingest_sales(self):
        for message_id in self.gmail.list_message_ids(GMAIL_SALES_QUERY):
            sale_id = self._build_scoped_message_id(message_id)
            existing_sale_doc = self.firebase.get_sale(sale_id)

            msg, _ = self.gmail.get_email_message(message_id)
            sale = parse_sale_email(msg, message_id)

            sale["saleId"] = sale_id
            sale["accountId"] = self.account_id
            sale["emailAddress"] = self.email_address

            sale_to_store = {k: v for k, v in sale.items() if k != "rawBodyPreview"}

            if existing_sale_doc.exists:
                existing_sale = existing_sale_doc.to_dict() or {}

                needs_update = (
                    not existing_sale.get("buyerUsername")
                    or not existing_sale.get("articleTitle")
                    or not existing_sale.get("articleTitleNormalized")
                    or existing_sale.get("value") is None
                    or (
                        not existing_sale.get("isBundleSale")
                        and not existing_sale.get("sku")
                    )
                    or existing_sale.get("isBundleSale") is None
                    or (
                        existing_sale.get("isBundleSale") is True
                        and existing_sale.get("bundleItemCount") is None
                    )
                    or existing_sale.get("status") in {None, "", "ERROR"}
                )

                if needs_update:
                    self.firebase.update_sale(sale_id, sale_to_store)
                    self._log_event(
                        event_type="SALE_UPDATED",
                        entity_id=sale_id,
                        payload={
                            "buyer": sale.get("buyerUsername"),
                            "title": sale.get("articleTitle"),
                            "sku": sale.get("sku"),
                            "value": sale.get("value"),
                            "isBundleSale": sale.get("isBundleSale"),
                            "bundleItemCount": sale.get("bundleItemCount"),
                        },
                    )
                    self.gmail.move_to_label_folder(message_id, "Vinted/Vendas")
                    print(f"[SALE UPDATED][{self.account_id}] {sale_id} -> {sale.get('articleTitle')}")
                else:
                    self.gmail.move_to_label_folder(message_id, "Vinted/Vendas")
                    print(f"[SKIP SALE][{self.account_id}] {sale_id}")
                continue

            self.firebase.upsert_sale(sale_id, sale_to_store)
            self._log_event(
                event_type="SALE_CREATED",
                entity_id=sale_id,
                payload={
                    "buyer": sale.get("buyerUsername"),
                    "title": sale.get("articleTitle"),
                    "sku": sale.get("sku"),
                    "value": sale.get("value"),
                    "isBundleSale": sale.get("isBundleSale"),
                    "bundleItemCount": sale.get("bundleItemCount"),
                },
            )
            self.gmail.move_to_label_folder(message_id, "Vinted/Vendas")
            print(f"[SALE][{self.account_id}] {sale_id} -> {sale.get('articleTitle')}")

    def ingest_labels(self):
        raw_labels_dir = self.dirs["rawLabelsDir"]

        for message_id in self.gmail.list_message_ids(GMAIL_LABELS_QUERY):
            label_id = self._build_scoped_message_id(message_id)
            existing_label_doc = self.firebase.get_label(label_id)

            if existing_label_doc.exists:
                existing_label = existing_label_doc.to_dict() or {}
                if existing_label.get("status") in FINAL_LABEL_STATUSES:
                    self.gmail.move_to_label_folder(message_id, "Vinted/Etiquetas")
                    print(f"[SKIP LABEL][{self.account_id}] {label_id} já finalizada")
                    continue

            msg, _ = self.gmail.get_email_message(message_id)
            label = parse_label_email(msg, message_id)

            label["labelId"] = label_id
            label["accountId"] = self.account_id
            label["emailAddress"] = self.email_address

            local_pdf = self.gmail.download_pdf_attachment(message_id, raw_labels_dir)
            if local_pdf:
                label["localRawPdfPath"] = str(local_pdf.resolve())
                label["status"] = "LABEL_RECEIVED"
                print(f"[PDF DOWNLOADED][{self.account_id}] {local_pdf}")
            else:
                label["localRawPdfPath"] = None
                label["status"] = "ERROR"
                label["errorReason"] = "RAW_PDF_NOT_FOUND"
                print(f"[PDF NOT FOUND][{self.account_id}] message_id={message_id}")

            label_to_store = {k: v for k, v in label.items() if k != "rawBodyPreview"}
            self.firebase.upsert_label(label_id, label_to_store)

            self._log_event(
                event_type="LABEL_INGESTED",
                entity_id=label_id,
                payload={
                    "orderId": label.get("orderId"),
                    "title": label.get("articleTitle"),
                    "bundleArticles": label.get("bundleArticles", []),
                    "rawPdf": label.get("localRawPdfPath"),
                    "isBundle": label.get("isBundle"),
                    "skus": label.get("skus", []),
                },
            )

            if local_pdf:
                self.gmail.move_to_label_folder(message_id, "Vinted/Etiquetas")

            print(f"[LABEL][{self.account_id}] {label_id} -> {label.get('articleTitle')}")

    def process_labels_for_enrichment(self):
        label_docs = self.firebase.get_labels_for_enrichment(self.account_id)

        for label_doc in label_docs:
            label_id = label_doc.id
            label = label_doc.to_dict() or {}
            label["labelId"] = label_id
            self._try_match_and_enrich(label)

    def _is_label_already_enriched(self, label: dict) -> bool:
        return label.get("status") in FINAL_LABEL_STATUSES

    def _try_match_and_enrich(self, label: dict):
        label_id = label["labelId"]

        if self._is_label_already_enriched(label):
            print(f"[SKIP ENRICH][{self.account_id}] {label_id} já finalizada")
            return

        raw_pdf_path = label.get("localRawPdfPath")
        if not raw_pdf_path or not Path(raw_pdf_path).exists():
            self.firebase.update_label(label_id, {
                "status": "ERROR",
                "errorReason": "RAW_PDF_MISSING",
            })
            self._log_event("LABEL_ERROR", label_id, {"reason": "RAW_PDF_MISSING"})
            print(f"[ENRICH FAIL][{self.account_id}] RAW_PDF_MISSING")
            return

        matches = []
        match_reason = None

        if label.get("isBundle"):
            matches, match_reason = self._find_bundle_matches(label)
            print(f"[MATCH BUNDLE][{self.account_id}] reason={match_reason} -> {len(matches)}")
        else:
            matches, match_reason = self._find_single_matches(label)
            print(f"[MATCH SINGLE][{self.account_id}] reason={match_reason} -> {len(matches)}")

        if len(matches) == 0:
            self.firebase.update_label(label_id, {
                "status": "REVIEW",
                "reviewReason": match_reason or "MATCH_COUNT_0",
                "matchOrderId": label.get("orderId"),
                "matchTitleNormalized": label.get("articleTitleNormalized"),
                "isBundle": label.get("isBundle", False),
                "skus": label.get("skus", []),
            })
            self._log_event("LABEL_REVIEW", label_id, {"reason": match_reason or "MATCH_COUNT_0"})
            print(f"[MATCH FAIL][{self.account_id}] 0 matches")
            return

        sale_doc = matches[0]
        sale = sale_doc.to_dict() or {}
        sale_id = sale_doc.id

        buyer_username = sale.get("buyerUsername") or "NO_BUYER"

        if label.get("isBundle"):
            sku_or_legacy = "BUNDLE"
            title_for_name = "bundle"
            article_for_overlay = self._bundle_overlay_title(label)
        else:
            sku_value = sale.get("sku")
            sku_or_legacy = sku_value if sku_value else "LEGACY"
            title_for_name = sale.get("articleTitle") or label.get("articleTitle") or "sem-titulo"
            article_for_overlay = sale.get("articleTitle")

        title_slug = self._slugify_title(title_for_name)

        safe_order = self._safe_filename_part(label.get("orderId") or "NO_ORDER")
        safe_sku = self._safe_filename_part(sku_or_legacy)
        safe_buyer = self._safe_filename_part(buyer_username)
        safe_slug = self._safe_filename_part(title_slug)

        output_filename = f"{safe_order}__{safe_sku}__{safe_buyer}__{safe_slug}.pdf"
        output_path = self.dirs["enrichedLabelsDir"] / output_filename

        if not output_path.exists():
            enrich_label_pdf(
                input_pdf_path=raw_pdf_path,
                output_pdf_path=output_path,
                order_id=label.get("orderId"),
                sku="" if label.get("isBundle") else sku_or_legacy,
                buyer_username=buyer_username,
                value=sale.get("value"),
                article_title=article_for_overlay,
            )
            print(f"[ENRICHED][{self.account_id}] {output_path}")
        else:
            print(f"[SKIP ENRICH][{self.account_id}] {output_path}")

        self.firebase.update_sale(sale_id, {
            "accountId": self.account_id,
            "emailAddress": self.email_address,
            "status": "READY_TO_PRINT",
            "matchedLabelId": label_id,
            "orderId": label.get("orderId"),
            "shippingCode": label.get("shippingCode"),
            "localEnrichedPdfPath": str(output_path.resolve()),
            "matchedBy": match_reason,
        })

        self.firebase.update_label(label_id, {
            "accountId": self.account_id,
            "emailAddress": self.email_address,
            "status": "READY_TO_PRINT",
            "matchedSaleId": sale_id,
            "localEnrichedPdfPath": str(output_path.resolve()),
            "matchedBy": match_reason,
        })

        self._create_sale_items(sale_id, sale, label)

        self._log_event(
            event_type="LABEL_ENRICHED",
            entity_id=label_id,
            payload={
                "saleId": sale_id,
                "outputPath": str(output_path.resolve()),
                "buyer": buyer_username,
                "sku": sku_or_legacy,
                "isBundle": label.get("isBundle", False),
                "matchedBy": match_reason,
            },
        )

        print_job_id = label_id
        created = self.firebase.create_print_job_if_missing(print_job_id, {
            "printJobId": print_job_id,
            "accountId": self.account_id,
            "emailAddress": self.email_address,
            "labelId": label_id,
            "saleId": sale_id,
            "pdfPath": str(output_path.resolve()),
            "status": "PENDING",
            "createdAt": self._utc_now_iso(),
            "updatedAt": self._utc_now_iso(),
        })

        if created:
            self._log_event(
                event_type="PRINT_JOB_CREATED",
                entity_id=print_job_id,
                payload={"labelId": label_id, "saleId": sale_id},
            )
            print(f"[PRINT JOB CREATED][{self.account_id}] {print_job_id}")

    def _find_bundle_matches(self, label: dict):
        if label.get("orderId"):
            by_order = self.firebase.find_waiting_sale_by_order_id(
                self.account_id,
                label["orderId"],
            )
            if len(by_order) == 1:
                return by_order, "BUNDLE_ORDER_ID_EXACT"
            if len(by_order) > 1:
                return [self._select_match_fifo(by_order)], "BUNDLE_ORDER_ID_FIFO"

        bundle_count = self._get_label_bundle_count(label)
        if not bundle_count:
            return [], "BUNDLE_WITHOUT_ITEM_COUNT"

        count_candidates = self.firebase.find_waiting_bundle_sales_by_count(
            self.account_id,
            bundle_count,
        )

        if len(count_candidates) == 0:
            return [], "BUNDLE_COUNT_NO_MATCH"

        if len(count_candidates) == 1:
            return count_candidates, "BUNDLE_COUNT_UNIQUE"

        return [self._select_match_fifo(count_candidates)], "BUNDLE_COUNT_FIFO"

    def _find_single_matches(self, label: dict):
        if label.get("orderId"):
            matches = self.firebase.find_waiting_sale_by_order_id(
                self.account_id,
                label["orderId"],
            )
            if len(matches) == 1:
                return matches, "ORDER_ID_EXACT"
            if len(matches) > 1:
                return [self._select_match_fifo(matches)], "ORDER_ID_FIFO"

        label_skus = label.get("skus") or []
        if len(label_skus) == 1:
            sku = label_skus[0].upper().strip()
            matches = self.firebase.find_waiting_sales_by_sku(self.account_id, sku)
            if len(matches) == 1:
                return matches, "SKU_EXACT"
            if len(matches) > 1:
                return [self._select_match_fifo(matches)], "SKU_FIFO"

        title_norm = label.get("articleTitleNormalized")
        if not title_norm:
            return [], "LABEL_WITHOUT_NORMALIZED_TITLE"

        matches = self.firebase.find_waiting_sale_by_title(self.account_id, title_norm)
        if len(matches) == 1:
            return matches, "TITLE_EXACT"
        if len(matches) > 1:
            return [self._select_match_fifo(matches)], "TITLE_FIFO"

        return [], "MATCH_COUNT_0"

    def _select_match_fifo(self, matches):
        if len(matches) == 1:
            return matches[0]

        sortable = []
        for doc in matches:
            sale = doc.to_dict() or {}
            sold_at = self._parse_datetime(sale.get("soldAt"))
            sortable.append((sold_at, doc))

        sortable.sort(key=lambda x: x[0] or datetime.max)
        return sortable[0][1]

    def _create_sale_items(self, sale_id: str, sale: dict, label: dict):
        total_value = sale.get("value")
        buyer = sale.get("buyerUsername")
        order_id = label.get("orderId")
        shipping_code = label.get("shippingCode")

        if label.get("isBundle"):
            items = label.get("bundleArticles", [])
            if not items:
                items = [label.get("articleTitle") or "BUNDLE"]

            allocations = self._allocate_bundle_values(total_value, len(items))

            for idx, article_name in enumerate(items):
                sku = self._extract_first_sku(article_name)
                sale_item_id = f"{sale_id}__{idx + 1}"

                self.firebase.upsert_sale_item(sale_item_id, {
                    "saleItemId": sale_item_id,
                    "accountId": self.account_id,
                    "emailAddress": self.email_address,
                    "saleId": sale_id,
                    "orderId": order_id,
                    "shippingCode": shipping_code,
                    "buyerUsername": buyer,
                    "isBundle": True,
                    "bundleIndex": idx + 1,
                    "articleTitle": self._remove_sku_prefix(article_name),
                    "articleTitleNormalized": self._normalize_text(self._remove_sku_prefix(article_name)),
                    "sku": sku,
                    "allocatedValue": allocations[idx],
                    "currency": sale.get("currency", "EUR"),
                    "createdAt": self._utc_now_iso(),
                })
            return

        article_name = sale.get("articleTitle") or label.get("articleTitle") or "SEM_ARTIGO"
        sku = sale.get("sku") or self._extract_first_sku(article_name)
        sale_item_id = f"{sale_id}__1"

        self.firebase.upsert_sale_item(sale_item_id, {
            "saleItemId": sale_item_id,
            "accountId": self.account_id,
            "emailAddress": self.email_address,
            "saleId": sale_id,
            "orderId": order_id,
            "shippingCode": shipping_code,
            "buyerUsername": buyer,
            "isBundle": False,
            "bundleIndex": 1,
            "articleTitle": self._remove_sku_prefix(article_name),
            "articleTitleNormalized": self._normalize_text(self._remove_sku_prefix(article_name)),
            "sku": sku,
            "allocatedValue": float(total_value) if total_value is not None else None,
            "currency": sale.get("currency", "EUR"),
            "createdAt": self._utc_now_iso(),
        })

    def _allocate_bundle_values(self, total_value, count: int) -> list[float]:
        if total_value is None or count <= 0:
            return [None] * count

        total_cents = int(round(float(total_value) * 100))
        base = total_cents // count
        remainder = total_cents % count

        allocations = []
        for i in range(count):
            cents = base + (1 if i < remainder else 0)
            allocations.append(cents / 100)

        return allocations

    def _get_label_bundle_count(self, label: dict) -> int | None:
        bundle_articles = label.get("bundleArticles") or []
        skus = label.get("skus") or []

        if len(bundle_articles) > 1:
            return len(bundle_articles)

        if len(skus) > 1:
            return len(skus)

        return None

    def _bundle_overlay_title(self, label: dict) -> str:
        items = label.get("bundleArticles", [])
        if not items:
            return "BUNDLE"

        cleaned = [self._remove_sku_prefix(x) for x in items]
        return "\n".join(cleaned)

    def _log_event(self, event_type: str, entity_id: str, payload: dict):
        event_id = f"{self._utc_now_compact()}__{self._safe_event_part(self.account_id)}__{event_type}__{entity_id}"
        self.firebase.add_event(event_id, {
            "eventId": event_id,
            "accountId": self.account_id,
            "emailAddress": self.email_address,
            "type": event_type,
            "entityId": entity_id,
            "payload": payload,
            "createdAt": self._utc_now_iso(),
        })

    def _build_scoped_message_id(self, message_id: str) -> str:
        raw = f"{self.account_id}:{message_id}"
        return hashlib.sha1(raw.encode()).hexdigest()

    @staticmethod
    def _safe_event_part(value: str) -> str:
        return re.sub(r"[^a-zA-Z0-9._-]+", "_", value)

    @staticmethod
    def _extract_first_sku(text: str | None) -> str | None:
        if not text:
            return None
        match = re.search(r"\[(SKU\d+)\]", text, re.IGNORECASE)
        return match.group(1).upper() if match else None

    @staticmethod
    def _remove_sku_prefix(text: str) -> str:
        return re.sub(r"^\s*\[SKU\d+\]\s*", "", text, flags=re.IGNORECASE).strip()

    @staticmethod
    def _normalize_text(text: str | None) -> str | None:
        if not text:
            return None
        text = text.lower().strip()
        text = re.sub(r"[–—-]", " ", text)
        text = re.sub(r"[^\w\s]", " ", text)
        text = re.sub(r"\s+", " ", text)
        return text

    @staticmethod
    def _parse_datetime(value):
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None

    @staticmethod
    def _slugify_title(title: str) -> str:
        text = title.lower().strip()
        text = text.replace("–", "-").replace("—", "-").replace("|", " ")
        text = re.sub(r"[^a-z0-9\s-]", "", text)
        text = re.sub(r"[\s-]+", "-", text).strip("-")
        return text[:60] or "sem-titulo"

    @staticmethod
    def _safe_filename_part(value: str) -> str:
        invalid_chars = '<>:"/\\|?*'
        cleaned = "".join("_" if c in invalid_chars else c for c in value)
        return cleaned.strip()[:80]

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _utc_now_compact() -> str:
        return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")