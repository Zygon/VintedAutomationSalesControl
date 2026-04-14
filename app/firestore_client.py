import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1.base_query import FieldFilter

from app.config import FIREBASE_SERVICE_ACCOUNT_FILE


class FirebaseClient:
    def __init__(self):
        if not firebase_admin._apps:
            cred = credentials.Certificate(str(FIREBASE_SERVICE_ACCOUNT_FILE))
            firebase_admin.initialize_app(cred)

        self.db = firestore.client()

    # -------------------------
    # ACCOUNTS
    # -------------------------
    def get_account(self, account_id: str):
        return self.db.collection("accounts").document(account_id).get()

    def upsert_account(self, account_id: str, data: dict):
        self.db.collection("accounts").document(account_id).set(data, merge=True)

    def ensure_account(self, account_id: str, data: dict):
        existing = self.get_account(account_id)
        if existing.exists:
            current = existing.to_dict() or {}
            merged = {**current, **data}
            self.upsert_account(account_id, merged)
            return False

        self.upsert_account(account_id, data)
        return True

    def list_active_accounts(self):
        docs = (
            self.db.collection("accounts")
            .where(filter=FieldFilter("status", "==", "ACTIVE"))
            .stream()
        )
        return list(docs)

    def touch_account_last_run(self, account_id: str, timestamp_iso: str):
        self.upsert_account(account_id, {
            "lastRunAt": timestamp_iso,
            "updatedAt": timestamp_iso,
        })

    # -------------------------
    # SALES
    # -------------------------
    def upsert_sale(self, sale_id: str, data: dict):
        self.db.collection("sales").document(sale_id).set(data, merge=True)

    def get_sale(self, sale_id: str):
        return self.db.collection("sales").document(sale_id).get()

    def update_sale(self, sale_id: str, data: dict):
        self.db.collection("sales").document(sale_id).set(data, merge=True)

    def find_waiting_sale_by_order_id(self, account_id: str, order_id: str):
        docs = (
            self.db.collection("sales")
            .where(filter=FieldFilter("accountId", "==", account_id))
            .where(filter=FieldFilter("status", "==", "WAITING_LABEL"))
            .where(filter=FieldFilter("orderId", "==", order_id))
            .stream()
        )
        return list(docs)

    def find_waiting_sale_by_title(self, account_id: str, article_title_normalized: str):
        docs = (
            self.db.collection("sales")
            .where(filter=FieldFilter("accountId", "==", account_id))
            .where(filter=FieldFilter("status", "==", "WAITING_LABEL"))
            .where(filter=FieldFilter("articleTitleNormalized", "==", article_title_normalized))
            .stream()
        )
        return list(docs)

    def find_waiting_sales_by_sku(self, account_id: str, sku: str):
        docs = (
            self.db.collection("sales")
            .where(filter=FieldFilter("accountId", "==", account_id))
            .where(filter=FieldFilter("status", "==", "WAITING_LABEL"))
            .where(filter=FieldFilter("sku", "==", sku))
            .stream()
        )
        return list(docs)

    def find_waiting_sales(self, account_id: str):
        docs = (
            self.db.collection("sales")
            .where(filter=FieldFilter("accountId", "==", account_id))
            .where(filter=FieldFilter("status", "==", "WAITING_LABEL"))
            .stream()
        )
        return list(docs)

    def find_waiting_bundle_sales_by_count(self, account_id: str, bundle_item_count: int | None):
        if not bundle_item_count or bundle_item_count <= 0:
            return []

        docs = (
            self.db.collection("sales")
            .where(filter=FieldFilter("accountId", "==", account_id))
            .where(filter=FieldFilter("status", "==", "WAITING_LABEL"))
            .where(filter=FieldFilter("isBundleSale", "==", True))
            .where(filter=FieldFilter("bundleItemCount", "==", bundle_item_count))
            .stream()
        )
        return list(docs)

    def find_sales_by_order_id(self, account_id: str, order_id: str, allowed_statuses: list[str]):
        docs = (
            self.db.collection("sales")
            .where(filter=FieldFilter("accountId", "==", account_id))
            .where(filter=FieldFilter("orderId", "==", order_id))
            .where(filter=FieldFilter("status", "in", allowed_statuses))
            .stream()
        )
        return list(docs)

    def find_sales_by_title(self, account_id: str, article_title_normalized: str, allowed_statuses: list[str]):
        docs = (
            self.db.collection("sales")
            .where(filter=FieldFilter("accountId", "==", account_id))
            .where(filter=FieldFilter("articleTitleNormalized", "==", article_title_normalized))
            .where(filter=FieldFilter("status", "in", allowed_statuses))
            .stream()
        )
        return list(docs)

    def find_sales_by_sku(self, account_id: str, sku: str, allowed_statuses: list[str]):
        docs = (
            self.db.collection("sales")
            .where(filter=FieldFilter("accountId", "==", account_id))
            .where(filter=FieldFilter("sku", "==", sku))
            .where(filter=FieldFilter("status", "in", allowed_statuses))
            .stream()
        )
        return list(docs)

    def find_bundle_sales_by_count(self, account_id: str, bundle_item_count: int | None, allowed_statuses: list[str]):
        if not bundle_item_count or bundle_item_count <= 0:
            return []

        docs = (
            self.db.collection("sales")
            .where(filter=FieldFilter("accountId", "==", account_id))
            .where(filter=FieldFilter("isBundleSale", "==", True))
            .where(filter=FieldFilter("bundleItemCount", "==", bundle_item_count))
            .where(filter=FieldFilter("status", "in", allowed_statuses))
            .stream()
        )
        return list(docs)

    # -------------------------
    # LABELS
    # -------------------------
    def upsert_label(self, label_id: str, data: dict):
        self.db.collection("labels").document(label_id).set(data, merge=True)

    def get_label(self, label_id: str):
        return self.db.collection("labels").document(label_id).get()

    def update_label(self, label_id: str, data: dict):
        self.db.collection("labels").document(label_id).set(data, merge=True)

    def get_labels_for_enrichment(self, account_id: str):
        docs = (
            self.db.collection("labels")
            .where(filter=FieldFilter("accountId", "==", account_id))
            .where(filter=FieldFilter("status", "in", ["LABEL_RECEIVED", "MATCH_PENDING", "REVIEW"]))
            .stream()
        )
        return list(docs)

    # -------------------------
    # PRINT JOBS
    # -------------------------
    def get_print_job(self, print_job_id: str):
        return self.db.collection("printJobs").document(print_job_id).get()

    def upsert_print_job(self, print_job_id: str, data: dict):
        self.db.collection("printJobs").document(print_job_id).set(data, merge=True)

    def create_print_job_if_missing(self, print_job_id: str, data: dict):
        doc = self.get_print_job(print_job_id)
        if not doc.exists:
            self.upsert_print_job(print_job_id, data)
            return True
        return False

    def update_print_job(self, print_job_id: str, data: dict):
        self.db.collection("printJobs").document(print_job_id).set(data, merge=True)

    def get_pending_print_jobs(self, account_id: str):
        docs = (
            self.db.collection("printJobs")
            .where(filter=FieldFilter("accountId", "==", account_id))
            .where(filter=FieldFilter("status", "==", "PENDING"))
            .stream()
        )
        return list(docs)

    # -------------------------
    # EVENTS
    # -------------------------
    def add_event(self, event_id: str, data: dict):
        self.db.collection("events").document(event_id).set(data, merge=True)

    # -------------------------
    # SALE ITEMS
    # -------------------------
    def upsert_sale_item(self, sale_item_id: str, data: dict):
        self.db.collection("saleItems").document(sale_item_id).set(data, merge=True)

    # -------------------------
    # EXPENSES
    # -------------------------
    def get_expense(self, expense_id: str):
        return self.db.collection("expenses").document(expense_id).get()

    def upsert_expense(self, expense_id: str, data: dict):
        self.db.collection("expenses").document(expense_id).set(data, merge=True)

    def update_expense(self, expense_id: str, data: dict):
        self.db.collection("expenses").document(expense_id).set(data, merge=True)

    # -------------------------
    # PAYOUTS
    # -------------------------
    def get_payout(self, payout_id: str):
        return self.db.collection("payouts").document(payout_id).get()

    def upsert_payout(self, payout_id: str, data: dict):
        self.db.collection("payouts").document(payout_id).set(data, merge=True)

    def update_payout(self, payout_id: str, data: dict):
        self.db.collection("payouts").document(payout_id).set(data, merge=True)