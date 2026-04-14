import argparse
import shutil
import sys
from pathlib import Path
from datetime import datetime, timezone

from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

from app.config import get_account_dirs, build_account_id, normalize_email_address
from app.firestore_client import FirebaseClient


RESETTABLE_COLLECTIONS = (
    "sales",
    "labels",
    "saleItems",
    "printJobs",
    "events",
    "expenses",
    "payouts",
)

DELETE_FIELD = firestore.DELETE_FIELD
BATCH_LIMIT = 400


class AccountResetService:
    def __init__(self, account_id: str):
        normalized = normalize_email_address(account_id)
        if not normalized:
            raise ValueError("account_id é obrigatório")

        self.account_id = build_account_id(normalized)
        self.firebase = FirebaseClient()
        self.db = self.firebase.db
        self.dirs = get_account_dirs(self.account_id)

    def reset_derived_data(self, dry_run: bool = True) -> dict:
        before = self._collect_derived_snapshot()

        if dry_run:
            return {
                "mode": "derived",
                "accountId": self.account_id,
                "before": before,
                "after": None,
                "executed": False,
            }

        sale_items = self._get_docs_by_account("saleItems")
        print_jobs = self._get_docs_by_account("printJobs")
        events = self._get_docs_by_account("events")
        sales = self._get_docs_by_account("sales")
        labels = self._get_docs_by_account("labels")

        self._delete_docs_in_batches(sale_items)
        self._delete_docs_in_batches(print_jobs)
        self._delete_docs_in_batches(events)

        self._reset_sales_in_batches(sales)
        self._reset_labels_in_batches(labels)

        self._clear_directory(self.dirs["enrichedLabelsDir"])
        self._clear_directory(self.dirs["printedLabelsDir"])

        after = self._collect_derived_snapshot()

        return {
            "mode": "derived",
            "accountId": self.account_id,
            "before": before,
            "after": after,
            "executed": True,
        }

    def reset_full_account_data(
        self,
        dry_run: bool = True,
        delete_account_document: bool = False,
    ) -> dict:
        before = self._collect_full_snapshot(delete_account_document=delete_account_document)

        if dry_run:
            return {
                "mode": "full",
                "accountId": self.account_id,
                "before": before,
                "after": None,
                "executed": False,
            }

        docs_by_collection = {
            name: self._get_docs_by_account(name)
            for name in RESETTABLE_COLLECTIONS
        }

        for docs in docs_by_collection.values():
            self._delete_docs_in_batches(docs)

        self._delete_directory(self.dirs["rootDir"])

        if delete_account_document:
            account_doc = self.firebase.get_account(self.account_id)
            if account_doc.exists:
                self.db.collection("accounts").document(self.account_id).delete()

        after = self._collect_full_snapshot(delete_account_document=delete_account_document)

        return {
            "mode": "full",
            "accountId": self.account_id,
            "before": before,
            "after": after,
            "executed": True,
        }

    def _collect_derived_snapshot(self) -> dict:
        return {
            "docs": {
                "saleItems": self._count_docs_by_account("saleItems"),
                "printJobs": self._count_docs_by_account("printJobs"),
                "events": self._count_docs_by_account("events"),
                "sales": self._count_docs_by_account("sales"),
                "labels": self._count_docs_by_account("labels"),
                "expenses": self._count_docs_by_account("expenses"),
                "payouts": self._count_docs_by_account("payouts"),
            },
            "statusBreakdown": {
                "sales": self._count_statuses("sales"),
                "labels": self._count_statuses("labels"),
                "printJobs": self._count_statuses("printJobs"),
                "expenses": self._count_statuses("expenses"),
                "payouts": self._count_statuses("payouts"),
            },
            "files": {
                "enriched_labels": self._count_files(self.dirs["enrichedLabelsDir"]),
                "printed_labels": self._count_files(self.dirs["printedLabelsDir"]),
            },
        }

    def _collect_full_snapshot(self, delete_account_document: bool) -> dict:
        account_exists = self.firebase.get_account(self.account_id).exists

        return {
            "docs": {
                name: self._count_docs_by_account(name)
                for name in RESETTABLE_COLLECTIONS
            },
            "files": {
                "accountRootDir": self._count_files_recursive(self.dirs["rootDir"]),
            },
            "accountDocumentExists": account_exists if delete_account_document else None,
        }

    def _count_docs_by_account(self, collection_name: str) -> int:
        return len(self._get_docs_by_account(collection_name))

    def _count_statuses(self, collection_name: str) -> dict:
        docs = self._get_docs_by_account(collection_name)
        result = {}

        for doc in docs:
            data = doc.to_dict() or {}
            status = data.get("status", "<SEM_STATUS>")
            result[status] = result.get(status, 0) + 1

        return dict(sorted(result.items(), key=lambda kv: kv[0]))

    def _get_docs_by_account(self, collection_name: str):
        docs = (
            self.db.collection(collection_name)
            .where(filter=FieldFilter("accountId", "==", self.account_id))
            .stream()
        )
        return list(docs)

    def _delete_docs_in_batches(self, docs):
        for chunk in self._chunked(docs, BATCH_LIMIT):
            batch = self.db.batch()
            for doc in chunk:
                batch.delete(doc.reference)
            batch.commit()

    def _reset_sales_in_batches(self, sales_docs):
        for chunk in self._chunked(sales_docs, BATCH_LIMIT):
            batch = self.db.batch()

            for sale_doc in chunk:
                data = sale_doc.to_dict() or {}

                update_data = {
                    "accountId": self.account_id,
                    "emailAddress": data.get("emailAddress"),
                    "status": "WAITING_LABEL",
                    "updatedAt": utc_now_iso(),
                    "matchedLabelId": DELETE_FIELD,
                    "orderId": DELETE_FIELD,
                    "shippingCode": DELETE_FIELD,
                    "localEnrichedPdfPath": DELETE_FIELD,
                    "printedPdfPath": DELETE_FIELD,
                    "completedAt": DELETE_FIELD,
                    "completedAtRaw": DELETE_FIELD,
                    "completedItemAmount": DELETE_FIELD,
                    "completedShippingAmount": DELETE_FIELD,
                    "transferredToVintedBalance": DELETE_FIELD,
                    "completionSource": DELETE_FIELD,
                    "shippedAt": DELETE_FIELD,
                    "estimatedDeliveryStartRaw": DELETE_FIELD,
                    "estimatedDeliveryEndRaw": DELETE_FIELD,
                }

                batch.set(sale_doc.reference, update_data, merge=True)

            batch.commit()

    def _reset_labels_in_batches(self, label_docs):
        for chunk in self._chunked(label_docs, BATCH_LIMIT):
            batch = self.db.batch()

            for label_doc in chunk:
                data = label_doc.to_dict() or {}
                has_raw_pdf = bool(data.get("localRawPdfPath"))
                next_status = "LABEL_RECEIVED" if has_raw_pdf else "ERROR"

                update_data = {
                    "accountId": self.account_id,
                    "emailAddress": data.get("emailAddress"),
                    "status": next_status,
                    "updatedAt": utc_now_iso(),
                    "matchedSaleId": DELETE_FIELD,
                    "localEnrichedPdfPath": DELETE_FIELD,
                    "printedPdfPath": DELETE_FIELD,
                    "reviewReason": DELETE_FIELD,
                    "errorReason": DELETE_FIELD,
                }

                batch.set(label_doc.reference, update_data, merge=True)

            batch.commit()

    @staticmethod
    def _count_files(directory: Path) -> int:
        if not directory.exists():
            return 0
        return sum(1 for item in directory.iterdir() if item.is_file())

    @staticmethod
    def _count_files_recursive(directory: Path) -> int:
        if not directory.exists():
            return 0
        return sum(1 for item in directory.rglob("*") if item.is_file())

    @staticmethod
    def _clear_directory(directory: Path):
        if not directory.exists():
            return
        for item in directory.iterdir():
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)

    @staticmethod
    def _delete_directory(directory: Path):
        if directory.exists():
            shutil.rmtree(directory)

    @staticmethod
    def _chunked(items, size: int):
        for i in range(0, len(items), size):
            yield items[i:i + size]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def print_summary(summary: dict):
    print(f"Modo: {summary['mode']}")
    print(f"Conta: {summary['accountId']}")
    print(f"Executado: {summary['executed']}")

    print("\nANTES:")
    _print_block(summary["before"])

    if summary["after"] is not None:
        print("\nDEPOIS:")
        _print_block(summary["after"])


def _print_block(block: dict):
    docs = block.get("docs")
    if docs:
        print("  Documentos:")
        for key, value in docs.items():
            print(f"    {key}: {value}")

    status_breakdown = block.get("statusBreakdown")
    if status_breakdown:
        print("  Estados:")
        for col, statuses in status_breakdown.items():
            print(f"    {col}:")
            if not statuses:
                print("      (vazio)")
            else:
                for status, count in statuses.items():
                    print(f"      {status}: {count}")

    files = block.get("files")
    if files:
        print("  Ficheiros:")
        for key, value in files.items():
            print(f"    {key}: {value}")

    if "accountDocumentExists" in block and block["accountDocumentExists"] is not None:
        print(f"  accountDocumentExists: {block['accountDocumentExists']}")


def build_parser():
    parser = argparse.ArgumentParser(description="Reset de dados por accountId")
    parser.add_argument("--account", required=True, help="accountId / email da conta")

    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--derived",
        action="store_true",
        help="Apaga dados derivados e repõe sales/labels ao estado base",
    )
    mode_group.add_argument(
        "--full",
        action="store_true",
        help="Apaga todos os dados da conta e a pasta local da conta",
    )

    parser.add_argument(
        "--delete-account-doc",
        action="store_true",
        help="No modo --full, apaga também accounts/{accountId}",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Executa de facto. Sem isto faz apenas dry-run.",
    )

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    service = AccountResetService(args.account.strip().lower())
    dry_run = not args.execute

    if args.derived:
        summary = service.reset_derived_data(dry_run=dry_run)
    else:
        summary = service.reset_full_account_data(
            dry_run=dry_run,
            delete_account_document=args.delete_account_doc,
        )

    print_summary(summary)

    if dry_run:
        print("\nDRY-RUN. Nada foi apagado.")
        print("Acrescenta --execute para executar mesmo.")
    else:
        print("\nReset concluído.")

    return 0


if __name__ == "__main__":
    sys.exit(main())