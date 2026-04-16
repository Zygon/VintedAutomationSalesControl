import argparse
import json
import sys
from pathlib import Path
from datetime import datetime, date
from decimal import Decimal

from google.cloud import firestore
from google.cloud.firestore_v1.document import DocumentReference

from app.firestore_client import FirebaseClient


class FirestoreExporter:
    def __init__(self):
        self.firebase = FirebaseClient()
        self.db = self.firebase.db

    def export_all(self, output_path: str):
        result = {}

        for collection in self.db.collections():
            name = collection.id
            print(f"[EXPORT] {name}")

            result[name] = self._export_collection(collection)

        self._write_file(output_path, result)
        return result

    def export_account(self, account_id: str, output_path: str):
        result = {}

        for collection in self.db.collections():
            name = collection.id
            print(f"[EXPORT][{account_id}] {name}")

            docs = (
                self.db.collection(name)
                .where("accountId", "==", account_id)
                .stream()
            )

            result[name] = [
                {
                    "id": doc.id,
                    "path": doc.reference.path,
                    "data": self._serialize(doc.to_dict() or {}),
                }
                for doc in docs
            ]

        self._write_file(output_path, result)
        return result

    def _export_collection(self, collection_ref):
        return [
            {
                "id": doc.id,
                "path": doc.reference.path,
                "data": self._serialize(doc.to_dict() or {}),
            }
            for doc in collection_ref.stream()
        ]

    def _serialize(self, value):
        if value is None:
            return None

        if isinstance(value, dict):
            return {k: self._serialize(v) for k, v in value.items()}

        if isinstance(value, list):
            return [self._serialize(v) for v in value]

        if isinstance(value, (datetime, date)):
            return value.isoformat()

        if isinstance(value, Decimal):
            return float(value)

        if isinstance(value, DocumentReference):
            return value.path

        try:
            json.dumps(value)
            return value
        except TypeError:
            return str(value)

    def _write_file(self, output_path: str, data):
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"[OK] Exported -> {path}")


# =========================
# CLI
# =========================

def build_parser():
    parser = argparse.ArgumentParser(description="Export Firestore data")

    parser.add_argument(
        "--output",
        required=True,
        help="Ficheiro de saída (ex: exports/firestore.json)",
    )

    parser.add_argument(
        "--account",
        help="Se definido, exporta apenas dados dessa conta",
    )

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    exporter = FirestoreExporter()

    if args.account:
        exporter.export_account(args.account.strip().lower(), args.output)
    else:
        exporter.export_all(args.output)

    return 0


if __name__ == "__main__":
    sys.exit(main())