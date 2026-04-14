#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Purge de documentos importados via CSV (source.type == "csv_import") para um ou mais ficheiros.

Objetivo:
- Apagar docs errados (ex: datas trocadas) antes de reimportar.
- Atua em 2 coleções: sales e saleItems (configuráveis).

Segurança:
- Por defeito é DRY-RUN (não apaga).
- Só apaga documentos que:
  - tenham accountId == --account
  - tenham source.type == "csv_import"
  - tenham source.file igual a um dos --file

Uso:
  python purge_csv_import.py --account EMAIL --file "Order History Fev ....csv" --file "🧾Order History ....csv"
  python purge_csv_import.py --account EMAIL --file ... --service-account "C:\path\sa.json" --execute
"""

from __future__ import annotations

import argparse
from typing import List, Optional, Dict, Any, Iterable, Tuple

try:
    import firebase_admin
    from firebase_admin import credentials, firestore
except Exception:  # pragma: no cover
    firebase_admin = None
    credentials = None
    firestore = None


def init_firestore(service_account_path: Optional[str]):
    if firebase_admin is None or credentials is None or firestore is None:
        raise RuntimeError("firebase-admin não está disponível. Ativa a venv e instala requirements.txt")

    if not firebase_admin._apps:
        if service_account_path:
            cred = credentials.Certificate(service_account_path)
            firebase_admin.initialize_app(cred)
        else:
            # usa ADC / GOOGLE_APPLICATION_CREDENTIALS
            firebase_admin.initialize_app()

    return firestore.client()


def iter_docs_by_file(db, collection: str, source_file: str):
    """
    Itera docs filtrando pelo campo source.file == source_file.
    (Só 1 filtro para evitar índices compostos.)
    """
    return db.collection(collection).where("source.file", "==", source_file).stream()


def purge_collection(
    db,
    collection: str,
    account_id: str,
    source_files: List[str],
    execute: bool,
    batch_size: int = 400,
) -> int:
    to_delete: List[Any] = []

    for sf in source_files:
        for doc in iter_docs_by_file(db, collection, sf):
            data = doc.to_dict() or {}
            if data.get("accountId") != account_id:
                continue
            src = data.get("source") or {}
            if src.get("type") != "csv_import":
                continue
            # match file again defensively
            if src.get("file") != sf:
                continue
            to_delete.append(doc.reference)

    print(f"[PURGE] {collection}: encontrados {len(to_delete)} docs para apagar (accountId={account_id})")

    if not execute:
        print("[DRY-RUN] Não foi apagado nada. Usa --execute para apagar.")
        return len(to_delete)

    batch = db.batch()
    ops = 0
    deleted = 0

    for ref in to_delete:
        batch.delete(ref)
        ops += 1
        deleted += 1
        if ops >= batch_size:
            batch.commit()
            batch = db.batch()
            ops = 0

    if ops:
        batch.commit()

    print(f"[OK] {collection}: apagados {deleted} docs.")
    return deleted


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Apagar docs importados via CSV (source.type=csv_import) por ficheiro")
    p.add_argument("--account", required=True, help="accountId/email usado no import (campo accountId)")
    p.add_argument("--file", action="append", required=True, help="Nome exato do ficheiro (source.file). Pode repetir.")
    p.add_argument("--service-account", help="Caminho para serviceAccount JSON (senão usa ADC/GOOGLE_APPLICATION_CREDENTIALS)")
    p.add_argument("--sales-collection", default="sales", help="Coleção de vendas (default: sales)")
    p.add_argument("--items-collection", default="saleItems", help="Coleção de itens (default: saleItems)")
    p.add_argument("--only-items", action="store_true", help="Apaga apenas saleItems")
    p.add_argument("--only-sales", action="store_true", help="Apaga apenas sales")
    p.add_argument("--execute", action="store_true", help="Executa delete (senão é dry-run)")
    return p


def main() -> int:
    args = build_parser().parse_args()

    db = init_firestore(args.service_account)

    do_sales = not args.only_items
    do_items = not args.only_sales

    if do_sales:
        purge_collection(db, args.sales_collection, args.account, args.file, args.execute)
    if do_items:
        purge_collection(db, args.items_collection, args.account, args.file, args.execute)

    print("[DONE]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
