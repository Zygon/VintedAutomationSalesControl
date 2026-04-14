#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import hashlib
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

try:
    import firebase_admin
    from firebase_admin import credentials, firestore  # type: ignore
except Exception:
    firebase_admin = None
    credentials = None
    firestore = None


PRICE_SPLIT_RE = re.compile(r"^\s*([0-9]+(?:[.,][0-9]{1,2})?)\s+(.+?)\s*$")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_title(title: str | None) -> str | None:
    if not title:
        return None
    title = title.lower().strip()
    title = re.sub(r"[–—-]", " ", title)
    title = re.sub(r"[^\w\s]", " ", title)
    title = re.sub(r"\s+", " ", title)
    return title


def extract_first_sku(text: str | None) -> str | None:
    if not text:
        return None
    match = re.search(r"\[(SKU\d+)\]", text, re.IGNORECASE)
    if not match:
        return None
    return match.group(1).upper()


def remove_sku_prefix(text: str | None) -> str | None:
    if not text:
        return text
    return re.sub(r"^\s*\[SKU\d+\]\s*", "", text, flags=re.IGNORECASE).strip()


def safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return int(value)
    except Exception:
        return None


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return None


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]
    return df


def read_csv_flexible(path: str, sep: Optional[str] = None) -> pd.DataFrame:
    encodings_to_try = ["utf-8-sig", "utf-8", "cp1252"]

    def _try_read(_sep: Optional[str], _enc: str) -> Optional[pd.DataFrame]:
        try:
            if _sep is None:
                df = pd.read_csv(path, sep=None, engine="python", encoding=_enc)
            else:
                df = pd.read_csv(path, sep=_sep, encoding=_enc)
            return clean_columns(df)
        except Exception:
            return None

    if sep is not None:
        for enc in encodings_to_try:
            df = _try_read(sep, enc)
            if df is not None:
                return df
        raise ValueError(f"Falha a ler CSV com sep='{sep}': {path}")

    for enc in encodings_to_try:
        df = _try_read(None, enc)
        if df is not None:
            if len(df.columns) == 1:
                df2 = _try_read(";", enc)
                if df2 is not None and len(df2.columns) > 1:
                    return df2
            return df

    raise ValueError(f"Falha a ler CSV: {path}")


def choose_dayfirst(date_values: List[Any], mode: str = "auto") -> bool:
    mode = (mode or "auto").strip().lower()
    if mode in ("dmy", "dayfirst", "pt", "eu"):
        return True
    if mode in ("mdy", "monthfirst", "us"):
        return False

    today = datetime.now().date()
    dmy_votes = 0
    mdy_votes = 0
    samples: List[str] = []

    for v in date_values:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        s = str(v).strip()
        if not s:
            continue
        if any(ch.isalpha() for ch in s):
            continue

        m = re.search(r"(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{2,4})", s)
        if not m:
            continue

        a = int(m.group(1))
        b = int(m.group(2))
        if a > 12 and b <= 12:
            dmy_votes += 1
        elif b > 12 and a <= 12:
            mdy_votes += 1

        if len(samples) < 200:
            samples.append(s)

    if dmy_votes or mdy_votes:
        return dmy_votes >= mdy_votes

    if not samples:
        return True

    dmy = pd.to_datetime(samples, dayfirst=True, errors="coerce")
    mdy = pd.to_datetime(samples, dayfirst=False, errors="coerce")

    def future_count(dt_series: pd.Series) -> int:
        try:
            dd = dt_series.dropna().dt.date
            return int((dd > today).sum())
        except Exception:
            return 10**9

    return future_count(dmy) <= future_count(mdy)


def parse_csv_date(value: Any, dayfirst: bool) -> Optional[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    raw = str(value).strip()
    if not raw:
        return None

    try:
        if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
            dt = pd.to_datetime(raw, dayfirst=False, errors="coerce")
        else:
            dt = pd.to_datetime(raw, dayfirst=dayfirst, errors="coerce")

        if pd.isna(dt):
            return None

        py_dt = dt.to_pydatetime()
        if py_dt.tzinfo is None:
            py_dt = py_dt.replace(tzinfo=timezone.utc)
        else:
            py_dt = py_dt.astimezone(timezone.utc)

        return py_dt.isoformat()
    except Exception:
        return None


def to_float_price(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None

    s = str(v).strip()
    if not s:
        return None

    s = s.replace("€", "").strip()
    s = s.replace(",", ".")
    s = re.sub(r"[^\d.]", "", s)

    if not s:
        return None

    try:
        return float(s)
    except Exception:
        return None


def allocate_values_evenly(total_value: Optional[float], n: int) -> List[Optional[float]]:
    if total_value is None:
        return [None] * n
    if n <= 0:
        raise ValueError("n tem de ser >= 1")

    total_cents = int(round(float(total_value) * 100))
    base = total_cents // n
    remainder = total_cents % n

    out: List[Optional[float]] = []
    for i in range(n):
        cents = base + (1 if i < remainder else 0)
        out.append(cents / 100.0)
    return out


def split_products(product_raw: Any) -> List[str]:
    if product_raw is None or (isinstance(product_raw, float) and pd.isna(product_raw)):
        return []
    s = str(product_raw).strip()
    if not s:
        return []
    return [p.strip() for p in s.split(",") if str(p).strip()]


def extract_price_product_from_price_field(price_field: Any) -> Tuple[Optional[str], Optional[str]]:
    if price_field is None or (isinstance(price_field, float) and pd.isna(price_field)):
        return None, None
    s = str(price_field).strip()
    m = PRICE_SPLIT_RE.match(s)
    if not m:
        return None, None
    return m.group(1), m.group(2).strip()


def build_sale_id(account_id: str, source_file: str, row_index: int, source_id: Any) -> str:
    raw = f"{account_id}:{source_file}:{row_index}:{source_id or ''}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def normalize_order_status(raw_status: Any) -> Optional[str]:
    if raw_status is None or (isinstance(raw_status, float) and pd.isna(raw_status)):
        return "COMPLETED"

    s = str(raw_status).strip().lower()
    if not s:
        return "COMPLETED"

    if s == "canceled":
        return None

    return "COMPLETED"


def normalize_csv_rows(df: pd.DataFrame, source_file: str, date_order: str = "dmy") -> pd.DataFrame:
    df = clean_columns(df)
    cols = set(df.columns)

    if "Product" not in cols:
        raise ValueError(f"CSV sem coluna 'Product'. Colunas encontradas: {list(df.columns)}")

    price_col = "Price" if "Price" in cols else None
    date_col = "Date" if "Date" in cols else None
    courier_col = "Courier" if "Courier" in cols else None
    status_col = "Order Status" if "Order Status" in cols else None
    id_col = "ID" if "ID" in cols else ("Reg no" if "Reg no" in cols else None)

    dayfirst = True
    if date_col:
        try:
            dayfirst = choose_dayfirst(df[date_col].tolist(), mode=date_order)
        except Exception:
            dayfirst = True

    out_rows: List[Dict[str, Any]] = []
    skipped_canceled = 0

    for idx, row in df.iterrows():
        product_raw = row.get("Product")
        price_val = row.get(price_col) if price_col else None

        if (product_raw is None or (isinstance(product_raw, float) and pd.isna(product_raw))) and price_val is not None:
            p_str, prod_str = extract_price_product_from_price_field(price_val)
            if p_str and prod_str:
                price_val = p_str
                product_raw = prod_str

        raw_status = (
            str(row.get(status_col)).strip()
            if status_col and row.get(status_col) is not None and not pd.isna(row.get(status_col))
            else None
        )

        normalized_status = normalize_order_status(raw_status)
        if normalized_status is None:
            skipped_canceled += 1
            continue

        products = split_products(product_raw)
        if not products:
            continue

        source_id = row.get(id_col) if id_col else None
        sold_at = parse_csv_date(row.get(date_col) if date_col else None, dayfirst=dayfirst)
        courier = (
            str(row.get(courier_col)).strip()
            if courier_col and row.get(courier_col) is not None and not pd.isna(row.get(courier_col))
            else None
        )

        total_value = to_float_price(price_val)
        allocations = allocate_values_evenly(total_value, len(products))
        is_bundle_sale = len(products) > 1
        bundle_item_count = len(products) if is_bundle_sale else None

        out_rows.append(
            {
                "source_file": source_file,
                "source_row_index": int(idx) + 1,
                "source_id": source_id,
                "soldAt": sold_at,
                "status": normalized_status,
                "rawOrderStatus": raw_status,
                "courier": courier,
                "products": products,
                "totalValue": total_value,
                "allocations": allocations,
                "isBundleSale": is_bundle_sale,
                "bundleItemCount": bundle_item_count,
            }
        )

    out = pd.DataFrame(out_rows)
    out.attrs["skipped_canceled"] = skipped_canceled
    return out


def init_firestore(service_account_path: Optional[str]):
    if firebase_admin is None or credentials is None or firestore is None:
        raise RuntimeError("firebase-admin não está disponível.")

    if not firebase_admin._apps:
        if service_account_path:
            cred = credentials.Certificate(service_account_path)
            firebase_admin.initialize_app(cred)
        else:
            firebase_admin.initialize_app()

    return firestore.client()


def purge_existing_csv_import_docs(db, account_id: str, source_files: List[str], batch_size: int = 400):
    for collection in ["sales", "saleItems"]:
        refs = []
        seen = set()

        for sf in source_files:
            for doc in db.collection(collection).where("source.file", "==", sf).stream():
                data = doc.to_dict() or {}
                if data.get("accountId") != account_id:
                    continue

                source = data.get("source") or {}
                if source.get("type") != "csv_import":
                    continue
                if source.get("file") != sf:
                    continue

                if doc.id in seen:
                    continue
                seen.add(doc.id)
                refs.append(doc.reference)

        print(f"[PURGE] {collection}: encontrados {len(refs)} docs para apagar.")

        batch = db.batch()
        ops = 0
        deleted = 0

        for ref in refs:
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


def import_rows_to_firestore(
    db,
    account_id: str,
    email_address: str,
    rows_df: pd.DataFrame,
    execute: bool,
    batch_size: int = 400,
):
    sales_count = 0
    items_count = 0

    sales_payloads: List[Tuple[str, Dict[str, Any]]] = []
    item_payloads: List[Tuple[str, Dict[str, Any]]] = []

    for _, row in rows_df.iterrows():
        source_file = row["source_file"]
        source_row_index = safe_int(row.get("source_row_index"))
        if source_row_index is None:
            raise ValueError(f"source_row_index inválido: {row.to_dict()}")

        source_id = row.get("source_id")
        sold_at = row.get("soldAt")
        status = row.get("status")
        products = row.get("products") or []
        allocations = row.get("allocations") or []
        total_value = safe_float(row.get("totalValue"))
        is_bundle_sale = bool(row.get("isBundleSale"))
        bundle_item_count = row.get("bundleItemCount")

        sale_id = build_sale_id(account_id, source_file, source_row_index, source_id)

        first_product = products[0] if products else None
        first_product_clean = remove_sku_prefix(first_product)
        first_product_norm = normalize_title(first_product_clean)
        first_sku = None if is_bundle_sale else extract_first_sku(first_product)

        sale_payload = {
            "saleId": sale_id,
            "accountId": account_id,
            "emailAddress": email_address,
            "saleEmailId": None,
            "subject": None,
            "buyerUsername": None,
            "articleTitle": first_product_clean,
            "articleTitleNormalized": first_product_norm,
            "sku": first_sku,
            "value": total_value,
            "currency": "EUR",
            "soldAt": sold_at,
            "status": status,
            "isBundleSale": bool(is_bundle_sale),
            "bundleItemCount": safe_int(bundle_item_count),
            "rawBodyPreview": None,
            "courier": row.get("courier"),
            "source": {
                "type": "csv_import",
                "file": source_file,
                "rowIndex": source_row_index,
                "sourceId": source_id,
                "rawOrderStatus": row.get("rawOrderStatus"),
            },
            "importedAt": firestore.SERVER_TIMESTAMP if firestore is not None else None,
        }

        sales_payloads.append((sale_id, sale_payload))
        sales_count += 1

        for idx, product in enumerate(products):
            product_clean = remove_sku_prefix(product)
            product_norm = normalize_title(product_clean)
            product_sku = extract_first_sku(product)
            allocated_value = safe_float(allocations[idx] if idx < len(allocations) else None)

            sale_item_id = f"{sale_id}__{idx + 1}"

            item_payload = {
                "saleItemId": sale_item_id,
                "accountId": account_id,
                "emailAddress": email_address,
                "saleId": sale_id,
                "orderId": None,
                "shippingCode": None,
                "buyerUsername": None,
                "isBundle": is_bundle_sale,
                "bundleIndex": idx + 1,
                "articleTitle": product_clean,
                "articleTitleNormalized": product_norm,
                "sku": product_sku,
                "allocatedValue": allocated_value,
                "currency": "EUR",
                "createdAt": sold_at or utc_now_iso(),
                "courier": row.get("courier"),
                "source": {
                    "type": "csv_import",
                    "file": source_file,
                    "rowIndex": source_row_index,
                    "sourceId": source_id,
                    "rawOrderStatus": row.get("rawOrderStatus"),
                },
                "importedAt": firestore.SERVER_TIMESTAMP if firestore is not None else None,
            }

            item_payloads.append((sale_item_id, item_payload))
            items_count += 1

    print(f"[IMPORT] Preparados {sales_count} sales e {items_count} saleItems")

    if sales_payloads:
        sid, sp = sales_payloads[0]
        print(
            f"[SALE SAMPLE] {sid} -> articleTitle={sp.get('articleTitle')} | "
            f"value={sp.get('value')} | status={sp.get('status')}"
        )

    if item_payloads:
        iid, ip = item_payloads[0]
        print(
            f"[ITEM SAMPLE] {iid} -> articleTitle={ip.get('articleTitle')} | "
            f"allocatedValue={ip.get('allocatedValue')}"
        )

    if not execute:
        print("[DRY-RUN] Não foi escrito nada no Firestore.")
        return

    def _commit(batch, ops):
        if ops:
            batch.commit()
        return db.batch(), 0

    batch = db.batch()
    ops = 0

    for doc_id, payload in sales_payloads:
        ref = db.collection("sales").document(doc_id)
        batch.set(ref, payload, merge=True)
        ops += 1
        if ops >= batch_size:
            batch, ops = _commit(batch, ops)

    for doc_id, payload in item_payloads:
        ref = db.collection("saleItems").document(doc_id)
        batch.set(ref, payload, merge=True)
        ops += 1
        if ops >= batch_size:
            batch, ops = _commit(batch, ops)

    if ops:
        batch.commit()

    print("[OK] Import concluído.")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Importar Order History CSV para sales + saleItems com schema operacional"
    )
    p.add_argument("--csv", action="append", required=True, help="Caminho para CSV (podes repetir)")
    p.add_argument("--sep", default=None, help="Separador CSV, ex: ';'")
    p.add_argument("--account", required=True, help="AccountId/email")
    p.add_argument("--service-account", help="JSON da service account")
    p.add_argument("--execute", action="store_true", help="Executa escrita no Firestore")
    p.add_argument("--date-order", default="dmy", choices=["auto", "dmy", "mdy"])
    p.add_argument("--purge-before-import", action="store_true")
    return p


def main() -> int:
    args = build_parser().parse_args()

    account_id = args.account.strip().lower()
    email_address = account_id

    all_rows: List[pd.DataFrame] = []
    total_skipped_canceled = 0
    source_files: List[str] = []

    for csv_path in args.csv:
        source_file = os.path.basename(csv_path)
        source_files.append(source_file)

        df_raw = read_csv_flexible(csv_path, sep=args.sep)
        rows = normalize_csv_rows(df_raw, source_file=source_file, date_order=args.date_order)

        skipped = int(rows.attrs.get("skipped_canceled", 0))
        total_skipped_canceled += skipped

        print(f"[FILE] {source_file}: {len(rows)} vendas importáveis | canceled ignoradas={skipped}")
        all_rows.append(rows)

    if not all_rows:
        print("[ERRO] Nenhum CSV processado.")
        return 1

    merged = pd.concat(all_rows, ignore_index=True)
    print(f"[OK] Total vendas importáveis: {len(merged)}")
    print(f"[OK] Total canceled ignoradas: {total_skipped_canceled}")

    db = None
    if args.execute or args.purge_before_import:
        db = init_firestore(args.service_account)

    if args.purge_before_import:
        if db is None:
            raise RuntimeError("Firestore não inicializado para purge.")
        purge_existing_csv_import_docs(db, account_id, sorted(set(source_files)))

    if db is None:
        class DummyDB:
            pass
        db = DummyDB()

    import_rows_to_firestore(
        db=db,
        account_id=account_id,
        email_address=email_address,
        rows_df=merged,
        execute=bool(args.execute),
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())