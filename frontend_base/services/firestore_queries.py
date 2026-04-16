from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:
    psycopg = None
    dict_row = None

import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1.base_query import FieldFilter


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DOTENV_PATH = PROJECT_ROOT / ".env"

if load_dotenv and DOTENV_PATH.exists():
    load_dotenv(dotenv_path=DOTENV_PATH)


DB_PROVIDER = os.getenv("DB_PROVIDER", "firebase").strip().lower()

FIREBASE_SERVICE_ACCOUNT_FILE = os.getenv("FIREBASE_SERVICE_ACCOUNT_FILE")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "").strip()
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "").strip()
POSTGRES_USER = os.getenv("POSTGRES_USER", "").strip()
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "").strip()
POSTGRES_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require").strip()


@dataclass
class DateRange:
    start: datetime
    end: datetime


COLLECTION_DATE_FIELDS: dict[str, list[str]] = {
    "sales": ["soldAt", "completedAt", "shippedAt", "updatedAt", "createdAt"],
    "labels": ["createdAt", "updatedAt"],
    "saleItems": ["completedAt", "createdAt", "updatedAt"],
    "expenses": ["receivedAt", "issuedAt", "createdAt", "updatedAt"],
    "payouts": ["paidOutAt", "createdAt", "updatedAt"],
    "printJobs": ["printedAt", "createdAt", "updatedAt"],
    "events": ["createdAt"],
    "accounts": ["lastRunAt", "updatedAt", "createdAt"],
    "users": ["lastLoginAt", "updatedAt", "createdAt"],
    "userAccounts": ["updatedAt", "createdAt"],
    "products": ["updatedAt", "createdAt"],
    "skuCounters": ["updatedAt", "createdAt"],
    "generatedSkus": ["createdAt", "updatedAt"],
}


def _ensure_timezone(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _resolve_credential_path() -> str:
    credential_path = (
        FIREBASE_SERVICE_ACCOUNT_FILE
        or GOOGLE_APPLICATION_CREDENTIALS
    )

    if credential_path:
        candidate = Path(credential_path)
        if candidate.exists():
            return str(candidate.resolve())

    try:
        from app.config import FIREBASE_SERVICE_ACCOUNT_FILE as BACKEND_FIREBASE_SERVICE_ACCOUNT_FILE  # type: ignore

        backend_candidate = Path(BACKEND_FIREBASE_SERVICE_ACCOUNT_FILE)
        if backend_candidate.exists():
            return str(backend_candidate.resolve())
    except Exception:
        pass

    fallback = PROJECT_ROOT / "credentials" / "firebase-service-account.json"
    if fallback.exists():
        return str(fallback.resolve())

    raise RuntimeError(
        "Define FIREBASE_SERVICE_ACCOUNT_FILE ou GOOGLE_APPLICATION_CREDENTIALS, "
        "ou coloca o ficheiro em credentials/firebase-service-account.json."
    )


def parse_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None

    if isinstance(value, datetime):
        return _ensure_timezone(value)

    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=timezone.utc)

    if hasattr(value, "to_datetime"):
        try:
            return _ensure_timezone(value.to_datetime())
        except Exception:
            return None

    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return _ensure_timezone(value.to_pydatetime())

    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None

        try:
            return _ensure_timezone(datetime.fromisoformat(raw.replace("Z", "+00:00")))
        except ValueError:
            pass

        known_formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y",
        ]
        for fmt in known_formats:
            try:
                return _ensure_timezone(datetime.strptime(raw, fmt))
            except ValueError:
                continue

    return None


def _pick_primary_date(row: dict[str, Any], collection_name: str) -> datetime | None:
    for field in COLLECTION_DATE_FIELDS.get(collection_name, ["createdAt", "updatedAt"]):
        parsed = parse_datetime(row.get(field))
        if parsed is not None:
            return parsed
    return None


def _coerce_date_range(date_range: dict[str, Any] | DateRange | None) -> DateRange | None:
    if date_range is None:
        return None

    if isinstance(date_range, DateRange):
        return date_range

    if isinstance(date_range, dict):
        start = parse_datetime(date_range.get("start"))
        end = parse_datetime(date_range.get("end"))
        if start and end:
            return DateRange(start=start, end=end)

    return None


def compute_date_range(
    period_mode: str,
    custom_start: date | None = None,
    custom_end: date | None = None,
) -> dict[str, datetime]:
    now = datetime.now(timezone.utc)
    today = now.date()

    if period_mode == "Dia":
        start_date = today
        end_date = today
    elif period_mode == "Semana":
        start_date = today - timedelta(days=today.weekday())
        end_date = today
    elif period_mode == "Mês":
        start_date = today.replace(day=1)
        end_date = today
    else:
        start_date = custom_start or today
        end_date = custom_end or today

    return {
        "start": datetime.combine(start_date, time.min, tzinfo=timezone.utc),
        "end": datetime.combine(end_date, time.max, tzinfo=timezone.utc),
    }


@st.cache_resource(show_spinner=False)
def get_db():
    if DB_PROVIDER == "postgres":
        return _get_postgres_connection()
    return _get_firebase_db()


def _get_firebase_db():
    credential_path = _resolve_credential_path()

    if not firebase_admin._apps:
        cred = credentials.Certificate(credential_path)
        firebase_admin.initialize_app(cred)

    return firestore.client()


def _postgres_dsn() -> str:
    missing = [
        name
        for name, value in {
            "POSTGRES_HOST": POSTGRES_HOST,
            "POSTGRES_DB": POSTGRES_DB,
            "POSTGRES_USER": POSTGRES_USER,
            "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
        }.items()
        if not value or value == "..."
    ]
    if missing:
        raise RuntimeError(
            f"Config PostgreSQL incompleta. Variáveis em falta: {', '.join(missing)}"
        )

    return (
        f"host={POSTGRES_HOST} "
        f"port={POSTGRES_PORT} "
        f"dbname={POSTGRES_DB} "
        f"user={POSTGRES_USER} "
        f"password={POSTGRES_PASSWORD} "
        f"sslmode={POSTGRES_SSLMODE}"
    )


def _get_postgres_connection():
    if psycopg is None or dict_row is None:
        raise RuntimeError(
            "psycopg não está instalado. Adiciona psycopg[binary] ao requirements."
        )
    return psycopg.connect(_postgres_dsn(), row_factory=dict_row)


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _normalize_firestore_doc(doc) -> dict[str, Any]:
    data = doc.to_dict() or {}
    data.setdefault("id", doc.id)
    return data


def _rows_from_firestore(
    collection_name: str,
    account_id: str | None = None,
    allowed_account_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    db = get_db()

    if account_id:
        docs = (
            db.collection(collection_name)
            .where(filter=FieldFilter("accountId", "==", account_id))
            .stream()
        )
        return [_normalize_firestore_doc(doc) for doc in docs]

    if allowed_account_ids:
        rows: list[dict[str, Any]] = []
        for allowed_account_id in allowed_account_ids:
            docs = (
                db.collection(collection_name)
                .where(filter=FieldFilter("accountId", "==", allowed_account_id))
                .stream()
            )
            rows.extend(_normalize_firestore_doc(doc) for doc in docs)
        return rows

    docs = db.collection(collection_name).stream()
    return [_normalize_firestore_doc(doc) for doc in docs]


def _rows_from_postgres(
    table_name: str,
    account_id: str | None = None,
    allowed_account_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    conn = get_db()
    sql = f"SELECT * FROM {_quote_ident(table_name)}"
    params: list[Any] = []

    if account_id:
        sql += ' WHERE "accountId" = %s'
        params.append(account_id)
    elif allowed_account_ids:
        sql += ' WHERE "accountId" = ANY(%s)'
        params.append(allowed_account_ids)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    normalized: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)

        if "id" not in item:
            if table_name == "saleItems" and "saleItemId" in item:
                item["id"] = item["saleItemId"]
            elif table_name == "printJobs" and "printJobId" in item:
                item["id"] = item["printJobId"]
            elif table_name == "labels" and "labelId" in item:
                item["id"] = item["labelId"]
            elif table_name == "sales" and "saleId" in item:
                item["id"] = item["saleId"]
            elif table_name == "expenses" and "expenseId" in item:
                item["id"] = item["expenseId"]
            elif table_name == "payouts" and "payoutId" in item:
                item["id"] = item["payoutId"]
            elif table_name == "products" and "productId" in item:
                item["id"] = item["productId"]
            elif table_name == "generatedSkus" and "generatedSkuId" in item:
                item["id"] = item["generatedSkuId"]
            elif table_name == "accounts" and "accountId" in item:
                item["id"] = item["accountId"]

        normalized.append(item)

    return normalized


def _rows_from_provider(
    collection_name: str,
    account_id: str | None = None,
    allowed_account_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    if DB_PROVIDER == "postgres":
        return _rows_from_postgres(
            collection_name,
            account_id=account_id,
            allowed_account_ids=allowed_account_ids,
        )
    return _rows_from_firestore(
        collection_name,
        account_id=account_id,
        allowed_account_ids=allowed_account_ids,
    )


def _filter_rows_by_date_range(
    rows: list[dict[str, Any]],
    collection_name: str,
    date_field: str | None = None,
    date_range: dict[str, Any] | DateRange | None = None,
) -> list[dict[str, Any]]:
    resolved_range = _coerce_date_range(date_range)
    if resolved_range is None:
        return rows

    filtered: list[dict[str, Any]] = []
    for row in rows:
        row_dt = parse_datetime(row.get(date_field)) if date_field else _pick_primary_date(row, collection_name)
        if row_dt is None:
            continue
        if resolved_range.start <= row_dt <= resolved_range.end:
            filtered.append(row)

    return filtered


def _sort_rows(
    rows: list[dict[str, Any]],
    collection_name: str,
    date_field: str | None = None,
    sort_desc: bool = True,
) -> list[dict[str, Any]]:
    def sort_key(row: dict[str, Any]):
        if date_field:
            parsed = parse_datetime(row.get(date_field))
            if parsed is not None:
                return parsed
        parsed = _pick_primary_date(row, collection_name)
        if parsed is not None:
            return parsed
        return datetime(1970, 1, 1, tzinfo=timezone.utc)

    return sorted(rows, key=sort_key, reverse=sort_desc)


def _rows_to_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    for column in [
        "soldAt",
        "receivedAt",
        "completedAt",
        "shippedAt",
        "updatedAt",
        "createdAt",
        "issuedAt",
        "paidOutAt",
        "lastLoginAt",
        "lastRunAt",
    ]:
        if column in df.columns:
            df[f"{column}Parsed"] = pd.to_datetime(df[column], errors="coerce", utc=True)

    return df


@st.cache_data(show_spinner=False, ttl=60)
def list_active_accounts() -> list[dict[str, str]]:
    rows = _rows_from_provider("accounts")

    accounts: list[dict[str, str]] = []
    for row in rows:
        if str(row.get("status", "")).upper() != "ACTIVE":
            continue

        account_id = str(row.get("accountId") or row.get("id") or "")
        if not account_id:
            continue

        email = str(row.get("emailAddress") or account_id)
        accounts.append({
            "accountId": account_id,
            "emailAddress": email,
        })

    accounts.sort(key=lambda x: x["emailAddress"])
    return accounts


@st.cache_data(show_spinner=False, ttl=60)
def list_user_account_ids(user_id: str) -> list[str]:
    if not user_id:
        return []

    rows = _rows_from_provider("userAccounts")
    result: list[str] = []

    for row in rows:
        if str(row.get("userId")) != str(user_id):
            continue
        if str(row.get("status", "ACTIVE")).upper() != "ACTIVE":
            continue

        account_id = row.get("accountId")
        if account_id:
            result.append(str(account_id))

    seen = set()
    ordered: list[str] = []
    for item in result:
        if item not in seen:
            seen.add(item)
            ordered.append(item)

    return ordered


@st.cache_data(show_spinner=False, ttl=60)
def load_collection_df(
    collection_name: str,
    account_id: str | None = None,
    allowed_account_ids: list[str] | None = None,
    date_field: str | None = None,
    date_range: dict[str, Any] | DateRange | None = None,
) -> pd.DataFrame:
    rows = _rows_from_provider(
        collection_name,
        account_id=account_id,
        allowed_account_ids=allowed_account_ids,
    )
    rows = _filter_rows_by_date_range(
        rows,
        collection_name=collection_name,
        date_field=date_field,
        date_range=date_range,
    )
    rows = _sort_rows(
        rows,
        collection_name=collection_name,
        date_field=date_field,
        sort_desc=True,
    )
    return _rows_to_df(rows)


@st.cache_data(show_spinner=False, ttl=60)
def load_sale_items_for_sale(
    sale_id: str,
    account_id: str | None = None,
    allowed_account_ids: list[str] | None = None,
) -> pd.DataFrame:
    if not sale_id:
        return pd.DataFrame()

    if DB_PROVIDER == "postgres":
        rows = _rows_from_postgres(
            "saleItems",
            account_id=account_id,
            allowed_account_ids=allowed_account_ids,
        )
        rows = [row for row in rows if str(row.get("saleId")) == str(sale_id)]
    else:
        db = get_db()
        query = db.collection("saleItems").where(
            filter=FieldFilter("saleId", "==", sale_id)
        )

        rows: list[dict[str, Any]] = []
        for doc in query.stream():
            row = doc.to_dict() or {}
            row_account_id = row.get("accountId")

            if account_id and row_account_id != account_id:
                continue

            if not account_id and allowed_account_ids and row_account_id not in allowed_account_ids:
                continue

            row.setdefault("id", doc.id)
            rows.append(row)

    rows = _sort_rows(rows, "saleItems", date_field="createdAt", sort_desc=False)
    return _rows_to_df(rows)


@st.cache_data(show_spinner=False, ttl=60)
def load_label_by_id(
    label_id: str,
    account_id: str | None = None,
    allowed_account_ids: list[str] | None = None,
) -> dict[str, Any] | None:
    if not label_id:
        return None

    if DB_PROVIDER == "postgres":
        rows = _rows_from_postgres(
            "labels",
            account_id=account_id,
            allowed_account_ids=allowed_account_ids,
        )
        for row in rows:
            if str(row.get("labelId")) == str(label_id) or str(row.get("id")) == str(label_id):
                return row
        return None

    db = get_db()
    doc = db.collection("labels").document(label_id).get()
    if not doc.exists:
        return None

    data = doc.to_dict() or {}
    row_account_id = data.get("accountId")

    if account_id and row_account_id != account_id:
        return None

    if not account_id and allowed_account_ids and row_account_id not in allowed_account_ids:
        return None

    data.setdefault("id", doc.id)
    return data


@st.cache_data(show_spinner=False, ttl=60)
def load_labels_for_sale(
    sale_id: str,
    account_id: str | None = None,
    allowed_account_ids: list[str] | None = None,
) -> pd.DataFrame:
    if not sale_id:
        return pd.DataFrame()

    rows = _rows_from_provider(
        "labels",
        account_id=account_id,
        allowed_account_ids=allowed_account_ids,
    )

    result = [row for row in rows if str(row.get("matchedSaleId")) == str(sale_id)]
    if not result:
        result = [row for row in rows if str(row.get("saleId")) == str(sale_id)]

    result = _sort_rows(result, "labels", date_field="createdAt", sort_desc=True)
    return _rows_to_df(result)


@st.cache_data(show_spinner=False, ttl=60)
def load_products_df(
    account_id: str | None = None,
    allowed_account_ids: list[str] | None = None,
) -> pd.DataFrame:
    return load_collection_df(
        "products",
        account_id=account_id,
        allowed_account_ids=allowed_account_ids,
    )


@st.cache_data(show_spinner=False, ttl=60)
def load_generated_skus_df(
    account_id: str | None = None,
    allowed_account_ids: list[str] | None = None,
) -> pd.DataFrame:
    return load_collection_df(
        "generatedSkus",
        account_id=account_id,
        allowed_account_ids=allowed_account_ids,
        date_field="createdAt",
    )


def update_sale_status(sale_id: str, new_status: str) -> None:
    if not sale_id or not new_status:
        return

    now_iso = datetime.now(timezone.utc).isoformat()

    if DB_PROVIDER == "postgres":
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE "sales"
                SET "status" = %s,
                    "updatedAt" = %s
                WHERE "saleId" = %s
                """,
                (new_status, now_iso, str(sale_id)),
            )
        conn.commit()
    else:
        db = get_db()
        db.collection("sales").document(str(sale_id)).set(
            {
                "status": str(new_status).strip(),
                "updatedAt": now_iso,
            },
            merge=True,
        )

    clear_all_caches()


def clear_all_caches() -> None:
    try:
        st.cache_data.clear()
    except Exception:
        pass

    try:
        st.cache_resource.clear()
    except Exception:
        pass