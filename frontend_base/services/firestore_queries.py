from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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

try:
    import firebase_admin
    from firebase_admin import credentials, firestore
except ImportError:
    firebase_admin = None
    credentials = None
    firestore = None


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
    "sales": ["soldAt", "completedAt", "updatedAt", "createdAt"],
    "labels": ["createdAt", "updatedAt"],
    "saleItems": ["completedAt", "createdAt", "updatedAt"],
    "expenses": ["issuedAt", "createdAt", "updatedAt"],
    "payouts": ["paidOutAt", "createdAt", "updatedAt"],
    "printJobs": ["createdAt", "printedAt", "updatedAt"],
    "events": ["createdAt"],
    "accounts": ["createdAt", "updatedAt", "lastRunAt"],
    "users": ["createdAt", "updatedAt", "lastLoginAt"],
    "userAccounts": ["createdAt", "updatedAt"],
    "products": ["createdAt", "updatedAt"],
    "skuCounters": ["createdAt", "updatedAt"],
    "generatedSkus": ["createdAt", "updatedAt"],
}


def _ensure_timezone(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def parse_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None

    if isinstance(value, datetime):
        return _ensure_timezone(value)

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
        text = value.strip()
        if not text:
            return None

        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        try:
            return _ensure_timezone(datetime.fromisoformat(text))
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
                return _ensure_timezone(datetime.strptime(text, fmt))
            except ValueError:
                continue

    return None


def build_date_range(period_key: str) -> DateRange:
    now = datetime.now(timezone.utc)

    if period_key == "day":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif period_key == "week":
        start = (now - timedelta(days=now.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    elif period_key == "month":
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        start = datetime(2000, 1, 1, tzinfo=timezone.utc)

    return DateRange(start=start, end=now)


def _get_collection_date_fields(collection_name: str) -> list[str]:
    return COLLECTION_DATE_FIELDS.get(
        collection_name,
        ["createdAt", "updatedAt"],
    )


def _pick_primary_date(row: dict[str, Any], collection_name: str) -> datetime | None:
    for field in _get_collection_date_fields(collection_name):
        parsed = parse_datetime(row.get(field))
        if parsed is not None:
            return parsed
    return None


@st.cache_resource(show_spinner=False)
def get_db():
    if DB_PROVIDER == "firebase":
        return _get_firebase_db()
    if DB_PROVIDER == "postgres":
        return _get_postgres_connection()
    raise RuntimeError(f"DB_PROVIDER inválido: {DB_PROVIDER}")


def _get_firebase_credentials_path() -> str | None:
    if FIREBASE_SERVICE_ACCOUNT_FILE:
        return FIREBASE_SERVICE_ACCOUNT_FILE
    if GOOGLE_APPLICATION_CREDENTIALS:
        return GOOGLE_APPLICATION_CREDENTIALS
    return None


def _get_firebase_db():
    if firebase_admin is None or credentials is None or firestore is None:
        raise RuntimeError(
            "firebase_admin não está instalado. Adiciona firebase-admin ao requirements."
        )

    cred_path = _get_firebase_credentials_path()
    if not cred_path:
        raise RuntimeError(
            "Define FIREBASE_SERVICE_ACCOUNT_FILE ou GOOGLE_APPLICATION_CREDENTIALS."
        )

    if not firebase_admin._apps:
        firebase_admin.initialize_app(credentials.Certificate(cred_path))

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
        if not value
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


def _normalize_firestore_doc(doc) -> dict[str, Any]:
    data = doc.to_dict() or {}
    if "id" not in data:
        data["id"] = doc.id
    return data


def _rows_from_firestore(
    collection_name: str,
    account_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    db = get_db()
    query = db.collection(collection_name)

    if account_ids:
        if len(account_ids) == 1:
            query = query.where("accountId", "==", account_ids[0])
            docs = query.stream()
        else:
            rows: list[dict[str, Any]] = []
            for account_id in account_ids:
                partial = (
                    db.collection(collection_name)
                    .where("accountId", "==", account_id)
                    .stream()
                )
                rows.extend(_normalize_firestore_doc(doc) for doc in partial)
            return rows
    else:
        docs = query.stream()

    return [_normalize_firestore_doc(doc) for doc in docs]


def _quote_ident(name: str) -> str:
    safe = name.replace('"', '""')
    return f'"{safe}"'


def _rows_from_postgres(
    table_name: str,
    account_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    conn = get_db()
    sql = f"SELECT * FROM {_quote_ident(table_name)}"
    params: list[Any] = []

    if account_ids:
        if len(account_ids) == 1:
            sql += ' WHERE "accountId" = %s'
            params.append(account_ids[0])
        else:
            sql += ' WHERE "accountId" = ANY(%s)'
            params.append(account_ids)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    normalized: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        if "id" not in item:
            if f"{table_name[:-1]}Id" in item:
                item["id"] = item[f"{table_name[:-1]}Id"]
            elif table_name == "saleItems" and "saleItemId" in item:
                item["id"] = item["saleItemId"]
            elif table_name == "printJobs" and "printJobId" in item:
                item["id"] = item["printJobId"]
        normalized.append(item)

    return normalized


def _rows_from_provider(
    collection_name: str,
    account_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    if DB_PROVIDER == "firebase":
        return _rows_from_firestore(collection_name, account_ids=account_ids)
    if DB_PROVIDER == "postgres":
        return _rows_from_postgres(collection_name, account_ids=account_ids)
    raise RuntimeError(f"DB_PROVIDER inválido: {DB_PROVIDER}")


def _filter_rows_by_date_range(
    rows: list[dict[str, Any]],
    collection_name: str,
    date_range: DateRange | None,
) -> list[dict[str, Any]]:
    if date_range is None:
        return rows

    filtered: list[dict[str, Any]] = []
    for row in rows:
        row_dt = _pick_primary_date(row, collection_name)
        if row_dt is None:
            continue
        if date_range.start <= row_dt <= date_range.end:
            filtered.append(row)
    return filtered


def _sort_rows(
    rows: list[dict[str, Any]],
    collection_name: str,
    sort_desc: bool = True,
) -> list[dict[str, Any]]:
    date_fields = _get_collection_date_fields(collection_name)

    def sort_key(row: dict[str, Any]):
        for field in date_fields:
            parsed = parse_datetime(row.get(field))
            if parsed is not None:
                return parsed
        return datetime(1970, 1, 1, tzinfo=timezone.utc)

    return sorted(rows, key=sort_key, reverse=sort_desc)


def _rows_to_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(
                lambda x: x.isoformat() if isinstance(x, datetime) else x
            )

    return df


@st.cache_data(show_spinner=False, ttl=60)
def list_active_accounts() -> list[dict[str, Any]]:
    rows = _rows_from_provider("accounts")
    active = [row for row in rows if str(row.get("status", "")).upper() == "ACTIVE"]
    active = sorted(active, key=lambda x: str(x.get("emailAddress") or x.get("accountId") or ""))
    return active


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
    account_ids: list[str] | None = None,
    date_range: DateRange | None = None,
    sort_desc: bool = True,
) -> pd.DataFrame:
    rows = _rows_from_provider(collection_name, account_ids=account_ids)
    rows = _filter_rows_by_date_range(rows, collection_name, date_range)
    rows = _sort_rows(rows, collection_name, sort_desc=sort_desc)
    return _rows_to_df(rows)


@st.cache_data(show_spinner=False, ttl=60)
def load_sale_items_for_sale(sale_id: str) -> pd.DataFrame:
    if not sale_id:
        return pd.DataFrame()

    rows = _rows_from_provider("saleItems")
    result = [row for row in rows if str(row.get("saleId")) == str(sale_id)]
    result = _sort_rows(result, "saleItems", sort_desc=False)
    return _rows_to_df(result)


@st.cache_data(show_spinner=False, ttl=60)
def load_labels_for_sale(sale_id: str) -> pd.DataFrame:
    if not sale_id:
        return pd.DataFrame()

    rows = _rows_from_provider("labels")
    result = [row for row in rows if str(row.get("matchedSaleId")) == str(sale_id)]

    if not result:
        result = [row for row in rows if str(row.get("saleId")) == str(sale_id)]

    result = _sort_rows(result, "labels", sort_desc=True)
    return _rows_to_df(result)


@st.cache_data(show_spinner=False, ttl=60)
def load_print_jobs_for_sale(sale_id: str) -> pd.DataFrame:
    if not sale_id:
        return pd.DataFrame()

    rows = _rows_from_provider("printJobs")
    result = [row for row in rows if str(row.get("saleId")) == str(sale_id)]
    result = _sort_rows(result, "printJobs", sort_desc=True)
    return _rows_to_df(result)


def update_sale_status(sale_id: str, new_status: str) -> None:
    if not sale_id or not new_status:
        return

    now = datetime.now(timezone.utc).isoformat()

    if DB_PROVIDER == "firebase":
        db = get_db()
        db.collection("sales").document(str(sale_id)).set(
            {
                "status": new_status,
                "updatedAt": now,
            },
            merge=True,
        )
    elif DB_PROVIDER == "postgres":
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE "sales"
                SET "status" = %s,
                    "updatedAt" = %s
                WHERE "saleId" = %s
                """,
                (new_status, now, str(sale_id)),
            )
        conn.commit()
    else:
        raise RuntimeError(f"DB_PROVIDER inválido: {DB_PROVIDER}")

    clear_all_caches()


@st.cache_data(show_spinner=False, ttl=60)
def load_label_by_id(label_id: str) -> dict | None:
    if not label_id:
        return None

    rows = _rows_from_provider("labels")

    for row in rows:
        # tenta vários campos possíveis (porque tens inconsistência de nomes)
        if (
            str(row.get("labelId")) == str(label_id)
            or str(row.get("id")) == str(label_id)
        ):
            return row

    return None

def clear_all_caches() -> None:
    try:
        st.cache_data.clear()
    except Exception:
        pass

    try:
        st.cache_resource.clear()
    except Exception:
        pass