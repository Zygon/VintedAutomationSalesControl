from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
import os
import sys

import pandas as pd
import streamlit as st

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    load_dotenv = None

try:
    import psycopg
    from psycopg.rows import dict_row
except ModuleNotFoundError:
    psycopg = None
    dict_row = None

import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1.base_query import FieldFilter


CURRENT_FILE = Path(__file__).resolve()
SERVICES_ROOT = CURRENT_FILE.parents[0]
PROJECT_ROOT = CURRENT_FILE.parents[1]
DOTENV_PATH = PROJECT_ROOT / ".env"

if load_dotenv and DOTENV_PATH.exists():
    load_dotenv(dotenv_path=DOTENV_PATH)

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


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
    "labels": ["receivedAt", "updatedAt", "createdAt"],
    "saleItems": ["completedAt", "createdAt", "updatedAt", "importedAt"],
    "expenses": ["receivedAt", "issuedAt", "createdAt", "updatedAt"],
    "payouts": ["receivedAt", "paidOutAt", "createdAt", "updatedAt"],
    "printJobs": ["printedAt", "updatedAt", "createdAt"],
    "events": ["createdAt"],
    "accounts": ["lastRunAt", "updatedAt", "createdAt"],
    "users": ["lastLoginAt", "updatedAt", "createdAt"],
    "userAccounts": ["updatedAt", "createdAt"],
    "products": ["updatedAt", "createdAt"],
    "skuCounters": ["updatedAt", "createdAt"],
    "generatedSkus": ["createdAt", "updatedAt"],
    "accountLinkCodes": ["updatedAt", "createdAt", "expiresAt"],
}


def _ensure_timezone(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_iso_datetime(value: Any) -> datetime | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        return _ensure_timezone(value)

    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=timezone.utc)

    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return _ensure_timezone(value.to_pydatetime())

    if hasattr(value, "to_datetime"):
        try:
            return _ensure_timezone(value.to_datetime())
        except Exception:
            return None

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


def _normalize_datetime_for_storage(value: Any) -> Any:
    parsed = _parse_iso_datetime(value)
    if parsed is not None:
        return parsed.isoformat()
    return value


def compute_date_range(
    period_mode: str,
    custom_start: date | None = None,
    custom_end: date | None = None,
):
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


def _coerce_date_range(date_range: dict | DateRange | None) -> DateRange | None:
    if date_range is None:
        return None

    if isinstance(date_range, DateRange):
        return date_range

    if isinstance(date_range, dict):
        start = _parse_iso_datetime(date_range.get("start"))
        end = _parse_iso_datetime(date_range.get("end"))
        if start and end:
            return DateRange(start=start, end=end)

    return None


def _resolve_firebase_credentials() -> credentials.Certificate:
    # 1) Streamlit Cloud secrets / local secrets.toml
    try:
        if "firebase_service_account" in st.secrets:
            secret_data = dict(st.secrets["firebase_service_account"])
            return credentials.Certificate(secret_data)
    except Exception:
        pass

    # 2) env vars locais
    credential_path = (
        FIREBASE_SERVICE_ACCOUNT_FILE
        or GOOGLE_APPLICATION_CREDENTIALS
    )
    if credential_path and Path(credential_path).exists():
        return credentials.Certificate(str(Path(credential_path).resolve()))

    # 3) backend config.py local, se existir
    try:
        from app.config import FIREBASE_SERVICE_ACCOUNT_FILE as BACKEND_FIREBASE_SERVICE_ACCOUNT_FILE  # type: ignore

        backend_path = Path(BACKEND_FIREBASE_SERVICE_ACCOUNT_FILE)
        if backend_path.exists():
            return credentials.Certificate(str(backend_path.resolve()))
    except Exception:
        pass

    raise RuntimeError(
        "Credenciais Firebase não encontradas. "
        "No Streamlit Cloud usa [firebase_service_account] em Secrets. "
        "Localmente usa FIREBASE_SERVICE_ACCOUNT_FILE ou GOOGLE_APPLICATION_CREDENTIALS."
    )


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


@st.cache_resource(show_spinner=False)
def get_db():
    if DB_PROVIDER == "postgres":
        if psycopg is None or dict_row is None:
            raise RuntimeError(
                "psycopg não está instalado. Adiciona psycopg[binary] ao requirements."
            )
        return psycopg.connect(_postgres_dsn(), row_factory=dict_row)

    cred = _resolve_firebase_credentials()

    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)

    return firestore.client()


def _normalize_firestore_doc(doc) -> dict[str, Any]:
    data = doc.to_dict() or {}
    data.setdefault("id", doc.id)
    return data


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _guess_id_for_postgres_row(table_name: str, row: dict[str, Any]) -> dict[str, Any]:
    if "id" in row:
        return row

    preferred_ids = [
        "id",
        "saleId",
        "labelId",
        "saleItemId",
        "payoutId",
        "expenseId",
        "productId",
        "printJobId",
        "eventId",
        "accountId",
        "userId",
        "generatedSkuId",
        "code",
    ]

    for key in preferred_ids:
        if key in row and row.get(key) is not None:
            row["id"] = row[key]
            return row

    # fallback fraco mas evita rebentar tabelas sem PK “bonita”
    if table_name[:-1] + "Id" in row:
        row["id"] = row[table_name[:-1] + "Id"]

    return row


def _stream_collection_for_allowed_accounts_firestore(
    collection_name: str,
    account_id: str | None = None,
    allowed_account_ids: list[str] | None = None,
):
    db = get_db()

    if account_id:
        query = db.collection(collection_name).where(
            filter=FieldFilter("accountId", "==", account_id)
        )
        return list(query.stream())

    if not allowed_account_ids:
        return []

    docs = []
    for allowed_account_id in allowed_account_ids:
        query = db.collection(collection_name).where(
            filter=FieldFilter("accountId", "==", allowed_account_id)
        )
        docs.extend(list(query.stream()))

    return docs


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
        rows = [dict(row) for row in cur.fetchall()]

    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized.append(_guess_id_for_postgres_row(table_name, row))

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

    docs = _stream_collection_for_allowed_accounts_firestore(
        collection_name,
        account_id=account_id,
        allowed_account_ids=allowed_account_ids,
    )
    return [_normalize_firestore_doc(doc) for doc in docs]


def _pick_primary_date(row: dict[str, Any], collection_name: str) -> datetime | None:
    for field in COLLECTION_DATE_FIELDS.get(collection_name, ["createdAt", "updatedAt"]):
        parsed = _parse_iso_datetime(row.get(field))
        if parsed is not None:
            return parsed
    return None


def _filter_rows_by_date_range(
    rows: list[dict[str, Any]],
    collection_name: str,
    date_field: str | None = None,
    date_range: dict | DateRange | None = None,
) -> list[dict[str, Any]]:
    resolved_range = _coerce_date_range(date_range)
    if resolved_range is None:
        return rows

    filtered: list[dict[str, Any]] = []

    for row in rows:
        parsed_dt = (
            _parse_iso_datetime(row.get(date_field))
            if date_field
            else _pick_primary_date(row, collection_name)
        )

        if parsed_dt is None:
            continue

        if resolved_range.start <= parsed_dt <= resolved_range.end:
            filtered.append(row)

    return filtered


def _sort_rows(
    rows: list[dict[str, Any]],
    collection_name: str,
    date_field: str | None = None,
    descending: bool = True,
) -> list[dict[str, Any]]:
    def sort_key(row: dict[str, Any]):
        if date_field:
            parsed = _parse_iso_datetime(row.get(date_field))
            if parsed is not None:
                return parsed

        parsed = _pick_primary_date(row, collection_name)
        if parsed is not None:
            return parsed

        return datetime(1970, 1, 1, tzinfo=timezone.utc)

    return sorted(rows, key=sort_key, reverse=descending)


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
        "printedAt",
        "lastLoginAt",
        "lastRunAt",
        "importedAt",
        "expiresAt",
    ]:
        if column in df.columns:
            df[f"{column}Parsed"] = pd.to_datetime(df[column], errors="coerce", utc=True)

    return df


@st.cache_data(show_spinner=False, ttl=60)
def list_active_accounts() -> list[dict[str, str]]:
    if DB_PROVIDER == "postgres":
        rows = _rows_from_postgres("accounts")
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

    db = get_db()
    docs = (
        db.collection("accounts")
        .where(filter=FieldFilter("status", "==", "ACTIVE"))
        .stream()
    )

    accounts: list[dict[str, str]] = []
    for doc in docs:
        data = doc.to_dict() or {}
        account_id = data.get("accountId") or doc.id
        email = data.get("emailAddress") or account_id
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

    if DB_PROVIDER == "postgres":
        rows = _rows_from_postgres("userAccounts")
        result: list[str] = []
        for row in rows:
            if str(row.get("userId")) != str(user_id):
                continue
            if str(row.get("status", "ACTIVE")).upper() != "ACTIVE":
                continue
            account_id = row.get("accountId")
            if account_id:
                result.append(str(account_id))
        return list(dict.fromkeys(result))

    db = get_db()
    docs = (
        db.collection("userAccounts")
        .where(filter=FieldFilter("userId", "==", user_id))
        .stream()
    )

    result: list[str] = []
    for doc in docs:
        data = doc.to_dict() or {}
        if str(data.get("status", "ACTIVE")).upper() != "ACTIVE":
            continue
        account_id = data.get("accountId")
        if account_id:
            result.append(str(account_id))

    return list(dict.fromkeys(result))


@st.cache_data(show_spinner=False, ttl=60)
def load_collection_df(
    collection_name: str,
    account_id: str | None = None,
    allowed_account_ids: list[str] | None = None,
    date_field: str | None = None,
    date_range: dict | DateRange | None = None,
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
        descending=True,
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
        rows = _sort_rows(rows, "saleItems", date_field="createdAt", descending=False)
        return _rows_to_df(rows)

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

    rows = _sort_rows(rows, "saleItems", date_field="createdAt", descending=False)
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

    result = _sort_rows(result, "labels", date_field="createdAt", descending=True)
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
                (str(new_status).strip(), now_iso, str(sale_id)),
            )
        conn.commit()
        clear_all_caches()
        return

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