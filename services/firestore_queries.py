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


def _get_setting(name: str, default: Any = None) -> Any:
    try:
        if name in st.secrets:
            return st.secrets[name]
    except Exception:
        pass

    env_value = os.getenv(name)
    if env_value is not None:
        return env_value

    return default


DB_PROVIDER = str(_get_setting("DB_PROVIDER", "postgres")).strip().lower()

FIREBASE_SERVICE_ACCOUNT_FILE = os.getenv("FIREBASE_SERVICE_ACCOUNT_FILE")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")


@dataclass
class DateRange:
    start: datetime
    end: datetime


PRIMARY_KEY_BY_TABLE: dict[str, str] = {
    "accounts": "accountId",
    "accountLinkCodes": "code",
    "users": "userId",
    "userAccounts": "id",
    "products": "productId",
    "skuCounters": "id",
    "generatedSkus": "generatedSkuId",
    "sales": "saleId",
    "labels": "labelId",
    "printJobs": "printJobId",
    "events": "eventId",
    "saleItems": "saleItemId",
    "expenses": "expenseId",
    "payouts": "payoutId",
    "appMeta": "id",
}

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


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _normalize_value_for_storage(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _normalize_value_for_storage(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize_value_for_storage(v) for v in value]
    return _normalize_datetime_for_storage(value)


def _get_postgres_config() -> dict[str, Any]:
    cfg = {}
    try:
        if "postgres" in st.secrets:
            cfg = dict(st.secrets["postgres"])
    except Exception:
        cfg = {}

    host = cfg.get("host") or os.getenv("POSTGRES_HOST", "").strip()
    port = int(cfg.get("port") or os.getenv("POSTGRES_PORT", "5432"))
    database = cfg.get("database") or os.getenv("POSTGRES_DB", "").strip()
    user = cfg.get("user") or os.getenv("POSTGRES_USER", "").strip()
    password = cfg.get("password") or os.getenv("POSTGRES_PASSWORD", "").strip()
    sslmode = cfg.get("sslmode") or os.getenv("POSTGRES_SSLMODE", "require").strip()
    connect_timeout = int(cfg.get("connect_timeout") or os.getenv("POSTGRES_CONNECT_TIMEOUT", "30"))

    missing = [
        name
        for name, value in {
            "host": host,
            "database": database,
            "user": user,
            "password": password,
        }.items()
        if not value or value == "..."
    ]

    if missing:
        raise RuntimeError(
            f"Config PostgreSQL incompleta. Variáveis em falta: {', '.join(missing)}"
        )

    return {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "sslmode": sslmode,
        "connect_timeout": connect_timeout,
    }


def _postgres_dsn() -> str:
    cfg = _get_postgres_config()
    return (
        f"host={cfg['host']} "
        f"port={cfg['port']} "
        f"dbname={cfg['database']} "
        f"user={cfg['user']} "
        f"password={cfg['password']} "
        f"sslmode={cfg['sslmode']} "
        f"connect_timeout={cfg['connect_timeout']}"
    )


def _resolve_firebase_credentials() -> credentials.Certificate:
    try:
        if "firebase_service_account" in st.secrets:
            secret_data = dict(st.secrets["firebase_service_account"])
            return credentials.Certificate(secret_data)
    except Exception:
        pass

    credential_path = FIREBASE_SERVICE_ACCOUNT_FILE or GOOGLE_APPLICATION_CREDENTIALS
    if credential_path and Path(credential_path).exists():
        return credentials.Certificate(str(Path(credential_path).resolve()))

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


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _normalize_firestore_doc(doc) -> dict[str, Any]:
    data = doc.to_dict() or {}
    data.setdefault("id", doc.id)
    return data


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

    fallback = table_name[:-1] + "Id"
    if fallback in row:
        row["id"] = row[fallback]

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

    return [_guess_id_for_postgres_row(table_name, row) for row in rows]


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


# =========================================================
# Generic provider-agnostic helpers
# =========================================================

def get_row_by_id(table_name: str, row_id: str) -> dict[str, Any] | None:
    if not row_id:
        return None

    pk_field = PRIMARY_KEY_BY_TABLE.get(table_name)
    if not pk_field:
        raise ValueError(f"Sem PK configurada para {table_name}")

    if DB_PROVIDER == "postgres":
        conn = get_db()
        sql = (
            f"SELECT * FROM {_quote_ident(table_name)} "
            f"WHERE {_quote_ident(pk_field)} = %s "
            f"LIMIT 1"
        )
        with conn.cursor() as cur:
            cur.execute(sql, (row_id,))
            row = cur.fetchone()

        if not row:
            return None

        return _guess_id_for_postgres_row(table_name, dict(row))

    db = get_db()
    doc = db.collection(table_name).document(str(row_id)).get()
    if not doc.exists:
        return None
    return _normalize_firestore_doc(doc)


def query_rows(
    table_name: str,
    filters: list[tuple[str, str, Any]] | None = None,
    order_by: str | None = None,
    descending: bool = False,
) -> list[dict[str, Any]]:
    filters = filters or []

    if DB_PROVIDER == "postgres":
        conn = get_db()
        sql = f"SELECT * FROM {_quote_ident(table_name)}"
        params: list[Any] = []
        where_parts: list[str] = []

        for field, op, value in filters:
            ident = _quote_ident(field)
            if op == "==":
                where_parts.append(f"{ident} = %s")
                params.append(value)
            elif op == "in":
                where_parts.append(f"{ident} = ANY(%s)")
                params.append(value)
            else:
                raise NotImplementedError(f"Operador não suportado: {op}")

        if where_parts:
            sql += " WHERE " + " AND ".join(where_parts)

        if order_by:
            sql += f" ORDER BY {_quote_ident(order_by)} {'DESC' if descending else 'ASC'}"

        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = [dict(row) for row in cur.fetchall()]

        return [_guess_id_for_postgres_row(table_name, row) for row in rows]

    db = get_db()
    query = db.collection(table_name)

    for field, op, value in filters:
        query = query.where(filter=FieldFilter(field, op, value))

    if order_by:
        direction = firestore.Query.DESCENDING if descending else firestore.Query.ASCENDING
        query = query.order_by(order_by, direction=direction)

    docs = query.stream()
    return [_normalize_firestore_doc(doc) for doc in docs]


def upsert_row(table_name: str, row_id: str, data: dict[str, Any]) -> None:
    if not row_id:
        raise ValueError("row_id é obrigatório")

    pk_field = PRIMARY_KEY_BY_TABLE.get(table_name)
    if not pk_field:
        raise ValueError(f"Sem PK configurada para {table_name}")

    payload = {k: _normalize_value_for_storage(v) for k, v in data.items()}
    payload[pk_field] = row_id

    if DB_PROVIDER == "postgres":
        conn = get_db()
        columns = list(payload.keys())
        values = [payload[c] for c in columns]

        column_sql = ", ".join(_quote_ident(c) for c in columns)
        placeholder_sql = ", ".join(["%s"] * len(columns))
        update_sql = ", ".join(
            f"{_quote_ident(c)} = EXCLUDED.{_quote_ident(c)}"
            for c in columns
            if c != pk_field
        )

        sql = (
            f"INSERT INTO {_quote_ident(table_name)} ({column_sql}) "
            f"VALUES ({placeholder_sql}) "
            f"ON CONFLICT ({_quote_ident(pk_field)}) DO UPDATE SET {update_sql}"
        )

        with conn.cursor() as cur:
            cur.execute(sql, values)
        conn.commit()
        clear_all_caches()
        return

    db = get_db()
    db.collection(table_name).document(str(row_id)).set(payload, merge=True)
    clear_all_caches()


def update_row_fields(table_name: str, row_id: str, fields: dict[str, Any]) -> None:
    existing = get_row_by_id(table_name, row_id) or {}
    merged = {**existing, **fields}
    upsert_row(table_name, row_id, merged)


# =========================================================
# High-level helpers for services
# =========================================================

def get_account_by_id(account_id: str) -> dict[str, Any] | None:
    return get_row_by_id("accounts", account_id)


def get_link_code_by_code(code: str) -> dict[str, Any] | None:
    return get_row_by_id("accountLinkCodes", code)


def get_user_account_by_id(rel_id: str) -> dict[str, Any] | None:
    return get_row_by_id("userAccounts", rel_id)


def list_user_account_rows(user_id: str, only_active: bool = False) -> list[dict[str, Any]]:
    filters: list[tuple[str, str, Any]] = [("userId", "==", user_id)]
    if only_active:
        filters.append(("status", "==", "ACTIVE"))

    rows = query_rows("userAccounts", filters=filters)
    rows.sort(key=lambda x: (not bool(x.get("isDefault")), str(x.get("accountId") or "")))
    return rows


def list_link_code_rows(account_id: str) -> list[dict[str, Any]]:
    rows = query_rows(
        "accountLinkCodes",
        filters=[("accountId", "==", account_id)],
    )
    rows.sort(key=lambda x: str(x.get("createdAt") or ""), reverse=True)
    return rows


def list_active_accounts() -> list[dict[str, str]]:
    rows = query_rows("accounts", filters=[("status", "==", "ACTIVE")])

    accounts: list[dict[str, str]] = []
    for row in rows:
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


def list_user_account_ids(user_id: str) -> list[str]:
    rows = list_user_account_rows(user_id, only_active=True)
    result: list[str] = []
    for row in rows:
        account_id = row.get("accountId")
        if account_id:
            result.append(str(account_id))
    return list(dict.fromkeys(result))


def upsert_user_account(rel_id: str, data: dict[str, Any]) -> None:
    payload = dict(data)
    payload.setdefault("id", rel_id)
    payload.setdefault("userAccountId", rel_id)
    upsert_row("userAccounts", rel_id, payload)


def revoke_user_account_link(rel_id: str) -> None:
    update_row_fields("userAccounts", rel_id, {
        "status": "REVOKED",
        "isDefault": False,
        "updatedAt": utc_now_iso(),
    })


def set_default_user_account(user_id: str, account_id: str) -> None:
    relations = list_user_account_rows(user_id, only_active=True)
    if not relations:
        raise ValueError("Conta não associada ao utilizador.")

    found = False
    now_iso = utc_now_iso()

    for rel in relations:
        rel_id = rel.get("userAccountId") or rel.get("id")
        if not rel_id:
            continue

        is_target = rel.get("accountId") == account_id
        if is_target:
            found = True

        upsert_user_account(str(rel_id), {
            "isDefault": is_target,
            "updatedAt": now_iso,
        })

    if not found:
        raise ValueError("Conta não associada ao utilizador.")


def upsert_account_link_code(code: str, data: dict[str, Any]) -> None:
    payload = dict(data)
    payload.setdefault("code", code)
    upsert_row("accountLinkCodes", code, payload)


def claim_account_link_code_usage(code: str, used_count: int) -> None:
    update_row_fields("accountLinkCodes", code, {
        "usedCount": int(used_count),
        "updatedAt": utc_now_iso(),
    })


def upsert_product_row(product_id: str, data: dict[str, Any]) -> None:
    payload = dict(data)
    payload.setdefault("productId", product_id)
    upsert_row("products", product_id, payload)


def reserve_next_sku(
    account_id: str,
    prefix: str,
    padding: int,
    generated_by: str = "streamlit-local-ui",
) -> dict[str, Any]:
    if not account_id:
        raise ValueError("account_id é obrigatório")

    counter_id = f"{account_id}__{prefix}"
    now_iso = utc_now_iso()

    if DB_PROVIDER == "postgres":
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                'SELECT * FROM "skuCounters" WHERE "id" = %s FOR UPDATE',
                (counter_id,),
            )
            current = cur.fetchone()
            current_value = 0
            created_at = now_iso

            if current:
                current_dict = dict(current)
                current_value = int(current_dict.get("currentValue") or 0)
                created_at = current_dict.get("createdAt") or now_iso

            next_value = current_value + 1
            next_sku = f"{prefix}{str(next_value).zfill(padding)}"
            generated_sku_id = f"{account_id}__{next_sku}"

            cur.execute(
                """
                INSERT INTO "skuCounters" (
                    "id", "accountId", "prefix", "currentValue", "createdAt", "updatedAt"
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT ("id") DO UPDATE SET
                    "accountId" = EXCLUDED."accountId",
                    "prefix" = EXCLUDED."prefix",
                    "currentValue" = EXCLUDED."currentValue",
                    "updatedAt" = EXCLUDED."updatedAt"
                """,
                (
                    counter_id,
                    account_id,
                    prefix,
                    next_value,
                    created_at,
                    now_iso,
                ),
            )

            cur.execute(
                """
                INSERT INTO "generatedSkus" (
                    "generatedSkuId", "accountId", "sku", "prefix", "sequenceNumber",
                    "createdAt", "updatedAt", "meta"
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("generatedSkuId") DO UPDATE SET
                    "accountId" = EXCLUDED."accountId",
                    "sku" = EXCLUDED."sku",
                    "prefix" = EXCLUDED."prefix",
                    "sequenceNumber" = EXCLUDED."sequenceNumber",
                    "updatedAt" = EXCLUDED."updatedAt",
                    "meta" = EXCLUDED."meta"
                """,
                (
                    generated_sku_id,
                    account_id,
                    next_sku,
                    prefix,
                    next_value,
                    now_iso,
                    now_iso,
                    {
                        "padding": padding,
                        "generatedFor": "LISTING_HELPER",
                        "generatedBy": generated_by,
                        "isAssigned": False,
                        "assignedProductId": None,
                    },
                ),
            )

        conn.commit()
        clear_all_caches()

        return {
            "sku": next_sku,
            "numericValue": next_value,
            "accountId": account_id,
            "prefix": prefix,
            "padding": padding,
        }

    db = get_db()
    counter_ref = db.collection("skuCounters").document(counter_id)

    @firestore.transactional
    def _tx(transaction: firestore.Transaction):
        snap = counter_ref.get(transaction=transaction)
        current_value = 0
        if snap.exists:
            current_value = int((snap.to_dict() or {}).get("currentValue") or 0)

        next_value = current_value + 1
        next_sku = f"{prefix}{str(next_value).zfill(padding)}"

        transaction.set(
            counter_ref,
            {
                "id": counter_id,
                "counterId": counter_id,
                "accountId": account_id,
                "prefix": prefix,
                "currentValue": next_value,
                "updatedAt": now_iso,
                "createdAt": now_iso if not snap.exists else firestore.DELETE_FIELD,
            },
            merge=True,
        )

        generated_ref = db.collection("generatedSkus").document(f"{account_id}__{next_sku}")
        transaction.set(
            generated_ref,
            {
                "generatedSkuId": f"{account_id}__{next_sku}",
                "accountId": account_id,
                "sku": next_sku,
                "prefix": prefix,
                "sequenceNumber": next_value,
                "createdAt": now_iso,
                "updatedAt": now_iso,
                "meta": {
                    "padding": padding,
                    "generatedFor": "LISTING_HELPER",
                    "generatedBy": generated_by,
                    "isAssigned": False,
                    "assignedProductId": None,
                },
            },
            merge=True,
        )

        return {
            "sku": next_sku,
            "numericValue": next_value,
            "accountId": account_id,
            "prefix": prefix,
            "padding": padding,
        }

    transaction = db.transaction()
    result = _tx(transaction)
    clear_all_caches()
    return result

# =========================================================
# Existing read helpers used by pages
# =========================================================

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

    rows = _rows_from_provider(
        "saleItems",
        account_id=account_id,
        allowed_account_ids=allowed_account_ids,
    )
    rows = [row for row in rows if str(row.get("saleId")) == str(sale_id)]
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

    rows = _rows_from_provider(
        "labels",
        account_id=account_id,
        allowed_account_ids=allowed_account_ids,
    )
    for row in rows:
        if str(row.get("labelId")) == str(label_id) or str(row.get("id")) == str(label_id):
            return row
    return None


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
    update_row_fields("sales", str(sale_id), {
        "status": str(new_status).strip(),
        "updatedAt": now_iso,
    })
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