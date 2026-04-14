from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
import os
import sys

import pandas as pd
import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1.base_query import FieldFilter

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    load_dotenv = None


CURRENT_FILE = Path(__file__).resolve()
FRONTEND_ROOT = CURRENT_FILE.parents[1]
PROJECT_ROOT = CURRENT_FILE.parents[2]
DOTENV_PATH = PROJECT_ROOT / ".env"

if load_dotenv and DOTENV_PATH.exists():
    load_dotenv(dotenv_path=DOTENV_PATH)

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _resolve_credential_path() -> str:
    # 1) tenta por env vars normais
    credential_path = (
        os.getenv("FIREBASE_SERVICE_ACCOUNT_FILE")
        or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    )
    if credential_path and Path(credential_path).exists():
        return str(Path(credential_path).resolve())

    # 2) tenta via backend config.py
    try:
        from app.config import FIREBASE_SERVICE_ACCOUNT_FILE  # type: ignore

        backend_path = Path(FIREBASE_SERVICE_ACCOUNT_FILE)
        if backend_path.exists():
            return str(backend_path.resolve())
    except Exception:
        pass

    # 3) fallback previsível dentro do projeto
    fallback = PROJECT_ROOT / "credentials" / "firebase-service-account.json"
    if fallback.exists():
        return str(fallback.resolve())

    raise RuntimeError(
        "Não foi possível localizar as credenciais Firebase. "
        "Coloca o ficheiro em credentials/firebase-service-account.json "
        "ou define FIREBASE_SERVICE_ACCOUNT_FILE / GOOGLE_APPLICATION_CREDENTIALS."
    )


@st.cache_resource(show_spinner=False)
def get_db():
    credential_path = _resolve_credential_path()

    if not firebase_admin._apps:
        cred = credentials.Certificate(credential_path)
        firebase_admin.initialize_app(cred)

    return firestore.client()


def _parse_iso_datetime(value: Any) -> datetime | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=timezone.utc)

    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None

    return None


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


def list_active_accounts() -> list[dict[str, str]]:
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


def _stream_collection_for_allowed_accounts(
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


def load_collection_df(
    collection_name: str,
    account_id: str | None = None,
    allowed_account_ids: list[str] | None = None,
    date_field: str | None = None,
    date_range: dict | None = None,
) -> pd.DataFrame:
    docs = _stream_collection_for_allowed_accounts(
        collection_name,
        account_id=account_id,
        allowed_account_ids=allowed_account_ids,
    )

    rows: list[dict[str, Any]] = []

    for doc in docs:
        data = doc.to_dict() or {}
        data.setdefault("id", doc.id)

        if date_field and date_range:
            parsed_dt = _parse_iso_datetime(data.get(date_field))
            if parsed_dt is None:
                continue
            if not (date_range["start"] <= parsed_dt <= date_range["end"]):
                continue
            data[f"{date_field}Parsed"] = parsed_dt

        rows.append(data)

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
    ]:
        if column in df.columns:
            df[f"{column}Parsed"] = pd.to_datetime(df[column], errors="coerce", utc=True)

    return df


def load_sale_items_for_sale(
    sale_id: str,
    account_id: str | None = None,
    allowed_account_ids: list[str] | None = None,
) -> pd.DataFrame:
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

    return pd.DataFrame(rows)


def load_label_by_id(
    label_id: str,
    account_id: str | None = None,
    allowed_account_ids: list[str] | None = None,
) -> dict[str, Any] | None:
    if not label_id:
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


def load_products_df(
    account_id: str | None = None,
    allowed_account_ids: list[str] | None = None,
) -> pd.DataFrame:
    return load_collection_df(
        "products",
        account_id=account_id,
        allowed_account_ids=allowed_account_ids,
    )


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