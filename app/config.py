from pathlib import Path
import os
import re
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
CREDENTIALS_DIR = BASE_DIR / "credentials"
DATA_DIR = BASE_DIR / "data"

DOWNLOADS_DIR = DATA_DIR / "downloads"
GMAIL_TOKENS_DIR = DATA_DIR / "gmail_tokens"

GMAIL_CLIENT_SECRET_FILE = CREDENTIALS_DIR / "gmail-oauth-client.json"
FIREBASE_SERVICE_ACCOUNT_FILE = CREDENTIALS_DIR / "firebase-service-account.json"

GMAIL_SALES_QUERY = os.getenv(
    "GMAIL_SALES_QUERY",
    'subject:"Vendeste um artigo na Vinted" newer_than:7d is:unread'
)

GMAIL_LABELS_QUERY = os.getenv(
    "GMAIL_LABELS_QUERY",
    'subject:"Etiqueta de envio" newer_than:7d is:unread'
)

GMAIL_EXPENSES_QUERY = os.getenv(
    "GMAIL_EXPENSES_QUERY",
    'subject:"Destaques de artigos - a tua fatura" newer_than:30d is:unread'
)

GMAIL_PAYOUTS_QUERY = os.getenv(
    "GMAIL_PAYOUTS_QUERY",
    'subject:"O teu pagamento está a ser enviado para o banco" newer_than:30d is:unread'
)

SUMATRA_PDF_PATH = os.getenv(
    "SUMATRA_PDF_PATH",
    r"C:\Users\cfilipem\AppData\Local\SumatraPDF\SumatraPDF.exe"
)
DEFAULT_PRINTER_NAME = os.getenv("DEFAULT_PRINTER_NAME", "")
PRINT_MOVE_AFTER_PRINT = os.getenv("PRINT_MOVE_AFTER_PRINT", "true").lower() == "true"


def normalize_email_address(email: str | None) -> str | None:
    if not email:
        return None
    return email.strip().lower()


def build_account_id(email_address: str) -> str:
    normalized = normalize_email_address(email_address)
    if not normalized:
        raise ValueError("email_address é obrigatório")
    return normalized


def sanitize_for_path(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9._-]+", "_", value)
    return value[:120] or "default"


def get_gmail_token_file(account_id: str) -> Path:
    GMAIL_TOKENS_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = sanitize_for_path(account_id)
    return GMAIL_TOKENS_DIR / f"{safe_name}.json"


def get_account_dirs(account_id: str) -> dict[str, Path]:
    safe_name = sanitize_for_path(account_id)
    root_dir = DOWNLOADS_DIR / safe_name

    raw_labels_dir = root_dir / "raw_labels"
    enriched_labels_dir = root_dir / "enriched_labels"
    printed_labels_dir = root_dir / "printed_labels"

    raw_labels_dir.mkdir(parents=True, exist_ok=True)
    enriched_labels_dir.mkdir(parents=True, exist_ok=True)
    printed_labels_dir.mkdir(parents=True, exist_ok=True)

    return {
        "rootDir": root_dir,
        "rawLabelsDir": raw_labels_dir,
        "enrichedLabelsDir": enriched_labels_dir,
        "printedLabelsDir": printed_labels_dir,
    }