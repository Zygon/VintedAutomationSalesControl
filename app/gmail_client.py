import base64
import json
from email import message_from_bytes
from pathlib import Path
from typing import Any

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from app.config import GMAIL_CLIENT_SECRET_FILE

SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
]


class GmailClient:
    def __init__(self, token_file: Path):
        self.token_file = token_file
        self.service = self._build_service()
        self._label_cache: dict[str, str] = {}

    def _build_service(self):
        creds = None

        if self.token_file.exists():
            try:
                creds = Credentials.from_authorized_user_file(str(self.token_file), SCOPES)
            except (json.JSONDecodeError, ValueError):
                self.token_file.unlink(missing_ok=True)
                creds = None

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(GMAIL_CLIENT_SECRET_FILE),
                    SCOPES,
                )
                creds = flow.run_local_server(port=0)

            self.token_file.parent.mkdir(parents=True, exist_ok=True)
            self.token_file.write_text(creds.to_json(), encoding="utf-8")

        return build("gmail", "v1", credentials=creds)

    def get_authenticated_email_address(self) -> str:
        profile = self.service.users().getProfile(userId="me").execute()
        return profile["emailAddress"].strip().lower()

    def list_message_ids(self, query: str, max_results: int = 20) -> list[str]:
        results = self.service.users().messages().list(
            userId="me",
            q=query,
            maxResults=max_results
        ).execute()

        messages = results.get("messages", [])
        return [m["id"] for m in messages]

    def get_message_raw(self, message_id: str) -> dict[str, Any]:
        return self.service.users().messages().get(
            userId="me",
            id=message_id,
            format="raw"
        ).execute()

    def get_message_full(self, message_id: str) -> dict[str, Any]:
        return self.service.users().messages().get(
            userId="me",
            id=message_id,
            format="full"
        ).execute()

    def get_email_message(self, message_id: str):
        raw_message = self.get_message_raw(message_id)
        raw_data = base64.urlsafe_b64decode(raw_message["raw"].encode("ASCII"))
        return message_from_bytes(raw_data), raw_message

    def download_pdf_attachment(self, message_id: str, output_dir: Path) -> Path | None:
        full_message = self.get_message_full(message_id)
        payload = full_message.get("payload", {})

        output_dir.mkdir(parents=True, exist_ok=True)
        return self._walk_parts_for_pdf(message_id, payload, output_dir)

    def _walk_parts_for_pdf(self, message_id: str, part: dict, output_dir: Path) -> Path | None:
        filename = part.get("filename")
        body = part.get("body", {})

        if filename and filename.lower().endswith(".pdf"):
            attachment_id = body.get("attachmentId")
            data = body.get("data")

            if attachment_id:
                attachment = self.service.users().messages().attachments().get(
                    userId="me",
                    messageId=message_id,
                    id=attachment_id
                ).execute()
                data = attachment.get("data")

            if data:
                file_data = base64.urlsafe_b64decode(data.encode("UTF-8"))
                file_path = output_dir / filename
                file_path.write_bytes(file_data)
                return file_path

        for child in part.get("parts", []) or []:
            result = self._walk_parts_for_pdf(message_id, child, output_dir)
            if result:
                return result

        return None

    def mark_as_read(self, message_id: str):
        self.service.users().messages().modify(
            userId="me",
            id=message_id,
            body={"removeLabelIds": ["UNREAD"]},
        ).execute()

    def mark_as_unread(self, message_id: str):
        self.service.users().messages().modify(
            userId="me",
            id=message_id,
            body={"addLabelIds": ["UNREAD"]},
        ).execute()

    def list_labels(self) -> list[dict]:
        result = self.service.users().labels().list(userId="me").execute()
        return result.get("labels", [])

    def get_or_create_label(self, label_name: str) -> str:
        cached = self._label_cache.get(label_name)
        if cached:
            return cached

        labels = self.list_labels()
        for label in labels:
            if label.get("name") == label_name:
                label_id = label["id"]
                self._label_cache[label_name] = label_id
                return label_id

        created = self.service.users().labels().create(
            userId="me",
            body={
                "name": label_name,
                "labelListVisibility": "labelShow",
                "messageListVisibility": "show",
            },
        ).execute()

        label_id = created["id"]
        self._label_cache[label_name] = label_id
        return label_id

    def ensure_vinted_labels(self):
        for label_name in [
            "Vinted",
            "Vinted/Vendas",
            "Vinted/Etiquetas",
            "Vinted/Despesas",
            "Vinted/Payouts",
        ]:
            self.get_or_create_label(label_name)

    def apply_label(self, message_id: str, label_name: str):
        label_id = self.get_or_create_label(label_name)
        self.service.users().messages().modify(
            userId="me",
            id=message_id,
            body={"addLabelIds": [label_id]},
        ).execute()

    def remove_label(self, message_id: str, label_name: str):
        label_id = self.get_or_create_label(label_name)
        self.service.users().messages().modify(
            userId="me",
            id=message_id,
            body={"removeLabelIds": [label_id]},
        ).execute()

    def archive_message(self, message_id: str):
        self.service.users().messages().modify(
            userId="me",
            id=message_id,
            body={"removeLabelIds": ["INBOX"]},
        ).execute()

    def move_to_label_folder(
        self,
        message_id: str,
        label_name: str,
        mark_read: bool = True,
        archive: bool = True,
    ):
        label_id = self.get_or_create_label(label_name)

        add_label_ids = [label_id]
        remove_label_ids = []

        if mark_read:
            remove_label_ids.append("UNREAD")

        if archive:
            remove_label_ids.append("INBOX")

        body: dict[str, list[str]] = {}
        if add_label_ids:
            body["addLabelIds"] = add_label_ids
        if remove_label_ids:
            body["removeLabelIds"] = remove_label_ids

        self.service.users().messages().modify(
            userId="me",
            id=message_id,
            body=body,
        ).execute()