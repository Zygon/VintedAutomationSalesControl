import os
import shutil
import subprocess
import time
from pathlib import Path
from datetime import datetime, timezone

from app.config import (
    DEFAULT_PRINTER_NAME,
    PRINT_MOVE_AFTER_PRINT,
    SUMATRA_PDF_PATH,
)
from app.firestore_client import FirebaseClient


PDF_PRINTER_HINTS = {
    "pdf",
    "microsoft print to pdf",
    "adobe pdf",
    "foxit pdf",
    "cutepdf",
    "pdfcreator",
}


class PrintEngine:
    def __init__(self, account_id: str):
        self.account_id = account_id
        self.firebase = FirebaseClient()

    def process_pending_print_jobs(self):
        jobs = self.firebase.get_pending_print_jobs(self.account_id)

        if not jobs:
            print(f"[PRINT][{self.account_id}] nenhum print job pendente")
            return

        for job_doc in jobs:
            job_id = job_doc.id
            job = job_doc.to_dict() or {}
            self._process_single_job(job_id, job)

    def _process_single_job(self, job_id: str, job: dict):
        pdf_path_value = job.get("pdfPath")
        label_id = job.get("labelId")
        sale_id = job.get("saleId")

        if not pdf_path_value:
            self.firebase.update_print_job(job_id, {
                "status": "FAILED",
                "error": "pdfPath vazio",
                "updatedAt": self._utc_now_iso(),
            })
            self._mark_label_and_sale(label_id, sale_id, "PRINT_ERROR")
            return

        pdf_path = Path(pdf_path_value)
        if not pdf_path.exists():
            self.firebase.update_print_job(job_id, {
                "status": "FAILED",
                "error": f"Ficheiro não existe: {pdf_path}",
                "updatedAt": self._utc_now_iso(),
            })
            self._mark_label_and_sale(label_id, sale_id, "FILE_MISSING")
            return

        self.firebase.update_print_job(job_id, {
            "status": "DISPATCHING",
            "updatedAt": self._utc_now_iso(),
        })
        self._mark_label_and_sale(label_id, sale_id, "PRINTING")

        try:
            dispatch_info = self._send_to_printer(pdf_path)

            final_pdf_path = pdf_path
            if PRINT_MOVE_AFTER_PRINT:
                final_pdf_path = self._move_to_printed_folder_with_retry(pdf_path)

            final_status = "PRINT_DISPATCHED" if dispatch_info["is_virtual_pdf"] else "PRINTED"
            final_job_status = "DISPATCHED" if dispatch_info["is_virtual_pdf"] else "PRINTED"

            self.firebase.update_print_job(job_id, {
                "status": final_job_status,
                "finalPdfPath": str(final_pdf_path.resolve()),
                "updatedAt": self._utc_now_iso(),
            })
            self._mark_label_and_sale(label_id, sale_id, final_status, str(final_pdf_path.resolve()))

        except Exception as e:
            self.firebase.update_print_job(job_id, {
                "status": "FAILED",
                "error": str(e),
                "updatedAt": self._utc_now_iso(),
            })
            self._mark_label_and_sale(label_id, sale_id, "PRINT_ERROR")

    def _mark_label_and_sale(self, label_id: str, sale_id: str, status: str, final_path: str | None = None):
        label_update = {"status": status}
        sale_update = {"status": status}

        if final_path:
            label_update["printedPdfPath"] = final_path
            label_update["localEnrichedPdfPath"] = final_path
            sale_update["printedPdfPath"] = final_path
            sale_update["localEnrichedPdfPath"] = final_path

        if label_id:
            self.firebase.update_label(label_id, label_update)
        if sale_id:
            self.firebase.update_sale(sale_id, sale_update)

    def _send_to_printer(self, pdf_path: Path) -> dict:
        is_virtual_pdf = self._is_virtual_pdf_printer(DEFAULT_PRINTER_NAME)
        sumatra_path = Path(SUMATRA_PDF_PATH)

        if sumatra_path.exists():
            cmd = [
                str(sumatra_path),
                "-print-to-default" if not DEFAULT_PRINTER_NAME else "-print-to",
            ]

            if DEFAULT_PRINTER_NAME:
                cmd.append(DEFAULT_PRINTER_NAME)

            cmd.extend([
                "-silent",
                "-exit-when-done",
                str(pdf_path),
            ])

            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise RuntimeError(
                    f"SumatraPDF falhou (code={result.returncode}): "
                    f"{result.stderr.strip() or result.stdout.strip()}"
                )

            time.sleep(2)
            return {"is_virtual_pdf": is_virtual_pdf}

        if os.name == "nt":
            os.startfile(str(pdf_path), "print")
            time.sleep(3)
            return {"is_virtual_pdf": is_virtual_pdf}

        raise RuntimeError("Nenhum método de impressão configurado.")

    def _move_to_printed_folder_with_retry(self, pdf_path: Path, retries: int = 10, delay_seconds: int = 2) -> Path:
        last_error = None

        for _ in range(retries):
            try:
                return self._move_to_printed_folder(pdf_path)
            except PermissionError as e:
                last_error = e
                time.sleep(delay_seconds)
            except OSError as e:
                last_error = e
                time.sleep(delay_seconds)

        raise RuntimeError(f"Não foi possível mover o ficheiro após impressão: {last_error}")

    def _move_to_printed_folder(self, pdf_path: Path) -> Path:
        printed_dir = pdf_path.parent.parent / "printed_labels"
        printed_dir.mkdir(parents=True, exist_ok=True)

        destination = printed_dir / pdf_path.name
        if destination.exists():
            destination.unlink()

        shutil.move(str(pdf_path), str(destination))
        return destination

    def _is_virtual_pdf_printer(self, printer_name: str) -> bool:
        if not printer_name:
            return False
        normalized = printer_name.strip().lower()
        return any(hint in normalized for hint in PDF_PRINTER_HINTS)

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()