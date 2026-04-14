from app.worker import VintedWorker
from app.print_engine import PrintEngine

if __name__ == "__main__":
    worker = VintedWorker()

    # Fase 1
    worker.ingest_sales()
    worker.ingest_labels()

    # Fase 2
    worker.process_labels_for_enrichment()

    # Fase 3
    printer = PrintEngine()
    printer.process_pending_print_jobs()