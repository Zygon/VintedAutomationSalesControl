import argparse
import sys

from app.config import (
    GMAIL_EXPENSES_QUERY,
    GMAIL_PAYOUTS_QUERY,
    get_gmail_token_file,
)
from app.expenses_processor import ExpensesProcessor
from app.firestore_client import FirebaseClient
from app.gmail_client import GmailClient
from app.order_events_processor import OrderEventsProcessor
from app.payouts_processor import PayoutsProcessor
from app.print_engine import PrintEngine
from app.worker import VintedWorker


DEFAULT_ORDER_EVENTS_QUERY = (
    '("Atualização do pedido para" OR "Este pedido está concluído") newer_than:30d is:unread'
)


def cmd_auth():
    result = VintedWorker.bootstrap_account_via_login()
    print("Conta autenticada com sucesso:")
    print(f"  accountId: {result['accountId']}")
    print(f"  emailAddress: {result['emailAddress']}")
    print(f"  tokenFile: {result['tokenFile']}")


def _build_gmail(account_id: str) -> GmailClient:
    return GmailClient(token_file=get_gmail_token_file(account_id))


def cmd_process_order_events(account_id: str):
    gmail = _build_gmail(account_id)
    processor = OrderEventsProcessor(account_id=account_id, gmail=gmail)
    message_ids = gmail.list_message_ids(DEFAULT_ORDER_EVENTS_QUERY, max_results=50)
    summary = processor.process_message_ids(message_ids)
    print(summary)


def cmd_process_expenses(account_id: str):
    gmail = _build_gmail(account_id)
    processor = ExpensesProcessor(account_id=account_id, gmail=gmail)
    message_ids = gmail.list_message_ids(GMAIL_EXPENSES_QUERY, max_results=50)
    summary = processor.process_message_ids(message_ids)
    print(summary)


def cmd_process_payouts(account_id: str):
    gmail = _build_gmail(account_id)
    processor = PayoutsProcessor(account_id=account_id, gmail=gmail)
    message_ids = gmail.list_message_ids(GMAIL_PAYOUTS_QUERY, max_results=50)
    summary = processor.process_message_ids(message_ids)
    print(summary)


def cmd_run_account(
    account_id: str,
    with_print: bool,
    with_order_events: bool,
    with_expenses: bool,
    with_payouts: bool,
):
    worker = VintedWorker(account_id=account_id)
    worker.run_once()

    if with_order_events:
        cmd_process_order_events(account_id)

    if with_expenses:
        cmd_process_expenses(account_id)

    if with_payouts:
        cmd_process_payouts(account_id)

    if with_print:
        printer = PrintEngine(account_id=account_id)
        printer.process_pending_print_jobs()


def cmd_full_run_account(account_id: str):
    cmd_run_account(
        account_id=account_id,
        with_print=True,
        with_order_events=True,
        with_expenses=True,
        with_payouts=True,
    )


def cmd_run_all(
    with_print: bool,
    with_order_events: bool,
    with_expenses: bool,
    with_payouts: bool,
):
    firebase = FirebaseClient()
    account_docs = firebase.list_active_accounts()

    if not account_docs:
        print("Não existem contas ACTIVE em accounts")
        return

    for account_doc in account_docs:
        account = account_doc.to_dict() or {}
        account_id = account.get("accountId") or account_doc.id

        print(f"\n=== RUN ACCOUNT: {account_id} ===")
        try:
            cmd_run_account(
                account_id,
                with_print=with_print,
                with_order_events=with_order_events,
                with_expenses=with_expenses,
                with_payouts=with_payouts,
            )
        except Exception as e:
            print(f"[ACCOUNT ERROR] {account_id} -> {e}")


def cmd_full_run_all():
    firebase = FirebaseClient()
    account_docs = firebase.list_active_accounts()

    if not account_docs:
        print("Não existem contas ACTIVE em accounts")
        return

    for account_doc in account_docs:
        account = account_doc.to_dict() or {}
        account_id = account.get("accountId") or account_doc.id

        print(f"\n=== FULL RUN ACCOUNT: {account_id} ===")
        try:
            cmd_full_run_account(account_id)
        except Exception as e:
            print(f"[ACCOUNT ERROR] {account_id} -> {e}")


def build_parser():
    parser = argparse.ArgumentParser(description="Vinted local automation runner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("auth", help="Autentica uma nova conta Google e cria accounts")

    run_parser = subparsers.add_parser("run", help="Corre o worker")
    run_group = run_parser.add_mutually_exclusive_group(required=True)
    run_group.add_argument("--account", help="Corre apenas uma conta")
    run_group.add_argument("--all-accounts", action="store_true", help="Corre todas as contas ativas")
    run_parser.add_argument("--with-print", action="store_true", help="Processa printJobs pendentes no fim")
    run_parser.add_argument(
        "--with-order-events",
        action="store_true",
        help="Processa emails de ORDER_SHIPPED e ORDER_COMPLETED",
    )
    run_parser.add_argument(
        "--with-expenses",
        action="store_true",
        help='Processa emails "Destaques de artigos - a tua fatura"',
    )
    run_parser.add_argument(
        "--with-payouts",
        action="store_true",
        help='Processa emails "O teu pagamento está a ser enviado para o banco"',
    )

    order_events_parser = subparsers.add_parser(
        "process-order-events",
        help="Processa apenas order events",
    )
    order_events_parser.add_argument("--account", required=True, help="Conta a processar")

    expenses_parser = subparsers.add_parser(
        "process-expenses",
        help="Processa apenas expenses",
    )
    expenses_parser.add_argument("--account", required=True, help="Conta a processar")

    payouts_parser = subparsers.add_parser(
        "process-payouts",
        help="Processa apenas payouts",
    )
    payouts_parser.add_argument("--account", required=True, help="Conta a processar")

    full_run_parser = subparsers.add_parser(
        "full-run",
        help="Corre worker + order events + expenses + payouts + print",
    )
    full_run_group = full_run_parser.add_mutually_exclusive_group(required=True)
    full_run_group.add_argument("--account", help="Corre apenas uma conta")
    full_run_group.add_argument("--all-accounts", action="store_true", help="Corre todas as contas ativas")

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    try:
        if args.command == "auth":
            cmd_auth()
            return 0

        if args.command == "run":
            if args.account:
                cmd_run_account(
                    args.account.strip().lower(),
                    with_print=args.with_print,
                    with_order_events=args.with_order_events,
                    with_expenses=args.with_expenses,
                    with_payouts=args.with_payouts,
                )
                return 0

            if args.all_accounts:
                cmd_run_all(
                    with_print=args.with_print,
                    with_order_events=args.with_order_events,
                    with_expenses=args.with_expenses,
                    with_payouts=args.with_payouts,
                )
                return 0

        if args.command == "process-order-events":
            cmd_process_order_events(args.account.strip().lower())
            return 0

        if args.command == "process-expenses":
            cmd_process_expenses(args.account.strip().lower())
            return 0

        if args.command == "process-payouts":
            cmd_process_payouts(args.account.strip().lower())
            return 0

        if args.command == "full-run":
            if args.account:
                cmd_full_run_account(args.account.strip().lower())
                return 0

            if args.all_accounts:
                cmd_full_run_all()
                return 0

        parser.print_help()
        return 1

    except Exception as e:
        print(f"[FATAL] {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())