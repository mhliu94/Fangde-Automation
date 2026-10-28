from __future__ import annotations

import argparse
from contextlib import nullcontext
import json
import logging
import sys
from dataclasses import asdict
from pathlib import Path

from .config import execution_backend, load_settings
from .errors import FangdeError
from .lock import EmulatorLock
from .monitor import main as monitor_main
from .monitor import monitor_status, start_monitor, stop_monitor
from .orders import OrderAutomation
from .runner import TradingRunner, command_plan, holdings_snapshot, request_runner_stop, runner_status


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fangde", description="Fangde trading automation for Linux")
    parser.add_argument("--config", help="configuration JSON (or set FANGDE_CONFIG)")
    parser.add_argument("--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("init-config", "validate-config", "render-service"):
        sub.add_parser(command, help="local setup: " + command.replace("-", " "))
    inspect = sub.add_parser("session-inspect", help="show export identity without credentials")
    inspect.add_argument("source", type=Path)
    install = sub.add_parser("session-import", help="install an export matching this server's account")
    install.add_argument("source", type=Path)
    authenticate = sub.add_parser('session-authenticate',help='restore password login, unlock trading, and verify holdings')
    authenticate.add_argument('--retry',action='store_true',help='explicitly retry a previously failed password attempt')
    sub.add_parser("holdings", help="query cash, positions and today's order statuses")
    sub.add_parser("today-orders", help="get today's US orders")
    sub.add_parser("orders", help="alias for today-orders")
    sub.add_parser("price-books-refresh", help="read Kafka once to refresh the local price-book cache")
    reconcile = sub.add_parser("reconcile-order", help="link an uncertain command to an operator-verified broker order")
    reconcile.add_argument("--command-id", required=True)
    reconcile.add_argument("--order-id", required=True)
    history = sub.add_parser("history-orders", help="get all pages of orders in a date range")
    history.add_argument("--from", dest="date_from", required=True)
    history.add_argument("--to", dest="date_to", required=True)

    market = sub.add_parser("market-order", help="place or safely stage a market order")
    market.add_argument("--symbol", required=True)
    market.add_argument("--quantity", required=True)
    market.add_argument("--side", required=True, choices=("Buy", "Sell", "BUY", "SELL", "buy", "sell"))
    market.add_argument("--execute", action="store_true", help="submit this order; default is preview")
    market.add_argument("--dry-run", action="store_true", help="preview the ticket without submitting")
    market.add_argument("--no-submit", action="store_true", help="alias for preview")

    limit = sub.add_parser("limit-order", help="place or safely stage a limit order")
    limit.add_argument("--symbol", required=True)
    limit.add_argument("--price", required=True)
    limit.add_argument("--quantity", required=True)
    limit.add_argument("--side", required=True, choices=("Buy", "Sell", "BUY", "SELL", "buy", "sell"))
    limit.add_argument("--kill", action="store_true", help="cancel any unfilled remainder after result")
    limit.add_argument("--execute", action="store_true", help="submit this order; default is preview")
    limit.add_argument("--dry-run", action="store_true", help="preview the ticket without submitting")
    limit.add_argument("--no-submit", action="store_true", help="alias for preview")

    cancel = sub.add_parser("cancel-all", help="cancel all detected in-progress orders")
    cancel.add_argument("--execute", action="store_true", help="send cancellations; default is preview")
    cancel.add_argument("--dry-run", action="store_true", help="report count without canceling")

    single = sub.add_parser("cancel-order", help="cancel one exact Fangde order number")
    single.add_argument("order_id")
    single.add_argument("--execute", action="store_true")
    single.add_argument("--dry-run", action="store_true")

    sub.add_parser("runner", help="run Kafka consumer and periodic holdings publisher in foreground")
    sub.add_parser("runner-status", help="report runner PID state")
    stop = sub.add_parser("runner-stop", help="request a graceful runner stop")
    stop.add_argument("--force", action="store_true", help="also send SIGTERM after validating the PID")

    monitor = sub.add_parser("monitor", help="run the local command center in the foreground")
    monitor.add_argument("--host")
    monitor.add_argument("--port", type=int)
    monitor.add_argument("--redirect-http-port", type=int)
    monitor.add_argument("--interval", type=int)
    monitor.add_argument("--cert-file")
    monitor.add_argument("--key-file")
    monitor_start = sub.add_parser("monitor-start", help="start the local command center in the background")
    monitor_start.add_argument("--host")
    monitor_start.add_argument("--port", type=int)
    monitor_start.add_argument("--redirect-http-port", type=int)
    monitor_start.add_argument("--interval", type=int)
    monitor_start.add_argument("--cert-file")
    monitor_start.add_argument("--key-file")
    monitor_start.add_argument("--open-browser", action="store_true")
    sub.add_parser("monitor-status", help="report command-center PID state")
    sub.add_parser("monitor-stop", help="stop the local command center")
    sub.add_parser("self-test", help="run dependency-free routing and conversion checks")
    return parser


def _json(value) -> None:
    print(json.dumps(value, indent=2, sort_keys=False))


def _locked(settings, *, device=False):
    if not device and execution_backend(settings) == "api":
        return nullcontext()
    return EmulatorLock(
        settings.work_dir / "emulator.lock",
        float(settings.get("automation.lockTimeoutSeconds", 300)),
    )


def _automation(settings, kind):
    execution_backend(settings)
    from .api_trading import ApiTrading
    return ApiTrading(settings)


def _self_test(settings) -> dict:
    market = command_plan({"type": "MARKET_ORDER", "symbol": "bili", "side": "BUY", "qty_shares": 2})
    limit = command_plan({
        "type": "LIMIT_ORDER", "symbol": "BILI", "side": "SELL", "qty_shares": 3,
        "limit_price": 17.38,
    })
    fok = command_plan({
        "type": "LIMIT_ORDER_FOK", "symbol": "BILI", "side": "BUY", "qty_shares": 4,
        "limit_price": 17.4,
    })
    try:
        command_plan({"type": "MARKET_ORDER", "symbol": "BILI", "side": "BUY", "notional_usd": 100})
        notional_guard = False
    except ValueError:
        notional_guard = True
    old_account = settings.data["fangde"].get("accountId")
    old_num = settings.data["fangde"].get("accountNumId")
    settings.data["fangde"]["accountId"] = "FANGDE-A"
    settings.data["fangde"]["accountNumId"] = 7
    try:
        snapshot = holdings_snapshot({
            "queriedAt": "2026-07-10T12:00:00Z",
            "cash": {"balances": [{"currency": "USD", "balance": "$1,234.50"}]},
            "positions": [{
                "symbol": "bili", "quantity": "12.5", "cost": "$17.25",
                "marketPrice": "$18.50", "marketValue": "$231.25",
                "dailyPL": "$15.00", "dailyPLPercent": "6.94%",
            }],
        }, settings, True)
    finally:
        settings.data["fangde"]["accountId"] = old_account
        settings.data["fangde"]["accountNumId"] = old_num
    checks = {
        "marketRouting": market.kind == "market" and market.arguments["quantity"] == "2",
        "limitRouting": limit.kind == "limit" and not limit.arguments["kill"],
        "fokRouting": fok.kind == "limit" and fok.arguments["kill"],
        "notionalGuard": notional_guard,
        "snapshot": snapshot["cash"] == 1234.5 and snapshot["positions"][0]["dailyPLPercent"] == 6.94,
    }
    if not all(checks.values()):
        raise RuntimeError(f"Self-test failed: {checks}")
    return {"ok": True, "checks": checks}


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if arguments.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    try:
        if arguments.command in {"init-config", "validate-config", "render-service"}:
            from .setup import main as setup_main
            return setup_main([*(["--config", arguments.config] if arguments.config else []), arguments.command])
        allow_missing = arguments.command in {
            "self-test", "session-inspect", "runner-status", "runner-stop",
            "monitor-status", "monitor-stop",
        }
        settings = load_settings(arguments.config, allow_missing=allow_missing)
        logging.getLogger().setLevel(logging.DEBUG if arguments.verbose else settings.get("logging.level", "INFO"))
        if arguments.command == "session-inspect":
            from .session_import import inspect_export
            _json(inspect_export(arguments.source))
        elif arguments.command == "session-import":
            from .session_import import import_export
            _json(import_export(settings, arguments.source))
        elif arguments.command == 'session-authenticate':
            from .api import ApiClient
            with EmulatorLock(settings.work_dir/'api/operation.lock',300):
                _json(ApiClient(settings).authenticate(retry=arguments.retry))
        elif arguments.command == "self-test":
            _json(_self_test(settings))
        elif arguments.command in {"orders", "today-orders"}:
            _json(_automation(settings, "orders").today_orders())
        elif arguments.command == "price-books-refresh":
            from .price_books import KafkaPriceBooks
            books = KafkaPriceBooks(settings)
            try:
                result = books.refresh()
                _json({"ok": True, "symbols": len(result["books"])})
            finally:
                books.close()
        elif arguments.command == "reconcile-order":
            _json(_automation(settings, "orders").reconcile_order(arguments.command_id, arguments.order_id))
        elif arguments.command == "history-orders":
            _json(_automation(settings, "orders").history_orders(arguments.date_from, arguments.date_to))
        elif arguments.command == "cancel-order":
            if arguments.execute and arguments.dry_run:
                raise ValueError("--execute and --dry-run are mutually exclusive")
            _json(_automation(settings, "cancel").cancel_order(arguments.order_id, dry_run=not arguments.execute))
        elif arguments.command == "holdings":
            with _locked(settings):
                _json(_automation(settings, "holdings").query())
        elif arguments.command == "market-order":
            if arguments.execute and (arguments.dry_run or arguments.no_submit):
                raise ValueError("--execute cannot be combined with a preview flag")
            with _locked(settings):
                _json(_automation(settings, "orders").market_order(
                    arguments.symbol, arguments.quantity, arguments.side,
                    dry_run=not arguments.execute, no_submit=arguments.no_submit,
                ))
        elif arguments.command == "limit-order":
            if arguments.execute and (arguments.dry_run or arguments.no_submit):
                raise ValueError("--execute cannot be combined with a preview flag")
            with _locked(settings):
                _json(_automation(settings, "orders").limit_order(
                    arguments.symbol, arguments.price, arguments.quantity, arguments.side,
                    dry_run=not arguments.execute, no_submit=arguments.no_submit, kill=arguments.kill,
                ))
        elif arguments.command == "cancel-all":
            if arguments.execute and arguments.dry_run:
                raise ValueError("--execute and --dry-run are mutually exclusive")
            with _locked(settings):
                _json(_automation(settings, "cancel").cancel_all(dry_run=not arguments.execute))
        elif arguments.command == "runner":
            TradingRunner(settings).run()
        elif arguments.command == "runner-status":
            _json(runner_status(settings))
        elif arguments.command == "runner-stop":
            _json(request_runner_stop(settings, force=arguments.force))
        elif arguments.command == "monitor":
            forwarded = ["--config", str(settings.path)]
            for option, value in (
                ("--host", arguments.host), ("--port", arguments.port),
                ("--redirect-http-port", arguments.redirect_http_port),
                ("--interval", arguments.interval), ("--cert-file", arguments.cert_file),
                ("--key-file", arguments.key_file),
            ):
                if value is not None:
                    forwarded.extend((option, str(value)))
            return monitor_main(forwarded)
        elif arguments.command == "monitor-start":
            _json(start_monitor(
                settings, host=arguments.host, port=arguments.port,
                redirect_http_port=arguments.redirect_http_port,
                interval=arguments.interval, cert_file=arguments.cert_file,
                key_file=arguments.key_file, open_browser=arguments.open_browser,
            ))
        elif arguments.command == "monitor-status":
            _json(monitor_status(settings))
        elif arguments.command == "monitor-stop":
            _json(stop_monitor(settings))
        return 0
    except (FangdeError, ValueError, RuntimeError) as exc:
        logging.getLogger("fangde").error("%s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
