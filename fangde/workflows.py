"""Direct order workflows. Financial requests are never retried blindly."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
import hashlib
import json
import logging
from pathlib import Path
import time
import uuid

from .api import AcceptedWithoutOrderNumber, ApiError, SessionError, TransportError, AuthenticationRequired, TradingApiClient, save_session
from .diagnostics import command_context, command_source_context, logged_workflow, workflow_attempt, workflow_retry, workflow_step
from .lock import EmulatorLock
from .orders import OrderAutomation

LOG = logging.getLogger("fangde")
TERMINAL = {"FilledStatus", "CanceledStatus", "PartialWithdrawal", "RejectedStatus", "ExpiredStatus"}
ACTIVE = {"NotReported", "ReplacedNotReported", "ProtectedNotReported", "VarietiesNotReported",
          "WaitToNew", "NewStatus", "WaitToReplace", "PendingReplaceStatus", "ReplacedStatus",
          "PartialFilledStatus", "WaitToCancel", "PendingCancelStatus"}


class OrderUncertain(RuntimeError):
    def __init__(self, message, *, submitted=False):
        self.submitted = submitted
        super().__init__(message)


class OrderFailed(RuntimeError):
    """A submitted order has a confirmed terminal failure at the broker."""
    def __init__(self, status):
        self.status = status
        self.submitted = True
        super().__init__(f"Broker order ended with {status}.")


def decimal(value, name="number") -> Decimal:
    try:
        number = Decimal(str(value))
        if not number.is_finite():
            raise InvalidOperation
        return number
    except (ValueError, InvalidOperation):
        raise ValueError(f"API returned an invalid {name}.") from None


def order_id(order: dict) -> str:
    value = order.get("order_id_str") or order.get("id")
    if not isinstance(value, str) or not value or len(value) > 100 or any(ord(c) < 33 or ord(c) > 126 for c in value):
        raise ValueError("API response has no exact order ID.")
    return value




class OrderJournal:
    """Durable per-command intent, acknowledgment and outcome, under a shared lock."""
    def __init__(self, directory: Path, account: str):
        self.directory = directory
        self.account = account
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)

    def path(self, key: str) -> Path:
        digest = hashlib.sha256((self.account + "\0" + key).encode()).hexdigest()
        return self.directory / (digest + ".json")

    def read(self, key: str):
        path = self.path(key)
        if not path.exists():
            return None
        try:
            record = json.loads(path.read_text())
            if not isinstance(record, dict) or record.get("account") != self.account or record.get("command_id") != key:
                raise ValueError
            return record
        except (ValueError, OSError):
            raise OrderUncertain("Order journal is unreadable; inspect it before trading.") from None

    def save(self, key: str, record: dict):
        record["updated_at"] = time.time()
        save_session(self.path(key), record)

    def records(self):
        for path in sorted(self.directory.glob("*.json")):
            try:
                record = json.loads(path.read_text())
                if not isinstance(record, dict):
                    raise ValueError
            except (ValueError, OSError):
                raise OrderUncertain("Order journal is unreadable; inspect it before trading.") from None
            if record.get("account") == self.account:
                yield record


class WorkflowBase:
    def __init__(self, settings, *, client=None, price_books=None, sleeper=time.sleep):
        from .price_books import KafkaPriceBooks
        self.settings = settings
        self.client = client or TradingApiClient.from_settings(settings)
        self.price_books = price_books or KafkaPriceBooks(settings)
        self.sleeper = sleeper
        self.directory = settings.work_dir / "api"
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.journal = OrderJournal(self.directory / "orders", str(settings.require("fangde.accountId")))
        self.on_order_observed = None

    def _observe_order(self, row, command_id=None):
        # Persist a confirmed state before returning from the workflow. The
        # reporting worker can send it immediately, even while cancellation waits.
        from .order_status import OrderStatusPublisher, order_snapshot
        snapshot = order_snapshot(row, self.settings, observed_at=datetime.now(timezone.utc),
                                  command_id=command_id)
        OrderStatusPublisher(self.settings).stage([snapshot])
        if self.on_order_observed:
            self.on_order_observed()

    def _lock(self):
        return EmulatorLock(self.directory / "operation.lock", float(self.settings.get("automation.lockTimeoutSeconds", 300)))

    def _order_action_delay(self):
        # Hold the operation lock while waiting so concurrent commands accumulate
        # their own delays. Read current ticket/account data only after the wait.
        delay = float(self.settings.get("api.orderActionDelaySeconds", 0))
        workflow_step("order_action_delay", delaySeconds=delay)
        self.sleeper(delay)

    def _read(self, endpoint, params=None):
        attempts = int(self.settings.get("api.readAttempts", 2))
        for attempt in range(1, attempts + 1):
            try:
                return self.client.read(endpoint, params).payload["data"]
            except (TransportError, ApiError) as error:
                if attempt == attempts or (isinstance(error, ApiError) and error.result.http_status < 500):
                    raise
                workflow_retry(error)
                workflow_attempt(attempt + 1)
                workflow_step("read_" + endpoint)
                self.sleeper(float(self.settings.get("api.readRetryDelaySeconds", 0.25)))
        raise AssertionError("unreachable")







    def _guard_submission(self):
        if any(row.get("sent_at") and row.get("phase") not in {"finished", "rejected"}
               for row in self.journal.records()):
            raise OrderUncertain("A previous submission is unresolved; reconcile the order journal before placing another order.")



    @staticmethod
    def _matches_ticket(row, request):
        return (row.get("counter_id") == request["counter_id"] and str(row.get("action")) == request["action"]
                and decimal(row["quantity"]) == decimal(request["quantity"])
                and row.get("order_type") == request["order_type"]
                and (request["order_type"] != "LO" or decimal(row["price"]) == decimal(request["price"])))

    def _accepted_sell_candidate(self, record):
        # This read-only fallback is limited to a successful ordinary limit sell.
        # Transport timeouts and orders requesting automatic cancellation retain the manual hold.
        if (record.get("phase") != "accepted_without_id" or "before_order_ids" not in record
                or record["intent"].get("kill") or record["request"]["action"] != "2"
                or record["request"]["order_type"] != "LO"):
            return None
        sent, ack = record["sent_at"], record["acknowledged_at"]
        if not 0 <= ack - sent <= 5:
            return None
        existing = set(record["before_order_ids"])
        candidates = []
        for row in self._orders():
            if row["counter_id"] != record["request"]["counter_id"] or order_id(row) in existing:
                continue
            created = datetime.fromisoformat(row["submitted_at"]).timestamp()
            if int(sent) <= created <= ack:
                candidates.append(row)
        if len(candidates) != 1 or not self._matches_ticket(candidates[0], record["request"]):
            return None
        identifier = order_id(candidates[0])
        if any(r.get("intent") and r.get("order_id") == identifier for r in self.journal.records()):
            return None
        return candidates[0]

    def _reconcile(self, record, key):
        workflow_step("reconcile_submission")
        identifier = record.get("order_id")
        candidate = None
        if not identifier:
            candidate = self._accepted_sell_candidate(record)
            if candidate is None:
                raise OrderUncertain("No unambiguous broker order number; reconcile manually. No request was resent.",
                                     submitted=bool(record.get("sent_at")))
            identifier = order_id(candidate)
        row = self._detail(identifier)
        if (not self._matches_ticket(row, record["request"])
                or (candidate and candidate["submitted_at"] != row["submitted_at"])):
            raise OrderUncertain("Acknowledged order does not match the intended ticket.", submitted=True)
        if candidate:
            record.update(order_id=identifier, phase="accepted", correlation="confirmed_submit_window")
            self.journal.save(key, record)
        self._observe_order(row, key)
        return row

    def _cancel_one(self, identifier, record, key):
        workflow_step("confirm_cancel", orderId=identifier)
        row = self._detail(identifier)
        self._observe_order(row, key if record.get("intent") else None)
        if row["status"] in TERMINAL:
            return row
        cancellation_sent = record.get("cancel_sent_at") or any(
            prior.get("order_id") == identifier and prior.get("cancel_sent_at") for prior in self.journal.records())
        if not cancellation_sent:
            if not row.get("cancelable"):
                raise OrderUncertain("Broker has not marked this order cancelable; no cancellation sent.")
            body = {"order_id": identifier, "order_id_str": identifier}
            def sent():
                record["cancel_sent_at"] = time.time()
                self.journal.save(key, record)
            try:
                self.client.write("withdraw", body, before_send=sent)
            except (ApiError, SessionError):
                workflow_retry("Cancel response was not confirmed; checking its order status")
        # A cancel acknowledgment is not a terminal status. Never treat absence of a
        # Cancel button or a timeout as successful cancellation.
        for delay in self.settings.get("api.cancelPollDelaysSeconds", [0, 1, 1, 2]):
            if delay:
                self.sleeper(delay)
            row = self._detail(identifier)
            self._observe_order(row, key if record.get("intent") else None)
            if row["status"] in TERMINAL:
                return row
        raise OrderUncertain("Cancellation is still unconfirmed; no additional cancel was sent.", submitted=bool(record.get("request")))

    def _order(self, symbol, quantity, side, price=None, *, kill=False, dry_run=False, no_submit=False):
        symbol = OrderAutomation.normalize_symbol(symbol)
        side = OrderAutomation.normalize_side(side)
        quantity = OrderAutomation.positive_number(quantity, "quantity")
        price = OrderAutomation.positive_number(price, "price") if price is not None else None
        intent = {"symbol": symbol, "side": side, "quantity": quantity, "price": price, "kill": kill}
        fingerprint = hashlib.sha256(json.dumps(intent, sort_keys=True).encode()).hexdigest()
        key = command_context.get() or "cli:" + uuid.uuid4().hex
        with self._lock():
            workflow_attempt(1)
            if dry_run or no_submit:
                workflow_step("prepare_ticket")
                body, pricing, _ = self._prepare(symbol, quantity, side, price)
                return {"ok": True, "dryRun": dry_run, "submitted": False, "kill": kill,
                        "ticket": {k: v for k, v in body.items() if k != "card_ids"}, "pricing": pricing}
            self._order_action_delay()
            record = self.journal.read(key)
            if record:
                if record.get("fingerprint") != fingerprint:
                    raise ValueError("Command ID was already used for a different order.")
                if record.get("result"):
                    return {**record["result"], "deduplicated": True}
                if record["phase"] == "rejected":
                    raise ValueError("This command was previously rejected; use a new command ID for a new intent.")
            if not record or record["phase"] == "prepared":
                workflow_step("prepare_ticket")
                body, pricing, previous_ids = self._prepare(symbol, quantity, side, price)
                # Local correlation only; Fangde has no verified request-ID lookup.
                previous = [int(row["request"]["client_request_id"]) for row in self.journal.records() if row.get("request")]
                request_id = str(max([time.time_ns() // 1_000_000, *[x + 1 for x in previous]]))
                body["client_request_id"] = request_id
                record = {"account": self.journal.account, "command_id": key, "fingerprint": fingerprint,
                          "intent": intent, "request": body, "pricing": pricing, "phase": "prepared"}
                if previous_ids is not None and price is not None and not kill:
                    record["before_order_ids"] = previous_ids
                if command_source_context.get() is not None:
                    record["source"] = dict(command_source_context.get())
                self.journal.save(key, record)
                def sent():
                    record.update(phase="dispatched", sent_at=time.time())
                    self.journal.save(key, record)
                workflow_step("submit", pricing=pricing)
                try:
                    response = self.client.write("submit", body, before_send=sent)
                    record.update(order_id=order_id(response), phase="accepted")
                    self.journal.save(key, record)
                except AcceptedWithoutOrderNumber as error:
                    record.update(phase="accepted_without_id", acknowledged_at=time.time(), ack_detail=str(error))
                    self.journal.save(key, record)
                except ApiError as error:
                    # A server-side failure can be ambiguous even with an HTTP reply.
                    record["phase"] = "rejected" if error.result.http_status < 500 and error.result.api_code is not None else "ambiguous"
                    self.journal.save(key, record)
                    if record["phase"] == "rejected":
                        raise
                except AuthenticationRequired as error:
                    record["authentication_error"] = {"message": str(error), **error.diagnostics()}
                    if record.get("sent_at"):
                        record["phase"] = "rejected"
                    self.journal.save(key, record)
                    raise
                except (SessionError, ValueError, KeyError, OSError) as error:
                    record["failure_type"] = type(error).__name__
                    if record["phase"] == "prepared":
                        raise
                    record["phase"] = "ambiguous"
                    try:
                        self.journal.save(key, record)
                    except (SessionError, OSError) as error:
                        raise OrderUncertain("Submission journal could not be saved; inspect broker orders before continuing.",
                                             submitted=bool(record.get("order_id"))) from error
            if record["phase"] == "ambiguous" or record["phase"] == "dispatched":
                workflow_retry("Submission response is ambiguous; reconciling without resubmission")
                workflow_attempt(2)
            workflow_step("confirm_result")
            try:
                row = self._reconcile(record, key)
                if kill and row["status"] not in TERMINAL:
                    workflow_step("fok_cancel")
                    self.sleeper(float(self.settings.get("api.fokWaitSeconds", 2)))
                    row = self._cancel_one(record["order_id"], record, key)
                if row["status"] in {"RejectedStatus", "ExpiredStatus"}:
                    record.update(phase="rejected", broker_status=row["status"])
                    self.journal.save(key, record)
                    raise OrderFailed(row["status"])
                result = {"ok": True, "submitted": True, "kill": kill, "result": "api_order",
                          "status": row["status"], "filledPrice": row.get("executed_price", ""),
                          "filledQty": row.get("executed_qty", "0"), "orderId": record["order_id"]}
                if record.get("pricing"):
                    result["pricing"] = record["pricing"]
                record.update(phase="finished", result=result)
                self.journal.save(key, record)
                return result
            except AuthenticationRequired:
                raise
            except (SessionError, ApiError, ValueError, OSError, KeyError) as error:
                raise OrderUncertain("Order was acknowledged but confirmation failed; inspect its recorded order ID.",
                                     submitted=bool(record.get("order_id"))) from error

    def recover_pending(self):
        """Reconcile interrupted commands before the runner accepts new work.

        Existing requests are only looked up. An acknowledged synthetic FOK may
        still need its one cancellation request after an interrupted process.
        """
        with self._lock():
            records = list(self.journal.records())
        recovered = 0
        for record in records:
            if not record.get("sent_at") or record.get("phase") in {"finished", "rejected"}:
                continue
            intent = record["intent"]
            token = command_context.set(record["command_id"])
            try:
                try:
                    self._order(intent["symbol"], intent["quantity"], intent["side"], intent["price"], kill=intent["kill"])
                except OrderFailed as error:
                    # The exact ticket was reconciled and its terminal failure
                    # saved. It cannot fill later or block startup indefinitely.
                    LOG.warning("api_recovery_terminal_failure command_id=%s status=%s", record["command_id"], error.status)
                recovered += 1
            finally:
                command_context.reset(token)
        LOG.info("api_recovery_finished recovered_commands=%d", recovered)
        return {"ok": True, "recovered": recovered}

    @logged_workflow
    def market_order(self, symbol, quantity, side, *, dry_run=False, no_submit=False):
        return self._order(symbol, quantity, side, dry_run=dry_run, no_submit=no_submit)

    @logged_workflow
    def limit_order(self, symbol, price, quantity, side, *, kill=False, dry_run=False, no_submit=False):
        return self._order(symbol, quantity, side, price, kill=kill, dry_run=dry_run, no_submit=no_submit)

    @logged_workflow
    def cancel_all(self, *, dry_run=False):
        with self._lock():
            workflow_attempt(1)
            if not dry_run:
                self._order_action_delay()
            rows = self._orders()
            if any(row.get("status") not in ACTIVE | TERMINAL for row in rows):
                raise ValueError("An unknown order status prevents cancel-all.")
            pending = [row for row in rows if row["status"] in ACTIVE and row.get("cancelable")]
            if dry_run:
                return {"dryRun": True, "canceledCount": 0, "remainingDetected": len(pending), "canceledOrders": []}
            maximum = int(self.settings.get("automation.maxCancelCount", 50))
            canceled = []
            for row in pending[:maximum]:
                identifier = order_id(row)
                key = "cancel:" + identifier
                record = self.journal.read(key) or {"account": self.journal.account, "command_id": key,
                                                   "phase": "cancel", "order_id": identifier}
                final = self._cancel_one(identifier, record, key)
                record.update(phase="finished", status=final["status"])
                self.journal.save(key, record)
                if final["status"] in {"CanceledStatus", "PartialWithdrawal"}:
                    canceled.append({"orderId": identifier, "status": final["status"]})
            remaining = [row for row in self._orders() if row["status"] not in TERMINAL]
            if remaining:
                raise OrderUncertain("Cancel-all finished with open orders remaining.")
            return {"completedAt": datetime.now(timezone.utc).isoformat(), "dryRun": False,
                    "canceledCount": len(canceled), "remainingDetected": 0, "canceledOrders": canceled}
