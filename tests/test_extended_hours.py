"""Kafka command -> workflow -> signed Fangde form, using synthetic HTTP only."""
import copy
import json
from datetime import datetime
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import pytest

from fangde.api import AuthenticationRequired, TradingApiClient, save_session
from fangde.broker import ApiTrading, market_session
from fangde.config import DEFAULTS, Settings
from fangde.diagnostics import command_context, command_source_context
from fangde.price_books import KafkaPriceBooks
from fangde.runner import TradingRunner, command_plan
from tests.test_api_trading import Broker, raw_order
from tests.test_authentication import ACCOUNTS, ADD, PORTFOLIO, UNLOCK_PATH, Sequence, ok, state, unlock


@pytest.fixture
def setup_order(tmp_path):
    def setup(now, *, side="SELL", kind="MARKET_ORDER", failure=None):
        credentials = state()
        data = copy.deepcopy(DEFAULTS)
        data["fangde"]["accountId"] = "TEST"
        data["api"].update(subjectSha256=credentials["identity"]["subject_sha256"],
                           orderActionDelaySeconds=0)
        settings = Settings(data, tmp_path / "config.json", tmp_path)
        save_session(settings.session_path, credentials)
        command = {"account_id": "TEST", "command_id": "extended-hours-test",
                   "ts": now.isoformat(), "type": kind, "symbol": "AEHL", "side": side,
                   "qty_shares": 100, "limit_price": "7.30"}
        # Reconstruct the observed AEHL outcome ($7.30), not unseen historical levels.
        books = KafkaPriceBooks(settings, clock=lambda: now.timestamp())
        stamp = int((now.timestamp() - 10) * 1e9)
        book = {"symbol": "AEHL", "update_time": stamp,
                "bid_side": {"update_time": stamp, "levels": [{"prc": "7.30", "total_qty": 119}]},
                "offer_side": {"update_time": stamp, "levels": [{"prc": "7.319", "total_qty": 119}]}}
        save_session(books.cache_path, {"version": 1, "source": books._source(),
                     "books": {"AEHL": {"book": book, "partition": 0, "offset": 456}}})
        sequence = [("/app/setting/global", ok(Broker().read("settings").payload["data"])),
                    ("/quotes/stock/detail", ok({"exchange": "US", "symbol": "AEHL"}))]
        if side == "SELL":
            sequence += [(PORTFOLIO, ok({"products": [{"exchange": "US", "symbol": "AEHL",
                                                      "qty": "100", "avail_sell_qty": "100"}]})),
                         ("/trade/index/todayOrder", ok({"list": []}))]
        sequence += [(ACCOUNTS, ok()),
                     (UNLOCK_PATH, {"status": 1102} if failure == "unlock" else unlock())]
        if failure != "unlock":
            sequence += [(ADD, {"status": 1102} if failure == "submit" else ok({"order_no": "test-order"}))]
        if failure is None:
            sequence += [("/trade/index/orderDetail", ok(raw_order(
                order_no="test-order", symbol="AEHL", qty="100", exec_qty="100",
                bs_flag="S" if side == "SELL" else "B", order_status="8", order_status_desc="全部成交",
                price="7.32" if side == "BUY" and kind == "MARKET_ORDER" else "7.30")))]
        opener = Sequence(*sequence)
        trading = ApiTrading(settings, client=TradingApiClient(settings, opener=opener),
                             price_books=books, sleeper=lambda _: None)
        runner = TradingRunner(settings)
        runner.api_executor = trading
        return command, runner, trading, opener
    return setup


def execute(command, runner, now):
    token = command_context.set(command["command_id"])
    source_token = command_source_context.set({"topic": "trading-commands", "partition": 0, "offset": 123})
    try:
        plan = command_plan(command)
        with patch("fangde.broker.datetime", wraps=datetime) as clock:
            clock.now.return_value = now
            return runner._operation(plan.kind, plan.arguments)
    finally:
        command_source_context.reset(source_token)
        command_context.reset(token)


@pytest.mark.parametrize("stamp,session", [
    ("2026-10-28T12:25:43+00:00", "premarket"),
    ("2026-10-28T22:25:43+00:00", "afterhours"),
    ("2026-11-02T13:25:43+00:00", "premarket"),
    ("2026-11-02T23:25:43+00:00", "afterhours"),
])
@pytest.mark.parametrize("side", ["BUY", "SELL"])
@pytest.mark.parametrize("kind", ["MARKET_ORDER", "LIMIT_ORDER", "LIMIT_ORDER_FOK"])
def test_extended_hours_wire_unlock_and_deduplication(setup_order, stamp, session, side, kind):
    now = datetime.fromisoformat(stamp)
    command, runner, trading, opener = setup_order(now, side=side, kind=kind)
    result = execute(command, runner, now)
    assert result["submitted"] is True
    assert result["pricing"]["effectiveOrderType"] == "LO"
    assert result["pricing"]["tradingSession"] == (session if kind == "MARKET_ORDER" else "limit")
    requests = [request for request in opener.calls if urlsplit(request.full_url).path == ADD]
    assert len(requests) == 1
    fields = parse_qs(requests[0].data.decode())
    assert fields["order_type"] == ["PL"]
    assert fields["extended_trading"] == ["1"]
    assert fields["duration_type"] == ["DayOrder"]
    assert fields["qty"] == ["100"]
    assert fields["bs_flag"] == ["B" if side == "BUY" else "S"]
    assert fields["price"] == ["7.32" if side == "BUY" and kind == "MARKET_ORDER" else "7.30"]
    assert fields["tradeToken"] == ["new-trade"]
    paths = [urlsplit(request.full_url).path for request in opener.calls]
    assert paths[paths.index(ADD) - 2:paths.index(ADD)] == [ACCOUNTS, UNLOCK_PATH]
    record = trading.journal.read(command["command_id"])
    assert record["source"] == {"topic": "trading-commands", "partition": 0, "offset": 123}
    assert record["pricing"]["priceBookOffset"] == 456
    assert execute(command, runner, now)["deduplicated"] is True
    assert not opener.items


@pytest.mark.parametrize("failure,stage,dispatched", [("unlock", "unlock_trading", False), ("submit", "submit", True)])
def test_premarket_auth_failure_records_stage_and_does_not_replay(setup_order, caplog, failure, stage, dispatched):
    now = datetime.fromisoformat("2026-10-28T12:25:43+00:00")
    command, runner, trading, opener = setup_order(now, failure=failure)
    with pytest.raises(AuthenticationRequired):
        execute(command, runner, now)
    record = trading.journal.read(command["command_id"])
    assert bool(record.get("sent_at")) is dispatched
    assert record["phase"] == ("rejected" if dispatched else "prepared")
    assert record["authentication_error"]["api_code"] == 1102
    assert record["authentication_error"]["endpoint"] == (ADD if dispatched else UNLOCK_PATH)
    event = json.loads(next(record.getMessage().split("Workflow finished ", 1)[1]
                           for record in caplog.records if record.getMessage().startswith("Workflow finished ")))
    assert event["stage"] == stage
    assert event["pricing"]["tradingSession"] == "premarket"
    assert event["authentication"]["api_code"] == 1102
    assert "old-trade" not in caplog.text and "new-trade" not in caplog.text
    assert sum(urlsplit(request.full_url).path == ADD for request in opener.calls) == int(dispatched)
    if dispatched:
        with pytest.raises(ValueError, match="previously rejected"):
            execute(command, runner, now)
    assert not opener.items


@pytest.mark.parametrize("stamp,expected", [
    ("2026-10-28T07:59:59+00:00", None),
    ("2026-10-28T08:00:00+00:00", "premarket"),
    ("2026-10-28T13:29:59+00:00", "premarket"),
    ("2026-10-28T13:30:00+00:00", "regular"),
    ("2026-10-28T20:00:00+00:00", "afterhours"),
    ("2026-10-28T23:59:59+00:00", "afterhours"),
    ("2026-10-29T00:00:00+00:00", None),
    ("2026-11-02T08:59:59+00:00", None),
    ("2026-11-02T09:00:00+00:00", "premarket"),
    ("2026-11-02T14:30:00+00:00", "regular"),
    ("2026-11-02T21:00:00+00:00", "afterhours"),
    ("2026-11-03T01:00:00+00:00", None),
    ("2026-11-27T18:00:00+00:00", "afterhours"),
    ("2026-11-27T21:59:59+00:00", "afterhours"),
    ("2026-11-27T22:00:00+00:00", None),
    ("2026-11-26T13:00:00+00:00", None),
    ("2026-11-01T13:00:00+00:00", None),
])
def test_session_boundaries_dst_holidays_and_early_close(stamp, expected):
    now = datetime.fromisoformat(stamp)
    if expected is None:
        with pytest.raises(ValueError):
            market_session(now)
    else:
        assert market_session(now) == expected


@pytest.mark.parametrize("blocked_by", ["stale_book", "disabled_conversion", "overnight"])
def test_extended_market_guards_block_before_authentication_or_submission(setup_order, blocked_by):
    now = datetime.fromisoformat("2026-10-28T12:25:43+00:00")
    command, runner, trading, opener = setup_order(now)
    if blocked_by == "stale_book":
        trading.price_books.clock = lambda: now.timestamp() + 301
    elif blocked_by == "disabled_conversion":
        trading.settings.data["api"]["extendedHoursMarketOrders"] = False
    else:
        now = datetime.fromisoformat("2026-10-29T00:00:00+00:00")
    with pytest.raises(ValueError):
        execute(command, runner, now)
    assert all(urlsplit(request.full_url).path not in (ACCOUNTS, UNLOCK_PATH, ADD) for request in opener.calls)
    assert trading.journal.read(command["command_id"]) is None
