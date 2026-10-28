"""Fangde broker adapter for the shared CLI, Kafka, journal and command center."""
from datetime import date, datetime, timezone
from decimal import Decimal
import logging
from zoneinfo import ZoneInfo
from .api import ApiError, AuthenticationRequired, TransportError, exact_positive
from .diagnostics import logged_workflow, workflow_attempt
from .workflows import (WorkflowBase, OrderUncertain, OrderFailed, OrderJournal, decimal,
                        order_id, ACTIVE, TERMINAL)
from .orders import OrderAutomation

LOG=logging.getLogger("fangde")
NORMAL_STATUS={"0":"NotReported","1":"WaitToNew","2":"NewStatus","3":"PendingCancelStatus",
    "4":"PendingCancelStatus","5":"PartialWithdrawal","6":"CanceledStatus",
    "7":"PartialFilledStatus","8":"FilledStatus","9":"RejectedStatus"}
DESCRIPTION_STATUS={"待触发":"NotReported","待觸發":"NotReported","已撤单":"CanceledStatus",
    "已撤單":"CanceledStatus","全部成交":"FilledStatus","已成交":"FilledStatus",
    "部成部撤":"PartialWithdrawal","部分成交":"PartialFilledStatus",
    "下单失败":"RejectedStatus","下單失敗":"RejectedStatus","已失效":"ExpiredStatus",
    "已过期":"ExpiredStatus","已過期":"ExpiredStatus"}

def broker_time(raw, field, settings):
    local=raw.get("local_"+field)
    zone=raw.get("local_timezone")
    if local and zone in ("美东","美東","America/New_York"):
        value,tz=local,ZoneInfo("America/New_York")
    else:
        value,tz=raw.get(field),ZoneInfo(settings.get("api.brokerTimezone","Asia/Hong_Kong"))
    if not isinstance(value,str) or not value:
        raise ValueError("Order has no broker timestamp.")
    try:
        parsed=datetime.fromisoformat(value)
        if parsed.tzinfo is None: parsed=parsed.replace(tzinfo=tz)
        return parsed.astimezone(timezone.utc).isoformat()
    except ValueError: raise ValueError("Invalid broker timestamp.") from None

def normalize_order(raw, settings):
    if not isinstance(raw,dict) or raw.get("exchange")!="US" or raw.get("asset_type") not in (None,"","Stock"):
        raise ValueError("Unsupported order asset or exchange.")
    identifier=raw.get("order_no")
    if not isinstance(identifier,str) or not identifier:
        raise ValueError("Order has no exact string order number.")
    strategy=str(raw.get("is_strategy","0"))=="1" or raw.get("order_type") not in ("PL","M","L")
    desc=str(raw.get("order_status_desc",""))
    status=DESCRIPTION_STATUS.get(desc)
    if status is None:
        status=NORMAL_STATUS.get(str(raw.get("order_status"))) if not strategy else None
    if status is None: status="UNKNOWN:"+str(raw.get("order_status"))+":"+desc
    side={"B":"1","S":"2"}.get(raw.get("bs_flag"))
    if side is None: raise ValueError("Order has an unsupported side.")
    result={"order_id_str":identifier,"counter_id":"ST/US/"+str(raw["symbol"]),
        "action":side,"order_type":{"PL":"LO","L":"LO","M":"MO"}.get(raw.get("order_type"),"OTHER"),
        "quantity":raw.get("qty"),"price":raw.get("price"),"executed_qty":raw.get("exec_qty"),
        "executed_price":raw.get("exec_price"),"status":status,
        "cancelable":str(raw.get("enable_cancel","0"))=="1",
        "submitted_at":broker_time(raw,"create_time",settings),
        "currency":raw.get("currency") or "USD","time_in_force":raw.get("tif"),
        "msg":desc,"remark":raw.get("reject_reason"),"broker_status":raw.get("order_status"),
        "broker_order_type":raw.get("order_type")}
    if raw.get("updated_time") or raw.get("local_updated_time"):
        result["updated_at"]=broker_time(raw,"updated_time",settings)
    return result

def market_session(now=None):
    """Calendar session; includes holiday, early-close and DST schedules."""
    import exchange_calendars as xcals
    now=now or datetime.now(timezone.utc)
    if now.tzinfo is None: raise ValueError("Market time must have a timezone.")
    local=now.astimezone(ZoneInfo("America/New_York"))
    calendar=xcals.get_calendar("XNYS")
    day=local.date().isoformat()
    if not calendar.is_session(day): raise ValueError("No US stock session today.")
    opening=calendar.session_open(day).to_pydatetime()
    closing=calendar.session_close(day).to_pydatetime()
    if opening<=now<closing: return "regular"
    if local.replace(hour=4,minute=0,second=0,microsecond=0)<=local<opening.astimezone(local.tzinfo):
        return "premarket"
    from datetime import timedelta
    extended_end=min(local.replace(hour=20,minute=0,second=0,microsecond=0), closing.astimezone(local.tzinfo)+timedelta(hours=4))
    if closing<=now<extended_end: return "afterhours"
    raise ValueError("US market is closed or overnight; market command was not queued.")

class ApiTrading(WorkflowBase):
    def reconcile_order(self, command_id, identifier):
        """Operator-supplied exact match after inspecting the broker's orders.

        Only reads the broker and updates local state. Runner recovery resumes
        confirmation (including an unfinished emulated FOK cancellation).
        """
        with self._lock():
            record = self.journal.read(command_id)
            if not record or not record.get('sent_at') or record.get('phase') in ('finished','rejected'):
                raise ValueError('No unresolved dispatched command with this command ID.')
            if record.get('order_id') and record['order_id'] != identifier:
                raise ValueError('Command already has a different broker order ID.')
            candidate = dict(record, order_id=identifier)
            row = self._reconcile(candidate, command_id)
            candidate['phase'] = 'accepted'
            self.journal.save(command_id, candidate)
            return {'ok':True,'orderId':identifier,'status':row['status'],'resubmitted':False}

    def _orders(self):
        data=self._read("orders")
        if not isinstance(data,dict) or not isinstance(data.get("list"),list):
            raise ValueError("Today's order list is missing.")
        rows=[normalize_order(row,self.settings) for row in data["list"] if row.get("exchange")=="US"]
        ids=[order_id(row) for row in rows]
        if len(ids)!=len(set(ids)): raise ValueError("Today's list repeated an order.")
        return rows

    def _detail(self, identifier):
        row=normalize_order(self._read("order_detail",{"order_no":identifier}),self.settings)
        if order_id(row)!=identifier: raise ValueError("Order detail returned a different order.")
        if row["status"] not in ACTIVE|TERMINAL:
            raise ValueError("Unrecognized broker order status; manual reconciliation is required.")
        return row

    def history_orders(self, start, end):
        try:
            if date.fromisoformat(start)>date.fromisoformat(end): raise ValueError()
        except (ValueError,TypeError): raise ValueError("Use YYYY-MM-DD dates with from <= to.") from None
        with self._lock():
            rows,seen=[],set()
            for page in range(1,int(self.settings.get("api.maxOrderPages",100))+1):
                data=self._read("order_history",{"from_trade_date":start,"to_trade_date":end,
                    "category":"0","symbol":"","page":str(page),"page_size":"50"})
                if not isinstance(data,dict) or not isinstance(data.get("list"),list):
                    raise ValueError("Malformed history page.")
                info=data.get("page_info",{})
                try:
                    total=int(info.get("page_sum",info.get("total_page")))
                    current=int(info.get("page",info.get("now_page")))
                except (ValueError,TypeError): raise ValueError("Missing history pagination metadata.") from None
                if current!=page or total<0 or (not data["list"] and total>page):
                    raise ValueError("Inconsistent history pagination.")
                for raw in data["list"]:
                    if raw.get("exchange")!="US": continue
                    row=normalize_order(raw,self.settings)
                    key=order_id(row)
                    if key in seen: raise ValueError("History pagination repeated an order.")
                    seen.add(key)
                    rows.append(row)
                if page>=total: return {"orders":rows,"from":start,"to":end}
            raise ValueError("History exceeds maxOrderPages; narrow the date range.")

    def today_orders(self):
        with self._lock(): return {"orders":self._orders()}

    @logged_workflow
    def query(self):
        with self._lock():
            workflow_attempt(1)
            data=self._read("positions")
            if not isinstance(data,dict) or not isinstance(data.get("products"),list) or not isinstance(data.get("cash"),list):
                raise ValueError("Fangde portfolio is incomplete.")
            positions=[]
            for row in data["products"]:
                if row.get("asset_type")!="Stock" or row.get("exchange")!="US":
                    raise ValueError("Non-US stock holdings need an explicit mapping before publishing.")
                qty=decimal(row.get("qty"))
                if qty==0: continue
                positions.append({"market":"US","symbol":row["symbol"],"name":row.get("name_en") or row.get("name",""),
                    "quantity":str(qty),"cost":str(decimal(row.get("avg_cost"))),
                    "marketPrice":str(decimal(row.get("last_price"))),"marketValue":str(decimal(row.get("market_value"))),
                    "dailyPL":str(decimal(row.get("today_pl"))),"dailyPLPercent":str(decimal(row.get("today_pl_percent")))})
            balances=[]
            for row in data["cash"]:
                currency=row.get("ccy")
                if currency not in ("USD","HKD","CNY","EUR","GBP","JPY"):
                    raise ValueError("Unrecognized cash currency.")
                balances.append({"currency":currency,"balance":str(decimal(row.get("amt")))})
            if not balances or len({row["currency"] for row in balances})!=len(balances):
                raise ValueError("Incomplete cash balances.")
            observed=datetime.now(timezone.utc)
            try: orders=self._orders()
            except AuthenticationRequired: raise
            except (ApiError,TransportError,ValueError):
                LOG.error("Today's orders query failed; holdings remain available, order statuses will retry.")
                orders,errors=[],1
            else: errors=0
            from .order_status import order_snapshot
            commands={r["order_id"]:r["command_id"] for r in self.journal.records() if r.get("order_id") and r.get("intent")}
            snapshots=[]
            for row in orders:
                try: snapshots.append(order_snapshot(row,self.settings,observed_at=observed,command_id=commands.get(order_id(row))))
                except (ValueError,TypeError,KeyError):
                    errors+=1
                    LOG.error("An order could not be mapped; no state was invented.")
            return {"queriedAt":observed.isoformat(),"cash":{"balances":balances},"positions":positions,
                    "orders":snapshots,"orderStatusErrors":errors}

    def _guard_sell(self,counter,quantity):
        symbol=counter.split("/")[-1]
        data=self._read("positions")
        rows=[row for row in data.get("products",[]) if row.get("exchange")=="US" and row.get("symbol")==symbol]
        if len(rows)!=1: raise ValueError("No unambiguous US long holding exists for this sell.")
        owned,available=decimal(rows[0].get("qty")),decimal(rows[0].get("avail_sell_qty"))
        pending=Decimal(0)
        for order in self._orders():
            if order["counter_id"]!=counter or order["action"]!="2": continue
            if order["status"] not in ACTIVE|TERMINAL: raise ValueError("Unknown sell status prevents an availability check.")
            if order["status"] in ACTIVE: pending+=max(Decimal(0),decimal(order["quantity"])-decimal(order["executed_qty"]))
        if decimal(quantity)>max(Decimal(0),min(available,owned-pending)):
            raise ValueError("Sell exceeds available long shares after pending sells.")

    def _prepare(self,symbol,quantity,side,price):
        self._guard_submission()
        quantity=exact_positive(quantity,"quantity")
        if decimal(quantity)!=decimal(quantity).to_integral_value(): raise ValueError("Whole shares are required.")
        broker_settings=self._read("settings").get("FDHongKong",{})
        types=[r for r in broker_settings.get("order_type",[]) if r.get("exchange")=="US"]
        durations=[r for r in broker_settings.get("duration_type",[]) if r.get("exchange")=="US"]
        if len(types)!=1 or len(durations)!=1: raise ValueError("Missing US trading settings.")
        supported={r["code"] for r in types[0].get("types",[])}
        if "DayOrder" not in {r["code"] for r in durations[0].get("types",[])}:
            raise ValueError("DAY orders unavailable.")
        security=self._read("security",{"symbol":symbol,"exchange":"US"})
        if security.get("symbol")!=symbol or security.get("exchange")!="US" or str(security.get("is_delisted","0"))=="1":
            raise ValueError("Security identity is unavailable or delisted.")
        counter="ST/US/"+symbol
        if side=="Sell": self._guard_sell(counter,quantity)
        session=market_session() if price is None else "limit"
        extended=price is not None or session!="regular"
        if price is None and extended and not self.settings.get("api.extendedHoursMarketOrders",True):
            raise ValueError("Extended-hours market conversion is disabled.")
        kind="LO" if extended else "MO"
        if ("PL" if kind=="LO" else "M") not in supported: raise ValueError("Requested order type unavailable.")
        # Fangde's app supplies a reference price even with market orders.
        # Use the existing fresh Kafka book; this is not a market-order cap.
        selected=self.price_books.price(symbol,side,quantity)
        reference=exact_positive(selected.price,"Kafka reference price")
        body={"counter_id":counter,"action":"1" if side=="Buy" else "2","quantity":quantity,
              "order_type":kind,"time_in_force":"0","_reference_price":reference}
        pricing={"requestedOrderType":"LO" if price is not None else "MO","effectiveOrderType":kind,
                 "tradingSession":session,"referencePrice":reference,**selected.details()}
        if kind=="LO":
            effective=OrderAutomation.order_price(price if price is not None else reference,side)
            body.update(price=effective,force_only_rth="2")
            pricing.update(submittedPrice=effective,priceSource="command" if price is not None else "kafka_book")
        else: pricing["priceSource"]="broker_market_execution"
        return body,pricing

    def cancel_order(self, identifier, *, dry_run=False):
        with self._lock():
            row=self._detail(identifier)
            if dry_run: return {"dryRun":True,"orderId":identifier,"cancelable":row["cancelable"],"status":row["status"]}
            self._order_action_delay()
            key="cancel:"+identifier
            record=self.journal.read(key) or {"account":self.journal.account,"command_id":key,"phase":"cancel","order_id":identifier}
            final=self._cancel_one(identifier,record,key)
            record.update(phase="finished",status=final["status"])
            self.journal.save(key,record)
            return {"orderId":identifier,"status":final["status"],"canceled":final["status"] in ("CanceledStatus","PartialWithdrawal")}
