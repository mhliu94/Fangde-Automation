"""Signed Fangde API with account-bound, optional password session renewal."""
from __future__ import annotations
from contextlib import contextmanager
from dataclasses import dataclass, field
from decimal import Decimal
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request
from . import legacy_holdings as fd
from .lock import EmulatorLock
HOST = "https://apitrade.techfx88.com"
READ_PATHS = {
    "orders": ("GET", "/trade/index/todayOrder"),
    "order_detail": ("GET", "/trade/index/orderDetail"),
    "order_history": ("GET", "/trade/index/historyOrder"),
    "positions": ("POST", "/trade/index/portfolio"),
    "summary": ("POST", "/trade/asset/summary2"),
    "accounts": ("POST", "/user/index/account_list"),
    "user_info": ("POST", "/user/index/info"),
    "settings": ("GET", "/app/setting/global"),
    "security": ("GET", "/quotes/stock/detail"),
}
class SessionError(RuntimeError):
    """A session must be renewed or bootstrapped again; messages contain no secrets."""


class AuthenticationRequired(SessionError):
    """Human verification or replacement account credentials are required."""

    def __init__(self, message, *, scope="unknown", api_code=None):
        super().__init__(message)
        self.scope = scope
        self.api_code = api_code


class TransportError(SessionError):
    """A read can be retried; a mutation must first be reconciled."""


@dataclass(frozen=True)
class ApiResult:
    path: str
    http_status: int
    api_code: int | None
    elapsed_ms: float
    request_id: str
    payload: dict[str, Any] = field(repr=False)

    @property
    def ok(self) -> bool:
        return self.http_status == 200 and self.api_code == 0

    def summary(self) -> dict[str, Any]:
        return {"ok": self.ok, "path": self.path, "http_status": self.http_status,
                "api_code": self.api_code, "elapsed_ms": self.elapsed_ms,
                "request_id": self.request_id, "uses_captured_signature": False}


class ApiError(RuntimeError):
    def __init__(self, result: ApiResult):
        self.result = result
        super().__init__(f"API request failed: HTTP {result.http_status}, code {result.api_code}.")



def save_session(path, state):
    path = Path(path)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd.private_write(path, json.dumps(state, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    if os.name == "posix":
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)

def load_session(path):
    try: state = fd.load_config(path)
    except fd.FangdeError as error: raise AuthenticationRequired(str(error)) from None
    identity = state.get("identity", {})
    if state.get("version") not in (2,3) or not isinstance(identity, dict):
        raise AuthenticationRequired("Export a version-2 or version-3 Fangde session with an account label.")
    if state.get('version')==3 and not isinstance(state.get('authentication'),dict):
        raise AuthenticationRequired('Version-3 sessions require a bound password profile.')
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", str(identity.get("account_id", ""))):
        raise SessionError("Export has no valid logical account ID.")
    if not re.fullmatch(r"[0-9a-f]{64}", str(identity.get("subject_sha256", ""))):
        raise SessionError("Export has no broker identity binding.")
    return state

def exact_positive(value, name):
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ValueError(f"{name} must be an exact decimal string.")
    text = str(value)
    if not re.fullmatch(r"\d{1,16}(?:\.\d{1,8})?", text) or Decimal(text) <= 0:
        raise ValueError(f"{name} must be a positive plain decimal.")
    return text

class ApiClient:
    def __init__(self, settings, *, opener=None):
        self.settings, self.opener = settings, opener
    @classmethod
    def from_settings(cls, settings): return cls(settings)
    @contextmanager
    def _session(self):
        with EmulatorLock(Path(str(self.settings.session_path)+'.lock'),300):
            state = load_session(self.settings.session_path)
            identity = state["identity"]
            if (identity["account_id"] != str(self.settings.require("fangde.accountId")) or
                    identity["subject_sha256"] != self.settings.require("api.subjectSha256")):
                raise AuthenticationRequired("Session belongs to a different account. Import the matching export.")
            if 'authentication' in state:
                from .authentication import validate_profile
                validate_profile(state)
            yield state
    def account_context(self):
        with self._session() as state: return dict(state["identity"])
    def _send(self, state, method, path, fields):
        if set(fields) & {"token", "tradeToken", "_t", "_sign"}:
            raise ValueError("Request cannot override authentication fields.")
        token, trade = state["session"]["token"], state["session"].get("trade_token", "")
        stamp = str(int(time.time()))
        signed = {**fields, "token": token, "_t": stamp,
                  "_sign": hashlib.md5((token + stamp + fd.SIGNING_SUFFIX).encode()).hexdigest()}
        if trade and path not in ("/trade/index/todayOrder", "/user/index/account_list", "/app/setting/global", "/quotes/stock/detail"):
            signed["tradeToken"] = trade
        encoded = urlencode(signed).encode("ascii")
        url, body = HOST + path, None
        headers = {"Accept":"application/json", "User-Agent":"okhttp/4.12.0", **state.get("headers",{})}
        if method=="GET": url += "?" + encoded.decode()
        else:
            body=encoded
            headers["Content-Type"]="application/x-www-form-urlencoded"
        opener=self.opener or fd.Client(state).opener
        start=time.monotonic()
        try:
            with opener.open(Request(url,data=body,headers=headers,method=method),
                             timeout=float(self.settings.get("api.requestTimeoutSeconds",15))) as response:
                maximum=int(self.settings.get("api.maxResponseBytes",4194304))
                raw=response.read(maximum+1)
        except HTTPError as error:
            status=error.code
            error.close()
            if status in (401,403):
                raise AuthenticationRequired("Fangde requires authentication. Unlock on the phone and export a fresh session.") from None
            raise TransportError(f"Fangde HTTP {status}; no automatic write retry.") from None
        except (URLError,TimeoutError,OSError,http.client.HTTPException):
            raise TransportError("Fangde network failure; reconcile submitted requests before retrying.") from None
        if len(raw)>maximum: raise TransportError("Fangde response exceeded the size limit.")
        try: result=json.loads(raw)
        except (ValueError,UnicodeError): raise TransportError("Fangde returned invalid JSON.") from None
        if not isinstance(result,dict) or type(result.get("status")) not in (str,int) or not re.fullmatch(r"-?\d+",str(result["status"])):
            raise TransportError("Fangde returned an unrecognized status.")
        code=int(result["status"])
        if code in (1001,1101,1102,1107,1401,401,403) or (code and re.search(r"登录|登錄|交易密码|交易密碼|验证码|驗證碼|token|session|authentication",
            str(result.get("info") or result.get("message") or ""),re.I)):
            scope = "login" if code == 1001 else "trade" if code in (1101,1102) else "unknown"
            raise AuthenticationRequired("Fangde requires a fresh authenticated session from the phone.",
                                         scope=scope, api_code=code)
        value=ApiResult(path,200,code,(time.monotonic()-start)*1000,stamp,fd.redact(result,(token,trade)))
        if not value.ok: raise ApiError(value)
        if "data" not in value.payload: raise TransportError("Fangde success response is missing data.")
        return value
    def read(self, endpoint, params=None):
        if endpoint not in READ_PATHS: raise ValueError("Unsupported Fangde read operation.")
        fields=dict(params or {})
        if endpoint=="orders":
            if fields: raise ValueError("Today's endpoint returns the complete list without client page parameters.")
            fields={"force":"1","category":"0","bs_flag":"B,S"}
        elif endpoint in ("positions","summary"): fields.setdefault("force","1")
        with self._session() as state:
            return self._read_authenticated(state,*READ_PATHS[endpoint],fields,
                needs_trade=endpoint in ('positions','summary','order_history','order_detail'))

    def _read_authenticated(self,state,method,path,fields,*,needs_trade):
        from .authentication import PasswordAuthentication
        auth=PasswordAuthentication(self.settings,self.opener)
        renewed=set()
        for attempt in range(3):
            if 'authentication' in state:
                if not state['session'].get('token') and 'login' not in renewed:
                    auth.renew(state,'login');renewed.add('login')
                if needs_trade and not state['session'].get('trade_token') and 'trade' not in renewed:
                    if 'login' not in renewed:
                        try:self._send(state,*READ_PATHS['accounts'],{})
                        except AuthenticationRequired as error:
                            if error.scope!='login':raise
                            auth.renew(state,'login');renewed.add('login')
                    auth.renew(state,'trade');renewed.add('trade')
            try:return self._send(state,method,path,fields)
            except AuthenticationRequired as error:
                if ('authentication' not in state or error.scope not in ('login','trade')
                        or error.scope in renewed or attempt==2):raise
                auth.renew(state,error.scope)
                renewed.add(error.scope)
        raise AuthenticationRequired('Session could not be restored.')

    def authenticate(self,*,retry=False):
        from .authentication import PasswordAuthentication
        with self._session() as state:
            auth=PasswordAuthentication(self.settings,self.opener)
            auth.renew(state,'login',retry=retry)
            auth.renew(state,'trade',retry=retry)
            self._send(state,*READ_PATHS['positions'],{'force':'1'})
            return {'ok':True,'account_id':state['identity']['account_id'],
                    'login_restored':True,'trading_unlocked':True,'holdings_verified':True}

class TradingApiClient(ApiClient):
    def write(self, operation, body, *, before_send):
        if operation=="withdraw":
            number=body.get("order_id")
            if not isinstance(number,str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,100}",number):
                raise ValueError("Invalid exact Fangde order number.")
            path,fields="/trade/index/orderActionCancel",{"order_no":number}
        elif operation=="submit":
            counter=body.get("counter_id","").split("/")
            if len(counter)!=3 or counter[:2]!=["ST","US"] or not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,23}",counter[2]):
                raise ValueError("Only US stock tickets are supported.")
            if body.get("action") not in ("1","2") or body.get("order_type") not in ("LO","MO"):
                raise ValueError("Only buy/sell market and limit orders are supported.")
            qty=exact_positive(body.get("quantity"),"quantity")
            if Decimal(qty)!=Decimal(qty).to_integral_value(): raise ValueError("Whole-share quantities are required.")
            reference=exact_positive(body.get("_reference_price"),"reference price")
            price=exact_positive(body.get("price"),"limit price") if body["order_type"]=="LO" else reference
            extended=str(body.get("force_only_rth","1"))=="2"
            if body["order_type"]=="MO" and extended: raise ValueError("Market orders are regular-session only.")
            path="/trade/index/orderActionAdd"
            fields={"symbol":counter[2],"exchange":"US","bs_flag":"B" if body["action"]=="1" else "S",
                "order_type":"PL" if body["order_type"]=="LO" else "M","qty":qty,"price":price,
                "market_price":reference,"duration_type":"DayOrder","valid_end_date":"",
                "extended_trading":"1" if extended else "0","coupon_id":"","need_transfer":""}
        else: raise ValueError("Unsupported Fangde write operation.")
        with self._session() as state:
            if 'authentication' in state:
                # Restore credentials using a read before the durable send marker.
                # Once a submit/cancel is dispatched it is never replayed here.
                self._read_authenticated(state,*READ_PATHS['positions'],{'force':'1'},needs_trade=True)
            if not state["session"].get("trade_token"):
                raise AuthenticationRequired("Unlock trading in Fangde and export the trading token first.")
            before_send()
            data=self._send(state,"POST",path,fields).payload["data"]
            if operation=="submit":
                if not isinstance(data,dict) or not isinstance(data.get("order_no"),str) or not data["order_no"]:
                    raise TransportError("No exact order number returned. Do not resubmit; reconcile manually.")
                return {"order_id_str":data["order_no"]}
            return data
