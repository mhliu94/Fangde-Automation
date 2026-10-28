# Fangde protocol evidence and boundaries

Implementation evidence is the user's locally extracted Fangde Android 7.7.1
code, plus read-only responses from the user's authenticated account. Shared
runner/outbox/command-center behavior was adapted from the user's GigaMoney
repository at commit `031709523a00607a50f1caaabf05afca23081c61`.

Trading/read requests use `https://apitrade.techfx88.com`. Password login uses
`https://api-apimsg.techfx88.com/user/login/index`. Requests use the app's current
timestamp/signature calculation, login token, and trading token where required.
TLS verification remains enabled. Credentials are not sent through ambient proxy
settings or followed to redirects. Response size and timeout are bounded.

| Operation | App endpoint | Evidence |
|---|---|---|
| Holdings | POST `/trade/index/portfolio` | Live read verified |
| Summary/accounts | POST `/trade/asset/summary2`, `/user/index/account_list` | Recovered read client |
| Today | GET `/trade/index/todayOrder` | Live read verified |
| History | GET `/trade/index/historyOrder` | All pages verified in validation range |
| Detail | GET `/trade/index/orderDetail` | Live read verified |
| Trading configuration | GET `/app/setting/global` | Live US type/duration settings verified |
| Security metadata | GET `/quotes/stock/detail` | Live identity read verified; not a price feed |
| Submit | POST `/trade/index/orderActionAdd` | App form inspected; offline transport tests |
| Cancel | POST `/trade/index/orderActionCancel` | App form inspected; offline transport tests |
| Password login | POST `/user/login/index` on the login host | Live password renewal verified |
| Trading unlock | POST `/user/trade/clientAuth` | Live independent unlock verified |
| Active account identity | POST `/user/index/info` | Live read verified for password setup |

Submit fields follow `BaseTradeService` and `PlaceOrderConformViewModel`: symbol,
exchange, side, order type, quantity, price, reference market price, validity,
extended-session flag and the empty optional coupon/transfer fields. The market
ticket's reference price behavior comes from `PlaceOrderData`. No live mutation
response has been captured by this project. A submit response without an exact
string `order_no` is treated as uncertain, never coerced or retried.

Normal and strategy orders use different status meanings. Strategy descriptions
are mapped explicitly; their numeric codes are not interpreted as normal-order
codes. Known normal statuses and returned descriptions are normalized before
mapping to KTrader. An unfamiliar detail status blocks financial follow-up.

Fangde responses include Hong Kong broker time and sometimes a New York-local
timestamp. The adapter prefers the explicit New York-local fields; otherwise it
uses `api.brokerTimezone` (Asia/Hong_Kong). UTC output retains the creation time.

The market session check uses the primary
[exchange_calendars project](https://github.com/gerrymanoim/exchange_calendars).
This is a calendar approximation rather than broker session/halts telemetry.
Early-close extended hours end at the earlier of 20:00 New York or four hours
after the scheduled close. Overnight market commands are rejected.

Version-2 phone exports bind the user and saved trading-token key. That key can
be shared by brokerage profiles. Password setup reads the phone-selected account
and upgrades to version 3, whose digest includes user, broker and trading-account
identifiers. Renewal selects that exact returned account rather than trusting the
response's default index. This is a local mix-up check, not broker attestation.
The server pins the digest and logical account label.

Password encoding matches the app's Base64 DEFAULT encoding of its fixed prefix
and UTF-8 password. Phone setup uses the app's production RSA public key and
PKCS#1 v1.5 padding; the phone signing seed and current timestamp produce each
login signature. Encoded passwords remain password-equivalent secrets.

Login expiry (`1001`) and trade unlock requirements (`1101`, `1102`) are distinct.
Recovery is bounded per read. Auth failures latch locally; explicit retry is
required after a failed/interrupted password request. Financial requests never
replay automatically. Main/trade password authentication was checked from Windows;
Linux network access, longevity, and concurrent phone/server behavior remain to
be checked. SMS verification is outside scope.

The initial Frida Java recorder caused ART garbage-collection crashes on the
rooted phone. It was stopped and Fangde restarted. Normal deployment and future
password setup use no instrumentation.
