# Validation record

## Order reconciliation and independent monitoring — 2026-11-06

- The 100-share AEHL sell received at 2026-11-05 15:59:08 EST was blocked
  before submission by an earlier unresolved dispatch. Logs, the dispatch
  journal and broker history were checked; it was not a broker fill or cancel.
  Its local rejection was published with the original command ID and verified
  in the order server as `REJECTED`, zero filled. The account's unreconciled list
  was empty after consumption. No submit or cancellation request was made for
  this repair.
- The runner now persists rejection feedback before committing discarded valid
  order commands, suppresses rejected-command redelivery, and keeps ambiguous
  dispatched orders unresolved until supported by broker evidence.
- Independent order polling targets 0.5 seconds. Read-only requests use validated
  session snapshots; concurrent authentication failures share one serialized
  renewal. Financial writes retain the exclusive session lock. FIFO admission
  prevents a poller from starving other session users.
- Confirmed changes are persisted immediately and sent by a separate Kafka
  worker with zero linger. Queued transitions survive delivery failures, process
  restart and midnight. Unchanged orders remain suppressed after restart.
  Missing daily-list orders are queried by exact ID; stale observations cannot
  decrease fills or reopen terminal orders.
- Live recovery also found three older strategy orders whose broker creation
  dates had rolled forward. Exact-ID detail reads confirmed cancellation. Their
  updates retained the original creation time after fixed-identity validation;
  all three were acknowledged by Kafka and removed from local active tracking.
- Validation: **245 tests passed**, plus **326 passing subtests**. Coverage
  includes simultaneous expired-token reads with one renewal, independent reads
  during a blocked portfolio response, fair session admission, blocked-command
  reporting, failed persistence before offset commit, duplicate prevention,
  missing/malformed broker list rows, queued transitions during Kafka failures,
  and strategy creation-date renewal. Four existing pandas deprecation warnings
  remain. Local startup validation and `git diff --check` passed.
- Final live runner restarted at 02:31:52 UTC. A subsequent 65-second observation
  found no runtime errors, no pending deliveries, and no unchanged order
  republications across restart. The measured polling sample reached 0.500
  seconds; individual broker responses can exceed that target. The order server
  was rechecked at 02:33:34 UTC: the target remained rejected with zero fills and
  the Fangde account had no unreconciled orders. Saved trading-enabled state was
  preserved.

## Premarket investigation and regression coverage — 2026-11-02

- Saved runner logs show a `MARKET_ORDER` to sell 100 AEHL received on
  2026-10-28 at 12:25:43 UTC (08:25:43 New York). The saved order journal records
  conversion to a DAY limit sell at USD 7.30 with `force_only_rth=2`, which the
  transport maps to `extended_trading=1`.
- Its reference was selected from `price-books`, partition 0, offset 95033967,
  with timestamp 1793190329387303168 ns. The journal records 119 shares of visible
  depth across four levels; this reference was within the five-minute age limit.
- At 12:25:55 UTC the submit request returned `AuthenticationRequired`. The
  journal has a dispatch timestamp, phase `rejected`, and no broker order ID.
  The runner stopped without committing that Kafka command. The original logs
  did not retain the broker authentication code or raw Kafka command bytes, so
  they cannot establish the exact authentication cause or original command offset.
- A separate sell of 2 PDD at 12:13:19 UTC was blocked before submission by the
  available-long-share guard. The saved PDD strategy snapshot shows 20 shares,
  zero fills and `待触发` (awaiting trigger). The guard incorrectly counted that
  dormant strategy as a submitted sell. It now excludes only explicitly dormant,
  unfilled strategies while retaining the broker's available-share cap and all
  submitted-order reservations. The runner continued until the AEHL auth failure
  roughly 12 minutes later; the log contains no unhandled crash traceback.
- PDD is absent from the saved price-book cache and the nearby refresh logs.
  A fresh PDD feed is still required before a PDD market command can be priced.
- The existing authentication correction validates login and obtains a fresh
  trading token before every financial write. New offline regressions reconstruct
  the AEHL intent using synthetic credentials, books and HTTP responses, then
  verify the signed extended-hours form, fresh token, journal, and duplicate guard.
  They also cover after-hours, both sides, explicit limits/emulated FOK, DST,
  holidays, early closes, stale books, and failures at unlock versus submission.
- Command logs now retain topic/partition/offset and trading fields; journals
  retain command location alongside price-book provenance. Workflow failures
  retain pricing context and structured authentication diagnostics.
- Validation: **226 tests passed**, plus 310 passing subtests, after network/socket
  access was enabled. This includes the four dashboard HTTP tests blocked during
  the initial sandboxed run. Local startup validation, CLI self-test and
  `git diff --check` passed. An earlier read-only Kafka metadata attempt failed
  under the network restriction with transport code -195; it did not join the
  runner group or commit offsets.
- The managed runner systemd unit was reinstalled and the user manager reloaded.
  `systemd-analyze --user verify` passed. Its existing enabled state and user
  lingering were preserved; the service remains inactive until explicitly started.
- Live Kafka/broker verification remains pending. The saved runner state is still
  `authentication_required`; the historical order has not been replayed.
  Overnight support is outside this change.

## Earlier validation

Updated on 2026-10-27 for version 0.3.0.

- Offline regression, including the preserved legacy holdings scripts:
  **160 passed, 3 skipped**, plus 305 passing subtests on Windows.
- The skipped tests require POSIX file modes or Linux systemd paths.
- Host: Windows, Python 3.12. Bundled pandas 3.0.1/numpy 2.3.5 were used with
  exchange_calendars 4.13.2 and workspace-installed test dependencies.
- CLI help and dependency-free self-test passed; all Python sources compiled.
- Live read-only checks: holdings, broker-listed today orders with no mapping
  errors, and paginated history across the complete validation range.
- Read checks used the current account on the home Windows machine. They do not
  establish token lifetime or validity after another phone login.
- Live authentication: an expired main session recovered through password login,
  trading unlock succeeded, and holdings were retrieved successfully. Independently
  clearing the local trading token also caused a successful unlock and holdings read.
- Fresh RSA phone encryption and the recorder-free password setup were verified
  with password login, exact brokerage selection, unlock, and holdings. The active
  account-info endpoint was read and matched the configured user/broker/account.
- Authentication validation used a transport allowlist excluding order and cancel
  endpoints. Additional verification is not implemented; SMS remains outside scope.
- The Frida recorder triggered native ART crashes/lag. It was stopped, its helper
  terminated, and Fangde restarted. Neither the package nor deployment needs Frida.
- No live submit, cancellation, real Kafka command publication, runner startup,
  Linux service installation, or remote deployment was performed.
- Ubuntu CI previously passed the version 0.2.0 offline suite. The deployment
  workflow now covers Python 3.10 and 3.12, fresh virtual-environment installation,
  both local startup checks, systemd unit syntax, and preservation of renewed
  credentials and order state on reinstall. It uses synthetic credentials and
  does not start either service or contact a brokerage/Kafka server.
- Deployment tests cover account mismatch rejection, separate runner/monitor
  execution, active-runner exclusion, private file modes, explicit credential
  replacement, and preservation of runtime configuration and account records.

The account's credentials and raw account responses are excluded from the source
package. A separate private directory contains the requested account export.
