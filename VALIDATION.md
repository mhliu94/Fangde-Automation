# Validation record

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
