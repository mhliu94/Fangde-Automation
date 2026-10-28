# Validation record

Updated on 2026-10-27 for version 0.2.0.

- Offline regression, including the preserved legacy holdings scripts:
  **141 passed, 1 skipped**, plus 289 passing subtests.
- The skipped test renders a Linux systemd unit and requires POSIX paths.
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
- An Ubuntu offline-test workflow is included for future repository CI; it has
  not been run as part of this validation.

The account's credentials and raw account responses are excluded from the source
package. A separate private directory contains the requested account export.
