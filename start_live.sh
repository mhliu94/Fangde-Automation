#!/usr/bin/env bash
set -euo pipefail
umask 077
repo_dir="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
python_bin="$repo_dir/.venv/bin/python"
if [[ ! -x "$python_bin" ]]; then
  echo 'Run ./install.sh /private/account.deployment.json first.' >&2
  exit 2
fi
if [[ $# -eq 1 && "$1" == --check ]]; then
  exec "$python_bin" -m fangde.deployment runner --check
fi
if [[ $# -gt 0 ]]; then
  echo 'Usage: ./start_live.sh [--check]'
  echo 'Enables live trading and starts the Kafka runner with nohup. Set FANGDE_CONFIG to select a config.'
  [[ $# -eq 1 && ( "$1" == --help || "$1" == -h ) ]] && exit 0
  exit 2
fi
command -v nohup >/dev/null
"$python_bin" -m fangde.deployment runner --check >/dev/null
run_log="$("$python_bin" - <<'PY'
from fangde.config import load_settings
from fangde.errors import SafetyError
from fangde.lock import EmulatorLock

settings = load_settings()
try:
    with EmulatorLock(settings.work_dir / 'runner.lock', 0):
        pass
except SafetyError:
    raise SystemExit('Runner already running. Stop its systemd service or use fangde runner-stop first.')
log = (settings.resolve_path(settings.get('monitor.runnerLog'))
       if settings.get('monitor.runnerLog') else settings.log_dir / 'runner.log')
log.parent.mkdir(parents=True, exist_ok=True)
print(log)
PY
)"
export PYTHONUNBUFFERED=1 FANGDE_RUN_LOG="$run_log"
nohup "$python_bin" -m fangde.cli runner --live >>"$run_log" 2>&1 </dev/null &
runner_pid=$!
sleep 1
if ! kill -0 "$runner_pid" 2>/dev/null; then
  echo "Runner exited during startup. Check $run_log" >&2
  exit 1
fi
printf 'Launched live runner (PID %s). Log: %s\n' "$runner_pid" "$run_log"
printf 'Check status with: %s/.venv/bin/fangde runner-status\n' "$repo_dir"
