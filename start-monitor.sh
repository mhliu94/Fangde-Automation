#!/usr/bin/env bash
set -euo pipefail
umask 077
repo_dir="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
if [[ ! -x "$repo_dir/.venv/bin/python" ]]; then
  echo 'Run ./install.sh /private/account.deployment.json first.' >&2
  exit 2
fi
exec "$repo_dir/.venv/bin/python" -m fangde.deployment monitor "$@"
