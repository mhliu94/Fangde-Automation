#!/usr/bin/env bash
set -euo pipefail
umask 077
repo_dir="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$repo_dir"
python_bin="${FANGDE_PYTHON:-python3}"
if [[ "$(uname -s)" != Linux ]]; then
  echo 'install.sh requires Linux. Use python -m fangde.deployment bundle on Windows.' >&2
  exit 2
fi
if [[ $# -eq 0 || "${1:-}" == --help || "${1:-}" == -h ]]; then
  "$python_bin" -m fangde.deployment install --help
  exit 0
fi
"$python_bin" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else "Python 3.10 or newer is required.")'
"$python_bin" -m fangde.deployment preflight
if ! "$python_bin" -m venv "$repo_dir/.venv"; then
  echo 'Could not create the virtual environment. Install your distribution’s python3-venv package and retry.' >&2
  exit 2
fi
"$repo_dir/.venv/bin/python" -m pip install --disable-pip-version-check -e "${repo_dir}[kafka]"
exec "$repo_dir/.venv/bin/python" -m fangde.deployment install "$@"
