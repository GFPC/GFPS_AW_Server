#!/usr/bin/env sh
# Editable-установка только из ./aw-core и ./aw-server (GFP-форк).
# Не использовать pip install aw-server с PyPI — там upstream ActivityWatch.
#
# Режим совместимости (legacy .pth), чтобы aw_datastore не ломался в setuptools 80+.
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
pip install -U pip wheel
pip uninstall -y aw-core aw-server 2>/dev/null || true
pip install --no-cache-dir -e ./aw-core --config-settings editable_mode=compat
pip install --no-cache-dir -e ./aw-server --config-settings editable_mode=compat
python3 -c "from aw_datastore import Datastore; print('aw_datastore OK')"
if [ -x "$ROOT/venv/bin/aw-server" ]; then
  "$ROOT/venv/bin/aw-server" --version
else
  aw-server --version
fi
