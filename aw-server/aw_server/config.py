import os
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv


def _project_root_with_dotenv() -> Path:
    """
    Repository root (parent of `aw-server/`) when running from a checkout
    (`.../aw-server/aw_server/config.py`). Otherwise CWD (e.g. wheel install + systemd).
    """
    p = Path(__file__).resolve()
    if p.parent.name == "aw_server":
        aw_server_dir = p.parent.parent
        if aw_server_dir.name == "aw-server":
            return aw_server_dir.parent
    return Path.cwd()


def _load_dotenv() -> None:
    explicit = os.environ.get("GFPS_ENV_FILE")
    if explicit:
        load_dotenv(Path(explicit), override=False)
        return
    root = _project_root_with_dotenv()
    load_dotenv(root / ".env", override=False)


_load_dotenv()


def _get(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def _get_int(key: str, default: int) -> int:
    v = os.environ.get(key)
    if v is None or str(v).strip() == "":
        return default
    return int(v)


def _mysql_block(*, testing: bool) -> Dict[str, Any]:
    if not testing:
        return {
            "host": _get("GFPS_MYSQL_HOST", "localhost"),
            "port": _get_int("GFPS_MYSQL_PORT", 3306),
            "user": _get("GFPS_MYSQL_USER", "root"),
            "password": _get("GFPS_MYSQL_PASSWORD", ""),
            "database": _get("GFPS_MYSQL_DATABASE", "activitywatch"),
        }
    return {
        "host": _get("GFPS_TESTING_MYSQL_HOST", _get("GFPS_MYSQL_HOST", "localhost")),
        "port": _get_int(
            "GFPS_TESTING_MYSQL_PORT",
            _get_int("GFPS_MYSQL_PORT", 3306),
        ),
        "user": _get("GFPS_TESTING_MYSQL_USER", _get("GFPS_MYSQL_USER", "root")),
        "password": _get(
            "GFPS_TESTING_MYSQL_PASSWORD",
            _get("GFPS_MYSQL_PASSWORD", ""),
        ),
        "database": _get("GFPS_TESTING_MYSQL_DATABASE", "activitywatch_test"),
    }


def _build_config() -> Dict[str, Any]:
    default_bronevik = "https://ibronevik.ru/taxi/c/0/api/v1/"
    return {
        "server": {
            "host": _get("GFPS_HOST", "127.0.0.1"),
            "port": str(_get_int("GFPS_PORT", 5700)),
            "storage": _get("GFPS_STORAGE", "mysql"),
            "cors_origins": _get("GFPS_CORS_ORIGINS", "*"),
            "bronevik_url": _get("GFPS_BRONEVIK_URL", default_bronevik),
            "mysql": _mysql_block(testing=False),
            "custom_static": {},
        },
        "server-testing": {
            "host": _get("GFPS_TESTING_HOST", "localhost"),
            "port": str(_get_int("GFPS_TESTING_PORT", 5777)),
            "storage": _get("GFPS_TESTING_STORAGE", "mysql"),
            "cors_origins": _get("GFPS_TESTING_CORS_ORIGINS", "*"),
            "bronevik_url": _get("GFPS_TESTING_BRONEVIK_URL", default_bronevik),
            "mysql": _mysql_block(testing=True),
            "custom_static": {},
        },
    }


config = _build_config()
