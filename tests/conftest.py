"""
Pytest fixtures for HTTP-style integration tests against AWFlask.

Uses Flask's test client (WSGI), which performs real routing, JSON, and host checks
like production. Requires MySQL (see GFPS_TESTING_* / GFPS_* in .env).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent
_aw_server = _REPO / "aw-server"
_aw_core = _REPO / "aw-core"
for _p in (_aw_server, _aw_core):
    s = str(_p)
    if s not in sys.path:
        sys.path.insert(0, s)


@pytest.fixture(scope="session")
def app():
    """Single app instance; uses server-testing MySQL config from env."""
    from aw_server.config import config
    from aw_server.server import AWFlask

    cfg = config["server-testing"]
    mysql = cfg["mysql"]
    cors = cfg["cors_origins"]
    if isinstance(cors, str):
        cors = [c.strip() for c in cors.split(",") if c.strip()]
    try:
        return AWFlask(
            cfg["host"],
            testing=True,
            cors_origins=cors,
            bronevik_url=cfg["bronevik_url"],
            mysql_kwargs={
                "host": mysql["host"],
                "port": mysql["port"],
                "user": mysql["user"],
                "password": mysql["password"],
                "database": mysql["database"],
            },
        )
    except Exception as e:  # noqa: BLE001 — surface connection errors as skip
        pytest.skip(f"MySQL / Datastore unavailable for integration tests: {e}")


@pytest.fixture(scope="session")
def http_client(app):
    return app.test_client()


@pytest.fixture
def bronevik_cached_session(app):
    """
    Puts a 24h Bronevik auth row so manager endpoints do not call the real API.
    """
    token = "pytest-bronevik-token"
    u_hash = "pytest-bronevik-uhash"
    app.api.db.set_user_authorized(token, u_hash, ttl_hours=24)
    yield token, u_hash
