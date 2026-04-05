#!/usr/bin/env python3
"""
Создать одно приглашение напрямую в MySQL (без HTTP и без Bronevik) и вывести токен.

Использует те же переменные окружения, что и сервер в --testing (GFPS_TESTING_MYSQL_*).

Пример:
  python scripts/dev_issue_invitation_token.py
  python scripts/dev_issue_invitation_token.py -o invitation.token
  python scripts/dev_issue_invitation_token.py --email me@local.test --team my-team
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_env() -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv(REPO_ROOT / ".env")
    except ImportError:
        pass


def _mysql_testing() -> dict:
    _load_env()
    return {
        "host": os.getenv("GFPS_TESTING_MYSQL_HOST", os.getenv("GFPS_MYSQL_HOST", "localhost")),
        "port": int(os.getenv("GFPS_TESTING_MYSQL_PORT", os.getenv("GFPS_MYSQL_PORT", "3306"))),
        "user": os.getenv("GFPS_TESTING_MYSQL_USER", os.getenv("GFPS_MYSQL_USER", "root")),
        "password": os.getenv(
            "GFPS_TESTING_MYSQL_PASSWORD",
            os.getenv("GFPS_MYSQL_PASSWORD", ""),
        ),
        "database": os.getenv("GFPS_TESTING_MYSQL_DATABASE", "activitywatch_test"),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Issue one GFP invitation token for local dev")
    ap.add_argument("-o", "--output", type=Path, help="Write token to this file (UTF-8, one line)")
    ap.add_argument("--email", default="dev@local.test", help="Invitee email")
    ap.add_argument("--team", default="local-team", help="team_id for the invitation row")
    args = ap.parse_args()

    sys.path.insert(0, str(REPO_ROOT / "aw-core"))
    from aw_datastore.datastore import Datastore

    kw = _mysql_testing()
    ds = Datastore(testing=True, **kw)

    rows = ds.create_invitations_batch(
        [
            {
                "email": args.email,
                "role_id": "local-dev",
                "firstName": "Local",
                "lastName": "Dev",
            }
        ],
        args.team,
    )
    if isinstance(rows, dict) and rows.get("error"):
        em = rows.get("emails") or []
        err = rows.get("error")
        print(f"{err}: {', '.join(em)}", file=sys.stderr)
        sys.exit(2)
    if not rows:
        print("create_invitations_batch returned no rows (empty email?)", file=sys.stderr)
        sys.exit(1)
    token = rows[0].get("token")
    if not token:
        print("No token in row (unexpected)", file=sys.stderr)
        sys.exit(1)
    print(token)
    if args.output:
        args.output.write_text(token.strip() + "\n", encoding="utf-8")
        print(f"Wrote: {args.output.resolve()}", file=sys.stderr)


if __name__ == "__main__":
    main()
