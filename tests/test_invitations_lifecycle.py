"""
Интеграционные тесты: два инвайта на один email, claim, delete user,
revoked / superseded → pending.

Требуется MySQL (см. conftest / GFPS_TESTING_*).
"""

from __future__ import annotations

import uuid

import pytest

MANAGER_INVITATIONS = "/api/1/manager/invitations"
CLAIM = "/api/0/gfps/invitations/claim"


def _json(client, method: str, path: str, **kwargs):
    kw = dict(kwargs)
    headers = kw.pop("headers", {})
    h = {"Host": "localhost", "Content-Type": "application/json", **headers}
    fn = getattr(client, method.lower())
    return fn(path, headers=h, **kw)


def _manager_delete_user(client, user_id: int, token: str, u_hash: str):
    return _json(
        client,
        "delete",
        f"/api/1/manager/users/{user_id}",
        json={"token": token, "u_hash": u_hash},
    )


def _manager_put_user(client, user_id: int, body: dict):
    return _json(
        client,
        "put",
        f"/api/1/manager/users/{user_id}",
        json=body,
    )


def _inv_row_for_email(rows, email: str):
    for r in rows:
        if (r.get("email") or "").strip().lower() == email.strip().lower():
            return r
    raise AssertionError(f"no invitation row for email {email!r} among {len(rows)} rows")


@pytest.mark.integration
def test_two_teams_same_email_claim_then_delete_reopens_superseded(
    http_client, bronevik_cached_session
):
    """
    Два приглашения на один email (разные team_id): claim по первому → вторая команда
    superseded; после delete пользователя — первая revoked, вторая снова pending.
    """
    token_b, u_hash_b = bronevik_cached_session
    suffix = uuid.uuid4().hex[:12]
    email = f"lifecycle-dual-{suffix}@example.test"
    team_a = f"team-a-{suffix}"
    team_b = f"team-b-{suffix}"

    r_a = _json(
        http_client,
        "post",
        MANAGER_INVITATIONS,
        json={
            "token": token_b,
            "u_hash": u_hash_b,
            "team_id": team_a,
            "invitations": [{"email": email, "role_id": "role-analyst"}],
        },
    )
    assert r_a.status_code == 200, r_a.get_data(as_text=True)
    assert r_a.get_json()["status"] == "success"
    created_a = r_a.get_json()["data"]["invitations"]
    assert len(created_a) == 1
    invite_token = created_a[0]["token"]

    r_b = _json(
        http_client,
        "post",
        MANAGER_INVITATIONS,
        json={
            "token": token_b,
            "u_hash": u_hash_b,
            "team_id": team_b,
            "invitations": [{"email": email, "role_id": "role-analyst"}],
        },
    )
    assert r_b.status_code == 200, r_b.get_data(as_text=True)
    assert r_b.get_json()["status"] == "success"

    r_claim = _json(
        http_client,
        "post",
        CLAIM,
        json={"token": invite_token, "uuid": str(uuid.uuid4()), "username": "emp-dual"},
    )
    assert r_claim.status_code == 200
    claim_body = r_claim.get_json()
    assert claim_body["status"] == "success"
    user_id = claim_body["user"]["id"]

    r_list_a = _json(
        http_client,
        "post",
        MANAGER_INVITATIONS,
        json={"token": token_b, "u_hash": u_hash_b, "team_id": team_a, "invitations": []},
    )
    r_list_b = _json(
        http_client,
        "post",
        MANAGER_INVITATIONS,
        json={"token": token_b, "u_hash": u_hash_b, "team_id": team_b, "invitations": []},
    )
    assert r_list_a.status_code == 200 and r_list_b.status_code == 200
    row_a = _inv_row_for_email(r_list_a.get_json()["data"]["invitations"], email)
    row_b = _inv_row_for_email(r_list_b.get_json()["data"]["invitations"], email)
    assert row_a["status"] == "claimed"
    assert row_b["status"] == "superseded"
    assert row_b.get("superseded_at")

    r_del = _manager_delete_user(http_client, user_id, token_b, u_hash_b)
    assert r_del.status_code == 200
    assert r_del.get_json()["status"] == "success"

    r_list_a2 = _json(
        http_client,
        "post",
        MANAGER_INVITATIONS,
        json={"token": token_b, "u_hash": u_hash_b, "team_id": team_a, "invitations": []},
    )
    r_list_b2 = _json(
        http_client,
        "post",
        MANAGER_INVITATIONS,
        json={"token": token_b, "u_hash": u_hash_b, "team_id": team_b, "invitations": []},
    )
    row_a2 = _inv_row_for_email(r_list_a2.get_json()["data"]["invitations"], email)
    row_b2 = _inv_row_for_email(r_list_b2.get_json()["data"]["invitations"], email)
    assert row_a2["status"] == "revoked"
    assert row_a2.get("revoked_at")
    assert row_a2.get("user_id") in (None, 0)
    assert row_a2.get("installed") is False

    assert row_b2["status"] == "pending"
    assert row_b2.get("superseded_at") in (None, "")
    assert row_b2.get("user_id") in (None, 0)


@pytest.mark.integration
def test_delete_user_single_team_claimed_invitation_becomes_revoked(
    http_client, bronevik_cached_session
):
    """Один инвайт, claim, delete — приглашение остаётся строкой со статусом revoked."""
    token_b, u_hash_b = bronevik_cached_session
    suffix = uuid.uuid4().hex[:12]
    email = f"lifecycle-one-{suffix}@example.test"
    team_id = f"team-one-{suffix}"

    r = _json(
        http_client,
        "post",
        MANAGER_INVITATIONS,
        json={
            "token": token_b,
            "u_hash": u_hash_b,
            "team_id": team_id,
            "invitations": [{"email": email, "role_id": "role-analyst"}],
        },
    )
    assert r.status_code == 200
    invite_token = r.get_json()["data"]["invitations"][0]["token"]

    r_claim = _json(
        http_client,
        "post",
        CLAIM,
        json={"token": invite_token, "uuid": str(uuid.uuid4()), "username": "emp-one"},
    )
    assert r_claim.status_code == 200
    user_id = r_claim.get_json()["user"]["id"]

    r_del = _manager_delete_user(http_client, user_id, token_b, u_hash_b)
    assert r_del.status_code == 200

    r_list = _json(
        http_client,
        "post",
        MANAGER_INVITATIONS,
        json={"token": token_b, "u_hash": u_hash_b, "team_id": team_id, "invitations": []},
    )
    assert r_list.status_code == 200
    row = _inv_row_for_email(r_list.get_json()["data"]["invitations"], email)
    assert row["status"] == "revoked"
    assert row.get("revoked_at")
    assert row.get("installed") is False


@pytest.mark.integration
def test_two_teams_change_email_before_delete_still_reopens_superseded(
    http_client, bronevik_cached_session
):
    """
    После claim меняем email пользователя; superseded строка хранит старый email в invitation.
    Удаление пользователя всё равно должно вернуть второй инвайт в pending.
    """
    token_b, u_hash_b = bronevik_cached_session
    suffix = uuid.uuid4().hex[:12]
    email = f"lifecycle-old-{suffix}@example.test"
    new_email = f"lifecycle-new-{suffix}@example.test"
    team_a = f"team-ca-{suffix}"
    team_b = f"team-cb-{suffix}"

    r_a = _json(
        http_client,
        "post",
        MANAGER_INVITATIONS,
        json={
            "token": token_b,
            "u_hash": u_hash_b,
            "team_id": team_a,
            "invitations": [{"email": email, "role_id": "role-analyst"}],
        },
    )
    assert r_a.status_code == 200
    invite_token = r_a.get_json()["data"]["invitations"][0]["token"]

    r_b = _json(
        http_client,
        "post",
        MANAGER_INVITATIONS,
        json={
            "token": token_b,
            "u_hash": u_hash_b,
            "team_id": team_b,
            "invitations": [{"email": email, "role_id": "role-analyst"}],
        },
    )
    assert r_b.status_code == 200
    assert r_b.get_json()["status"] == "success"
    r_claim = _json(
        http_client,
        "post",
        CLAIM,
        json={"token": invite_token, "uuid": str(uuid.uuid4()), "username": "emp-move"},
    )
    assert r_claim.status_code == 200
    user_id = r_claim.get_json()["user"]["id"]

    r_put = _manager_put_user(
        http_client,
        user_id,
        {"token": token_b, "u_hash": u_hash_b, "email": new_email},
    )
    assert r_put.status_code == 200, r_put.get_data(as_text=True)
    assert r_put.get_json()["status"] == "success"

    r_del = _manager_delete_user(http_client, user_id, token_b, u_hash_b)
    assert r_del.status_code == 200

    r_list_b = _json(
        http_client,
        "post",
        MANAGER_INVITATIONS,
        json={"token": token_b, "u_hash": u_hash_b, "team_id": team_b, "invitations": []},
    )
    invs = r_list_b.get_json()["data"]["invitations"]
    row_b = _inv_row_for_email(invs, email)
    assert row_b["status"] == "pending"
    assert row_b.get("superseded_at") in (None, "")
