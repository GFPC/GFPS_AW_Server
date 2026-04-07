"""
HTTP integration tests: invitations (manager) + public claim.

Uses Flask test_client (full request/response cycle). Requires running MySQL and
GFPS_TESTING_MYSQL_* (or defaults) pointing at a test database.
"""

from __future__ import annotations

import uuid

import base58
import pytest

MANAGER_INVITATIONS = "/api/1/manager/invitations"
CLAIM = "/api/0/gfps/invitations/claim"


def _manager_put_user(client, user_id: int, body: dict):
    return _json(
        client,
        "put",
        f"/api/1/manager/users/{user_id}",
        json=body,
    )


def _json(client, method: str, path: str, **kwargs):
    """Call API with JSON and Host header so host_header_check passes."""
    kw = dict(kwargs)
    headers = kw.pop("headers", {})
    h = {"Host": "localhost", "Content-Type": "application/json", **headers}
    fn = getattr(client, method.lower())
    return fn(path, headers=h, **kw)


def test_manager_create_list_and_claim_employee(http_client, bronevik_cached_session):
    token_b, u_hash_b = bronevik_cached_session
    suffix = uuid.uuid4().hex[:12]
    email = f"invitee-{suffix}@example.test"
    team_id = f"team-{suffix}"

    # --- create invitations (manager, JSON body mimics real client) ---
    create_body = {
        "token": token_b,
        "u_hash": u_hash_b,
        "team_id": team_id,
        "invitations": [
            {
                "email": email,
                "role_id": "role-analyst",
                "firstName": "Иван",
                "lastName": "Тестов",
                "middleName": "Петрович",
            }
        ],
    }
    r = _json(http_client, "post", MANAGER_INVITATIONS, json=create_body)
    assert r.status_code == 200, r.get_data(as_text=True)
    created = r.get_json()
    assert created["status"] == "success"
    invs = created["data"]["invitations"]
    assert len(invs) == 1
    assert invs[0]["email"] == email
    assert invs[0]["role_id"] == "role-analyst"
    assert invs[0]["firstName"] == "Иван"
    assert invs[0]["lastName"] == "Тестов"
    assert invs[0]["middleName"] == "Петрович"
    assert invs[0]["installed"] is False
    invite_token = invs[0]["token"]
    assert invite_token
    assert len(base58.b58decode(invite_token)) == 16

    # --- list invitations (POST without invitations[] = list mode) ---
    r_list = _json(
        http_client,
        "post",
        MANAGER_INVITATIONS,
        json={"token": token_b, "u_hash": u_hash_b, "team_id": team_id},
    )
    assert r_list.status_code == 200
    listed = r_list.get_json()
    assert listed["status"] == "success"
    rows_same = [r for r in listed["data"]["invitations"] if r["email"] == email]
    assert len(rows_same) == 1
    assert rows_same[0].get("token") == invite_token

    # --- employee accepts invitation (public claim) ---
    employee_uuid = str(uuid.uuid4())
    r_claim = _json(
        http_client,
        "post",
        CLAIM,
        json={
            "token": invite_token,
            "uuid": employee_uuid,
        },
    )
    assert r_claim.status_code == 200
    body = r_claim.get_json()
    assert body["status"] == "success"
    assert "user" in body
    assert body["user"]["uuid"] == employee_uuid
    assert body["user"]["email"] == email
    assert body["user"]["username"] == ""
    assert body["user"]["firstName"] == "Иван"
    assert body["user"]["lastName"] == "Тестов"
    assert body["user"]["middleName"] == "Петрович"
    assert body["user"]["role_id"] == "role-analyst"
    assert team_id in (body["user"].get("team") or [])
    assert body["invitation"]["installed"] is True
    assert body["invitation"]["user_id"] == body["user"]["id"]


def test_manager_changes_employee_role_with_team_id(http_client, bronevik_cached_session):
    """PUT with role_id + team_id updates only that team membership."""
    token_b, u_hash_b = bronevik_cached_session
    suffix = uuid.uuid4().hex[:12]
    email = f"role-change-{suffix}@example.test"
    team_id = f"team-{suffix}"
    create_body = {
        "token": token_b,
        "u_hash": u_hash_b,
        "team_id": team_id,
        "invitations": [{"email": email, "role_id": "role-analyst"}],
    }
    r = _json(http_client, "post", MANAGER_INVITATIONS, json=create_body)
    assert r.status_code == 200
    invite_token = r.get_json()["data"]["invitations"][0]["token"]
    r_claim = _json(
        http_client,
        "post",
        CLAIM,
        json={"token": invite_token, "uuid": str(uuid.uuid4())},
    )
    assert r_claim.status_code == 200
    user = r_claim.get_json()["user"]
    uid = user["id"]
    assert user["role_id"] == "role-analyst"

    r_put = _manager_put_user(
        http_client,
        uid,
        {
            "token": token_b,
            "u_hash": u_hash_b,
            "team_id": team_id,
            "role_id": "role-lead",
        },
    )
    assert r_put.status_code == 200, r_put.get_data(as_text=True)
    updated = r_put.get_json()
    assert updated["status"] == "success"
    assert updated["data"]["user"]["role_id"] == "role-lead"
    assert team_id in (updated["data"]["user"].get("team") or [])


def test_manager_put_role_id_without_team_rejected(http_client, bronevik_cached_session):
    token_b, u_hash_b = bronevik_cached_session
    suffix = uuid.uuid4().hex[:12]
    r = _json(
        http_client,
        "post",
        MANAGER_INVITATIONS,
        json={
            "token": token_b,
            "u_hash": u_hash_b,
            "invitations": [{"email": f"rej-{suffix}@example.test", "role_id": "r1"}],
        },
    )
    assert r.status_code == 200
    invite_token = r.get_json()["data"]["invitations"][0]["token"]
    r_claim = _json(
        http_client,
        "post",
        CLAIM,
        json={"token": invite_token, "uuid": str(uuid.uuid4())},
    )
    assert r_claim.status_code == 200
    uid = r_claim.get_json()["user"]["id"]
    r = _manager_put_user(
        http_client,
        uid,
        {"token": token_b, "u_hash": u_hash_b, "role_id": "x"},
    )
    assert r.status_code == 200
    body = r.get_json()
    assert body["status"] == "error"
    assert "team" in body.get("message", "").lower()


def test_claim_twice_second_call_fails(http_client, bronevik_cached_session):
    token_b, u_hash_b = bronevik_cached_session
    suffix = uuid.uuid4().hex[:12]
    create_body = {
        "token": token_b,
        "u_hash": u_hash_b,
        "invitations": [{"email": f"twice-{suffix}@example.test", "role_id": "r1"}],
    }
    r = _json(http_client, "post", MANAGER_INVITATIONS, json=create_body)
    assert r.status_code == 200
    invite_token = r.get_json()["data"]["invitations"][0]["token"]

    r1 = _json(
        http_client,
        "post",
        CLAIM,
        json={"token": invite_token, "uuid": str(uuid.uuid4())},
    )
    assert r1.status_code == 200
    assert r1.get_json()["status"] == "success"

    r2 = _json(
        http_client,
        "post",
        CLAIM,
        json={"token": invite_token, "uuid": str(uuid.uuid4())},
    )
    assert r2.status_code == 200
    err = r2.get_json()
    assert err["status"] == "error"
    assert err.get("error") == "invitation_already_used"


def test_claim_username_in_body_is_ignored(http_client, bronevik_cached_session):
    """Claim does not set username; legacy clients may still send the key."""
    token_b, u_hash_b = bronevik_cached_session
    suffix = uuid.uuid4().hex[:12]
    r = _json(
        http_client,
        "post",
        MANAGER_INVITATIONS,
        json={
            "token": token_b,
            "u_hash": u_hash_b,
            "invitations": [{"email": f"ignore-un-{suffix}@example.test", "role_id": "r1"}],
        },
    )
    assert r.status_code == 200
    invite_token = r.get_json()["data"]["invitations"][0]["token"]
    r_claim = _json(
        http_client,
        "post",
        CLAIM,
        json={
            "token": invite_token,
            "uuid": str(uuid.uuid4()),
            "username": "this-must-not-be-stored",
        },
    )
    assert r_claim.status_code == 200
    assert r_claim.get_json()["user"]["username"] == ""


def test_manager_invitations_list_minimal_body(http_client, bronevik_cached_session):
    """List mode: only token + u_hash (no invitations key)."""
    token_b, u_hash_b = bronevik_cached_session
    r = _json(
        http_client,
        "post",
        MANAGER_INVITATIONS,
        json={"token": token_b, "u_hash": u_hash_b},
    )
    assert r.status_code == 200
    assert r.get_json()["status"] == "success"
    assert "invitations" in r.get_json().get("data", {})


def test_manager_invitations_list_empty_array(http_client, bronevik_cached_session):
    """List mode: invitations: [] is treated as list, not create."""
    token_b, u_hash_b = bronevik_cached_session
    r = _json(
        http_client,
        "post",
        MANAGER_INVITATIONS,
        json={"token": token_b, "u_hash": u_hash_b, "invitations": []},
    )
    assert r.status_code == 200
    assert r.get_json()["status"] == "success"


def test_invitation_create_accepts_snake_case_aliases(http_client, bronevik_cached_session):
    """Backend accepts first_name / last_name / middle_name on create."""
    token_b, u_hash_b = bronevik_cached_session
    suffix = uuid.uuid4().hex[:12]
    email = f"snake-{suffix}@example.test"
    r = _json(
        http_client,
        "post",
        MANAGER_INVITATIONS,
        json={
            "token": token_b,
            "u_hash": u_hash_b,
            "invitations": [
                {
                    "email": email,
                    "first_name": "Snake",
                    "last_name": "Case",
                    "middle_name": "Alias",
                }
            ],
        },
    )
    assert r.status_code == 200
    row = r.get_json()["data"]["invitations"][0]
    assert row["firstName"] == "Snake"
    assert row["lastName"] == "Case"
    assert row["middleName"] == "Alias"


def test_invitation_data_create_list_put_clear(http_client, bronevik_cached_session):
    token_b, u_hash_b = bronevik_cached_session
    suffix = uuid.uuid4().hex[:12]
    email = f"data-{suffix}@example.test"
    team_id = f"team-{suffix}"
    r = _json(
        http_client,
        "post",
        MANAGER_INVITATIONS,
        json={
            "token": token_b,
            "u_hash": u_hash_b,
            "team_id": team_id,
            "invitations": [
                {
                    "email": email,
                    "role_id": "r1",
                    "data": {"emailSent": False, "draft": True},
                }
            ],
        },
    )
    assert r.status_code == 200
    row = r.get_json()["data"]["invitations"][0]
    inv_id = row["id"]
    assert row["data"] == {"emailSent": False, "draft": True}

    r_list = _json(
        http_client,
        "post",
        MANAGER_INVITATIONS,
        json={"token": token_b, "u_hash": u_hash_b, "team_id": team_id},
    )
    assert r_list.status_code == 200
    listed = [x for x in r_list.get_json()["data"]["invitations"] if x["email"] == email][0]
    assert listed["data"] == {"emailSent": False, "draft": True}

    r_put = _json(
        http_client,
        "put",
        MANAGER_INVITATIONS,
        json={
            "token": token_b,
            "u_hash": u_hash_b,
            str(inv_id): {"data": {"emailSent": True}},
        },
    )
    assert r_put.status_code == 200
    res = r_put.get_json()["data"]["results"]
    assert len(res) == 1
    assert res[0]["invitation"]["data"] == {"emailSent": True}

    r_clear = _json(
        http_client,
        "put",
        MANAGER_INVITATIONS,
        json={
            "token": token_b,
            "u_hash": u_hash_b,
            str(inv_id): {"data": None},
        },
    )
    assert r_clear.status_code == 200
    assert r_clear.get_json()["data"]["results"][0]["invitation"]["data"] == {}


def test_invitation_batch_put_data(http_client, bronevik_cached_session):
    token_b, u_hash_b = bronevik_cached_session
    suffix = uuid.uuid4().hex[:12]
    team_id = f"team-{suffix}"
    create = _json(
        http_client,
        "post",
        MANAGER_INVITATIONS,
        json={
            "token": token_b,
            "u_hash": u_hash_b,
            "team_id": team_id,
            "invitations": [
                {"email": f"a-{suffix}@example.test", "role_id": "r1"},
                {"email": f"b-{suffix}@example.test", "role_id": "r1"},
            ],
        },
    )
    assert create.status_code == 200
    invs = create.get_json()["data"]["invitations"]
    id_a, id_b = invs[0]["id"], invs[1]["id"]

    r = _json(
        http_client,
        "put",
        MANAGER_INVITATIONS,
        json={
            "token": token_b,
            "u_hash": u_hash_b,
            str(id_a): {"data": {"sent": True}},
            str(id_b): {"data": {"sent": False}},
        },
    )
    assert r.status_code == 200
    results = r.get_json()["data"]["results"]
    assert len(results) == 2
    by_id = {x["id"]: x for x in results}
    assert by_id[id_a]["invitation"]["data"] == {"sent": True}
    assert by_id[id_b]["invitation"]["data"] == {"sent": False}


def test_invitation_batch_put_partial_not_found(http_client, bronevik_cached_session):
    token_b, u_hash_b = bronevik_cached_session
    suffix = uuid.uuid4().hex[:12]
    r = _json(
        http_client,
        "post",
        MANAGER_INVITATIONS,
        json={
            "token": token_b,
            "u_hash": u_hash_b,
            "invitations": [{"email": f"one-{suffix}@example.test", "role_id": "r1"}],
        },
    )
    oid = r.get_json()["data"]["invitations"][0]["id"]
    r2 = _json(
        http_client,
        "put",
        MANAGER_INVITATIONS,
        json={
            "token": token_b,
            "u_hash": u_hash_b,
            str(oid): {"data": {"a": 1}},
            "999999999": {"data": {}},
        },
    )
    assert r2.status_code == 200
    parts = {x["id"]: x for x in r2.get_json()["data"]["results"]}
    assert "invitation" in parts[oid]
    assert parts[999999999]["error"] == "not_found"


def test_invitation_batch_put_validation(http_client, bronevik_cached_session):
    token_b, u_hash_b = bronevik_cached_session
    r = _json(
        http_client,
        "put",
        MANAGER_INVITATIONS,
        json={"token": token_b, "u_hash": u_hash_b},
    )
    assert r.status_code == 400
    r2 = _json(
        http_client,
        "put",
        MANAGER_INVITATIONS,
        json={
            "token": token_b,
            "u_hash": u_hash_b,
            "not-a-number": {"data": {}},
        },
    )
    assert r2.status_code == 400


def test_claim_unknown_token(http_client):
    r = _json(
        http_client,
        "post",
        CLAIM,
        json={"token": "definitely-not-a-stored-token-xyz", "uuid": str(uuid.uuid4())},
    )
    assert r.status_code == 200
    body = r.get_json()
    assert body["status"] == "error"
    assert "not found" in body.get("message", "").lower()
