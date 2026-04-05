# aw-server (GFP backend)

HTTP API and storage for ActivityWatch-compatible clients. This tree is used as a **standalone backend** (not the full ActivityWatch desktop bundle).

## Install

From the repository root (virtualenv recommended). **Do not `pip install aw-server` from PyPI** — that is upstream ActivityWatch. Use editable installs from this repo only:

```bash
pip install -r requirements-all.txt
```

Copy `.env.example` to `.env` in the **repository root** and set `GFPS_*` variables (MySQL, Bronevik URL, bind address/port). Alternatively set `GFPS_ENV_FILE` to an absolute path to a `.env` file.

Or:

```bash
pip install -e ./aw-core
pip install -e ./aw-server
```

Run (после `pip install -e ./aw-core` и `pip install -e ./aw-server`, или `pip install -r ../requirements-all.txt` из корня репозитория):

```bash
aw-server
```

Эквивалент: `python -m aw_server`. Файл `.env` подхватывается из **корня репозитория** (родитель каталога `aw-server`), см. `aw_server.config`; иначе задайте `GFPS_ENV_FILE`.

Development / separate DB: `aw-server --testing`. Локальный запуск и dev invitation token: [`docs/LOCAL_DEV.md`](../docs/LOCAL_DEV.md).

## API

With the server running, browse `/api/` for the REST browser (port from config, default in `aw_server` config).

**User objects** (workers, GFP claim, manager user APIs) expose: `username`, `firstName`, `lastName`, `middleName`, `email`, `role_id`, `team` (array of team ids), `client_version`, plus existing fields. JSON accepts the same camelCase keys (or legacy `first_name` / `last_name` / `middle_name` where noted in Swagger).

Upstream reference: [ActivityWatch API docs](https://docs.activitywatch.net/en/latest/api.html).

Russian API notes: [`docs/API_V1_RU.md`](../docs/API_V1_RU.md) (shortcut: [`v1.md`](../v1.md)). **Client apps (invitation + claim flow):** [`docs/GFP_CLIENT_INVITATION_FLOW.md`](../docs/GFP_CLIENT_INVITATION_FLOW.md). **Production deploy:** [`docs/DEPLOY.md`](../docs/DEPLOY.md). Manager invitations use a single **POST** `/api/1/manager/invitations`: without a non-empty `invitations` array it lists; with rows in `invitations` it creates. Installer **tokens** are random 16-byte secrets encoded as **Base58**; the database stores only **SHA-256** of those bytes (legacy plaintext tokens are migrated to the same lookup scheme).

## Production (systemd)

See `deploy/systemd/` in the repository root.
