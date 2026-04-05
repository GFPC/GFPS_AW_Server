# GFP TIM Server (ActivityWatch-compatible API)

Бэкенд HTTP API и хранилище событий для клиентов ActivityWatch / GFP. Репозиторий рассчитан на **развёртывание как отдельный сервис** (не полный десктопный бандл ActivityWatch).

**Не ставьте сервер командой `pip install aw-server` с PyPI** — там другой пакет (оригинальный ActivityWatch). Нужен клон этого репозитория и установка **`pip install -e ./aw-core`** и **`pip install -e ./aw-server`** (на Linux при проблемах с `aw_datastore` см. [docs/DEPLOY.md](docs/DEPLOY.md) и `scripts/install_editable_compat.sh`).

## Быстрый старт

```bash
pip install -r requirements-all.txt
cp .env.example .env
# Заполните GFPS_MYSQL_* и при необходимости GFPS_HOST / GFPS_PORT
aw-server
```

Режим разработки с отдельной тестовой БД: `aw-server --testing` (или `python -m aw_server --testing`). Подробнее: [docs/LOCAL_DEV.md](docs/LOCAL_DEV.md).

Файл `.env` ищется в **корне репозитория** (рядом с каталогами `aw-core` и `aw-server`) — см. `aw_server.config`. При необходимости задайте **`GFPS_ENV_FILE`** с абсолютным путём к `.env`.

## Документация

| Документ | Содержание |
|----------|------------|
| [aw-server/README.md](aw-server/README.md) | Установка, Swagger, ссылки на клиентский флоу |
| [docs/API_V1_RU.md](docs/API_V1_RU.md) | Описание эндпоинтов v1 (менеджер, инвайты, бакеты) |
| [docs/GFP_CLIENT_INVITATION_FLOW.md](docs/GFP_CLIENT_INVITATION_FLOW.md) | Приглашения и claim для клиентов и админки |
| [docs/LOCAL_DEV.md](docs/LOCAL_DEV.md) | Локальный запуск, тестовая БД, скрипт инвайт-токена |
| [docs/DEPLOY.md](docs/DEPLOY.md) | Выкладка на боевой сервер (systemd, переменные, тесты) |

Краткая отсылка в корне: [v1.md](v1.md) → полный текст в `docs/API_V1_RU.md`.

## Тесты

Из корня репозитория (нужен доступ к MySQL, см. `conftest`):

```bash
python -m pytest tests/ -v
```

## Лицензия и upstream

См. исходные пакеты `aw-core` / `aw-server` и файлы `LICENSE` в подпроектах при необходимости.
