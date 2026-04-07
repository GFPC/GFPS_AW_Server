## API v1

### Пользователи (Users)

- POST `/api/1/manager/workers`
  - Тело запроса (JSON):
    - `token` — ibronevik auth
    - `u_hash` — ibronevik auth
    - `team_id` — строка идентификатора команды (фильтр списка сотрудников)
  - Действие: возвращаются все сотрудники указанной команды. В каждом объекте сотрудника поля **`team`** и **`role_id`** отражают **только эту** команду (связка из `user_team_role` для запрошенного `team_id`); участие сотрудника в других командах **не** подставляется в ответ.
  - Ответ: `application/json`

#### Пример ответа

```json
{
  "status": "success",
  "data": {
    "workers": [
      {
        "id": 1,
        "username": "user1",
        "uuid": "user-uuid-123",
        "firstName": "Иван",
        "lastName": "Иванов",
        "middleName": "Иванович",
        "created": "2025-02-26T08:59:00+00:00",
        "data": {}
      },
      {
        "id": 2,
        "username": "user2",
        "uuid": "user-uuid-456",
        "firstName": null,
        "lastName": null,
        "middleName": null,
        "created": "2025-02-26T09:15:00+00:00",
        "data": {}
      }
    ]
  }
}
```

#### Формат ответа (структура)

```json
{
  "status": "success | error",
  "data": {
    "workers": [
      {
        "id": "int",
        "username": "string",
        "uuid": "string",
        "firstName": "string | null",
        "lastName": "string | null",
        "middleName": "string | null",
        "created": "string",
        "data": "object"
      }
    ]
  }
}
```

### Приглашения сотрудников (GFP)

- **POST** `/api/1/manager/invitations` (Bronevik: `token`, `u_hash` в JSON) — один эндпоинт:
  - **Список:** тело содержит только `token`, `u_hash` и опционально `team_id` (фильтр). Поле `invitations` отсутствует, равно `null` или **пустой массив** `[]` — тогда возвращается `data.invitations[]` (все или по `team_id`).
  - **Создание:** в теле передан **непустой** массив `invitations[]`; опционально `team_id` — общий для пачки. Каждая строка:
    - `email` (обязательно),
    - `role_id` — строка внешней роли,
    - `firstName`, `lastName`, `middleName` — ФИО для предзаполнения (допускаются также `first_name` / `last_name` / `middle_name`),
    - опционально **`data`** — произвольный JSON-объект (opaque для сервера: флаги рассылки, черновики и т.д.).
  - Ответ при создании: `data.invitations[]` с полями в camelCase, в т.ч. `token` — секрет для установщика (**Base58**, ~22 символа; в БД хранится только SHA-256), и **`data`** (объект или `{}`).
  - Ответ при **списке**: поле **`token`** — тот же Base58-секрет, что и при создании (чтобы менеджеры могли передать сотруднику при утере). Для **старых** строк, созданных до появления хранения секрета в БД, может быть **`null`**.

- **PUT** `/api/1/manager/invitations` (тот же путь, что и POST списка/создания; Bronevik в JSON) — обновление поля **`data`**: кроме `token` и `u_hash` каждый ключ — **строковый числовой id** приглашения (`"12"`, `"13"`), значение — **`{"data": ...}`** (JSON-объект или `null` для очистки; заменяет целиком). Один id или несколько в одном запросе. Опциональный `team_id` игнорируется. Ответ: `{"status":"success","data":{"results":[...]}}` — по каждому id либо `invitation`, либо `error: not_found`. Некорректное тело — `400`.

- **POST** `/api/0/gfps/invitations/claim` (без Bronevik)
  - Тело: только **`token`** и **`uuid`**. Поле **`username` не задаётся** при claim (в БД остаётся пустой строкой); смена — через **`PUT /api/0/user`** (сотрудник) или **`PUT /api/1/manager/users/<id>`** (менеджер).
  - Успех: `status: "success"`, объекты `user` и `invitation`. У пользователя из инвайта подставляются `firstName`, `lastName`, `middleName`, `email`.

- **PUT** `/api/1/manager/users/<id>` — обновление профиля; в теле можно передать `firstName`, `lastName`, `middleName` (или snake_case), `username`, `email`, `client_version`, `data`. Чтобы **сменить роль сотрудника в команде**, передайте **`role_id`** и **`team_id`** (тот же идентификатор команды, что и для списка сотрудников): обновится только строка `user_team_role` для этой пары пользователь–команда; остальные команды пользователя не затрагиваются. Если передан только `role_id` без `team_id` и без массива `team`, сервер вернёт ошибку (нужно явно указать контекст команды или список `team`).

- **DELETE** `/api/1/manager/users/<id>` — удаление пользователя и его бакетов. В теле JSON: **`token`**, **`u_hash`** (Bronevik). Приглашения, которые были **claimed** и привязаны к пользователю, переводятся в **`revoked`** (строка сохраняется для аудита, поле `revoked_at`). Приглашения в статусе **`superseded`** с тем же нормализованным email снова становятся **`pending`** (email освобождён).

- **PUT** `/api/0/user` (публичный GFP, без Bronevik) — обновление полей пользователя по **`uuid`** (строка). Для периодического отчёта версии клиента: тело вида `{"uuid": "...", "client_version": "1.2.3"}`; ответ `{"status":"ok"}`. Отдельного эндпоинта только для версии нет.

#### Статусы приглашения (список и JSON)

| Статус | Смысл |
|--------|--------|
| `pending` | Ожидает установки по токену |
| `claimed` | Установка выполнена, пользователь привязан |
| `superseded` | Другой инвайт с тем же email уже занял регистрацию; при удалении пользователя менеджером строка может снова стать `pending` |
| `revoked` | После `claimed` пользователь удалён менеджером; строка для аудита (`revoked_at`) |

В ответах также могут быть поля `superseded_at`, `installed`, `installed_at`, вложенный `user` (если есть связь).

### Букеты (Buckets)

- POST `/api/1/manager/buckets`
  - Тело запроса (JSON):
    - `users` (array<int>, или array<string>, или "all", обязателен) — список ID пользователей
      - когда =all, возвращаются букеты для всех сотрудников команды
    - `token` — ibronevik auth
    - `u_hash` — ibronevik auth
  - Действие: возвращаются букеты для указанных пользователей
  - Ответ: `application/json`

#### Пример ответа

```json
{
  "status": "success",
  "data": {
    "buckets": {
      "1": {
        "aaeaaeaaeaaaeaaeaae": {
          "id": "0",
          "created": "2025-02-26T08:59:00+00:00",
          "type": "aaa",
          "client": "aaa",
          "hostname": "aaa",
          "data": {}
        }
      },
      "2": {},
      "3": {}
    }
  }
}
```

#### Формат ответа (структура)

```json
{
  "status": "success | error",
  "data": {
    "buckets": {
      "<userId>": {
        "<bucketHashKey>": {
          "id": "string",
          "created": "string",
          "type": "string",
          "client": "string",
          "hostname": "string",
          "data": "object"
        }
      }
    }
  }
}
```

### События (Events)

- POST `/api/1/manager/buckets/events`
  - Тело:
    - `buckets` — (array<string>, обязателен) список хеш-ключей букетов
    - `limit` — (int, необязателен) количество событий на каждый букет; по умолчанию без лимита
    - `start` — (string, ISO 8601, необязателен) нижняя граница по времени
    - `end` — (string, ISO 8601, необязателен) верхняя граница по времени
    - `token` — ibronevik auth
    - `u_hash` — ibronevik auth
  - Действие:
    - Возвращаются события для указанных букетов. `start`/`end` принимаются с любой временной зоной и приводятся к UTC на сервере. Пример:

```json
{
  "status": "success",
  "data": {
    "events": {
      "aaeaaeaaeaaaeaaeaae": [
        {
          "id": 0,
          "timestamp": "2360-02-29T17:15:12+00:00",
          "duration": 56.0,
          "data": {}
        }
      ]
    }
  }
}
```

Формат:

```json
{
  "status": "success | error",
  "data": {
    "events": {
      "<bucketHashKey>": [
        {
          "id": "int",
          "timestamp": "string",
          "duration": "float",
          "data": "object"
        }
      ]
    }
  }
}
```

#### Параметры времени и TZ

- Поля `start` и `end` принимают строки в ISO 8601, например: `"2025-10-13 14:35:25+07:00"`.
- Сервер конвертирует любую указанную TZ в UTC перед фильтрацией.
- Микросекунды нормализуются к миллисекундам; верхняя граница включает следующий миллисекундный тик.

### Подсчёт количества событий

- POST `/api/1/manager/buckets/events/count`
  - Тело:
    - `buckets` — (array<string>, обязателен) список хеш-ключей букетов
    - `token`, `u_hash` — ibronevik auth
  - Действие: возвращает количество событий по каждому хеш-ключу
  - Ответ (пример):

```json
{
  "status": "success | error",
  "data": {}
}
```

### Общие правила

- Все временные значения в ответах возвращаются в формате ISO 8601 и в UTC.
- Текстовые JSON-поля в данных событий/бакетов/пользователей хранятся и передаются в UTF‑8; допускаются любые Unicode-символы.
- В случае ошибки авторизации на Bronevik возвращается `{ "status": "error", "message": "unauthorized access" }`.
- Content-Type: `application/json`

### Подсказки по интеграции

- Расширенная фильтрация событий — метод `/api/0/query` (см. документацию ActivityWatch).
- Клиент ActivityWatch: в настройках GFP указать хост и порт сервера (по умолчанию порт из `.env`, часто `5700`).
- Репозиторий клиента: [GFPC/AWClient](https://github.com/GFPC/AWClient).
- Пример базового URL API: `http://<host>:<port>/api/1/manager/workers` (в продакшене — только HTTPS).
