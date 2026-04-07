# Приглашения GFP — кратко для SaaS

Документ можно целиком переслать разработчику фронта / вики.

## Эндпоинты

| Кто | Метод | Назначение |
|-----|--------|------------|
| Менеджер | `POST /api/1/manager/invitations` | Список или создание (один URL, разное тело). В JSON всегда Bronevik: `token` + `u_hash`. |
| Менеджер | `PUT /api/1/manager/invitations` | Обновить `data`: `token`, `u_hash` и пары `"<id>": {"data": ...}` (одна строка или несколько). |
| Сотрудник | `POST /api/0/gfps/invitations/claim` | Активация: секретный `token` инвайта + стабильный `uuid` устройства. |

**Заголовки:** `Content-Type: application/json`, корректный `Host`; в проде — HTTPS.

---

## Флоу

1. **Создание:** менеджер шлёт непустой `invitations[]` → строки с `status: pending`, `installed: false`, у каждой свой секретный `token` (Base58). В каждой строке опционально **`data`** — произвольный JSON (например `{ "emailSent": false }`).
2. **Список:** тот же `POST` без режима создания (`invitations` нет / `null` / `[]`) → таблица: `status`, `token`, **`data`** (объект или `{}`), `installed`, `user` / `user_id` и т.д.
3. **Claim:** сотрудник один раз вызывает claim → эта строка переходит в `claimed`, заполняются `installed`, `installed_at`, профиль в `user`.
4. **Удаление пользователя** (`DELETE /api/1/manager/users/<id>` с Bronevik): приглашения, привязанные к пользователю, → `revoked`. При освобождении email сервер может вернуть **другие** строки `superseded` → `pending`.

---

## Статусы (`status`)

| Значение | Смысл |
|----------|--------|
| `pending` | Инвайт выдан, по нему ещё можно пройти claim (если email не занят). |
| `claimed` | Установка выполнена, пользователь создан. |
| `superseded` | Инвайт недействителен: другой выиграл по тому же email или email занят при claim. |
| `revoked` | Раньше было `claimed`, пользователь удалён менеджером; строка для аудита (`revoked_at`). |

Поля `installed`, `installed_at`, `user` частично дублируют смысл; для UI ориентируйтесь на `status`.

---

## Переходы

- **Создание:** → `pending`
- **Успешный claim:** `pending` → `claimed`; остальные `pending` с **тем же нормализованным email** → `superseded`
- **Гонка / email занят при claim:** `pending` → `superseded` (ошибка вида `email_taken`)
- **Повторный claim по тому же токену:** статус уже не `pending` → `invitation_already_used` (эта строка не возвращается в `pending`)
- **Удаление user:** `claimed` → `revoked`; при освобождении email возможен `superseded` → `pending` для старых строк с тем email

---

## `POST /api/1/manager/invitations`

### Список — тело запроса

```json
{
  "token": "<bronevik_api_token>",
  "u_hash": "<bronevik_u_hash>",
  "team_id": "optional-team-id"
}
```

`team_id` опционален (фильтр по команде).

### Список — пример успешного ответа

```json
{
  "status": "success",
  "data": {
    "invitations": [
      {
        "id": 42,
        "token": "3vKx9mN2pQrL8wZyAbcDEfG",
        "team_id": "sales-1",
        "email": "user@company.com",
        "role_id": "analyst",
        "firstName": "Иван",
        "lastName": "Иванов",
        "middleName": "Петрович",
        "status": "pending",
        "installed": false,
        "installed_at": null,
        "superseded_at": null,
        "revoked_at": null,
        "user_id": null,
        "user": null,
        "created": "2026-04-05T12:00:00+00:00",
        "data": {}
      }
    ]
  }
}
```

### Создание — тело запроса

```json
{
  "token": "<bronevik_api_token>",
  "u_hash": "<bronevik_u_hash>",
  "team_id": "sales-1",
  "invitations": [
    {
      "email": "newuser@company.com",
      "role_id": "analyst",
      "firstName": "Мария",
      "lastName": "Сидорова",
      "middleName": "Ивановна",
      "data": { "emailSent": false }
    }
  ]
}
```

### Создание — ответ

Тот же каркас: `status`, `data.invitations[]`. У созданных строк будет `token` (Base58); тот же секрет затем отдаётся в списке.

### Обновление `data` — `PUT /api/1/manager/invitations`

Тело: **`token`**, **`u_hash`**, затем для каждой строки приглашения ключ — **строка с числовым id** (`"12"`), значение — **`{"data": ...}`** (объект или `null` для очистки; заменяет целиком). Можно передать **один** id или **несколько**. Поле **`team_id`**, если передано, **игнорируется**.

```json
{
  "token": "<bronevik_api_token>",
  "u_hash": "<bronevik_u_hash>",
  "12": { "data": { "emailSent": true, "sentAt": "2026-04-05T15:00:00Z" } },
  "13": { "data": { "emailSent": false } }
}
```

Ответ `200`: `data.results[]` — по каждому id либо `{ "id", "invitation" }`, либо `{ "id", "error": "not_found" }`. Неверное тело — **400**.

### Ошибка авторизации (пример)

```json
{
  "status": "error",
  "message": "unauthorized access"
}
```

---

## `POST /api/0/gfps/invitations/claim` (сотрудник)

```json
{
  "token": "<base58 из списка или создания>",
  "uuid": "550e8400-e29b-41d4-a716-446655440000"
}
```

---

## UI SaaS (кратко)

- **`pending`** — инвайт можно активировать, `token` показать/скопировать сотруднику.
- **`claimed`** — установка уже была.
- **`superseded`** — ссылка недействительна (дубликат email / гонка).
- **`revoked`** — пользователь удалён, строка историческая.

---

## Связанные документы в репозитории

- Подробный флоу клиента: [GFP_CLIENT_INVITATION_FLOW.md](GFP_CLIENT_INVITATION_FLOW.md)
- API v1 (рус.): [API_V1_RU.md](API_V1_RU.md)
