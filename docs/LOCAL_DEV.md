# Локальный запуск сервера и токен приглашения

## Что нужно

- **MySQL** (локально или Docker), доступ из `.env` в корне репозитория.
- Виртуальное окружение с установленными **`aw-core`** и **`aw-server`** (`pip install -e ./aw-core` и `pip install -e ./aw-server` из корня).

Переменные для **тестовой** БД (как у `aw-server --testing`):

- `GFPS_TESTING_MYSQL_HOST`, `GFPS_TESTING_MYSQL_PORT`, `GFPS_TESTING_MYSQL_USER`, `GFPS_TESTING_MYSQL_PASSWORD`
- `GFPS_TESTING_MYSQL_DATABASE` (по умолчанию `activitywatch_test`)

Порт HTTP в testing: **`GFPS_TESTING_PORT`** (по умолчанию **5777**), хост **`GFPS_TESTING_HOST`** (часто `localhost`).

---

## 1. Запустить сервер

Из корня репозитория с активированным venv (после `pip install -r requirements-all.txt`):

```bash
aw-server --testing
```

или

```bash
python -m aw_server --testing
```

Сервер слушает адрес из `.env` (например `http://127.0.0.1:5777`). Swagger: `http://127.0.0.1:5777/api/`.

Клиент должен слать запросы на этот же host/port и корректный заголовок **`Host`** (как у обычного HTTP-клиента к этому URL).

---

## 2. Получить инвайт-токен без Bronevik

Чтобы не настраивать Bronevik, можно **создать строку приглашения в БД** скриптом (тот же MySQL, что и `--testing`):

```bash
python scripts/dev_issue_invitation_token.py
```

Токен печатается в stdout. Записать в файл:

```bash
python scripts/dev_issue_invitation_token.py -o invitation.token
```

Файл `invitation.token` в корне — только для локальной отладки; он в **`.gitignore`**, в репозиторий не коммитьте.

Опции: `--email`, `--team`.

Дальше этот текст положите в конфиг клиента или вручную вставьте в поле ввода токена.

---

## 3. Проверить claim вручную

После запуска сервера:

```bash
curl -s -X POST "http://127.0.0.1:5777/api/0/gfps/invitations/claim" ^
  -H "Content-Type: application/json" ^
  -H "Host: 127.0.0.1:5777" ^
  -d "{\"token\":\"<BASE58_FROM_FILE>\",\"uuid\":\"00000000-0000-4000-8000-000000000001\"}"
```

(В PowerShell удобнее `Invoke-RestMethod` или одинарные кавычки для JSON в bash.)

---

## 4. Альтернатива: токен через API менеджера

Нужны рабочие **`token`** и **`u_hash`** Bronevik **или** предварительная запись в кэше авторизации (как в интеграционных тестах). Для чисто локальной отладки проще скрипт из §2.
