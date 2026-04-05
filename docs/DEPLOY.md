# Выкладка на боевой сервер

## 1. Код и зависимости

- Клонировать репозиторий в каталог деплоя (например `/opt/gfps-aw-server`).
- Создать виртуальное окружение и установить зависимости:

  ```bash
  python3 -m venv venv
  ./venv/bin/pip install -U pip
  ./venv/bin/pip install -r requirements-all.txt
  ```

- Убедиться, что `aw-core` и `aw-server` установлены в editable-режиме, если так настроен ваш `requirements-all.txt` (обычно `pip install -e ./aw-core -e ./aw-server` из корня).

## 2. Конфигурация

- Скопировать **`.env.example`** → **`.env`** в **корне** репозитория (рядом с `aw-core` / `aw-server`), либо задать **`GFPS_ENV_FILE`** на абсолютный путь к `.env`.
- Задать как минимум:
  - **`GFPS_MYSQL_HOST`**, **`GFPS_MYSQL_PORT`**, **`GFPS_MYSQL_USER`**, **`GFPS_MYSQL_PASSWORD`**, **`GFPS_MYSQL_DATABASE`**
  - **`GFPS_HOST`** / **`GFPS_PORT`** — привязка HTTP (часто за reverse-proxy достаточно `127.0.0.1` и локальный порт)
  - **`GFPS_BRONEVIK_URL`** — URL API Bronevik для менеджерских эндпоинтов
- **Не** коммитить `.env` в git (файл в `.gitignore`).
- Для **`GFPS_CORS_ORIGINS`** в продакшене задайте конкретные origin-ы фронта (избегайте `*` при публичном доступе).

Переменные читает `aw_server.config` при старте; при использовании systemd можно дублировать критичные значения в `Environment=` или `EnvironmentFile` (см. юнит ниже).

## 3. systemd

Пример юнита: [deploy/systemd/gfps-aw-server.service](../deploy/systemd/gfps-aw-server.service).

- Правьте **`User`**, **`Group`**, **`WorkingDirectory`**, **`ExecStart`**, путь к **`EnvironmentFile=-/path/to/.env`**.
- После правок: `sudo systemctl daemon-reload`, `sudo systemctl enable --now gfps-aw-server`.
- Логи: `journalctl -u gfps-aw-server -f`.

Точка входа: консольная команда **`aw-server`** (ставится из `aw-server` пакета при `pip install -e ./aw-server`) или эквивалент **`python -m aw_server`** из активированного venv.

## 4. База данных

- Сервер при старте выполняет **автомиграции** схемы MySQL (колонки пользователей, инвайтов и т.д.). Учётная запись MySQL должна иметь права **CREATE/ALTER** на выбранную базу при первом запуске новой версии.
- Регулярные **бэкапы** БД — отдельная задача администратора (mysqldump, репликация и т.п.).

## 5. HTTPS и сеть

- Рекомендуется **TLS** на границе (nginx, Caddy, traefik) с проксированием на локальный порт сервера.
- Клиенты должны слать корректный заголовок **`Host`** (см. проверку в коде сервера).

## 6. Проверка после выкладки

- Открыть **`/api/`** (Swagger) на том же хосте/порту.
- Прогнать интеграционные тесты на стенде с тестовой БД:

  ```bash
  export GFPS_TESTING_MYSQL_DATABASE=...
  python -m pytest tests/ -v
  ```

## 7. Демо-страница

Маршрут **`/demo/manager-invitations`** — только для отладки списка приглашений; при необходимости ограничьте доступ на уровне reverse-proxy или отключите раздачу статики в продакшене по политике безопасности.
