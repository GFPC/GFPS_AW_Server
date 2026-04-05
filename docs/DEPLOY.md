# Выкладка на боевой сервер

## 1. Код и зависимости

### Важно: не путать с PyPI

Команда **`pip install aw-server`** (без пути к каталогу) тянет пакет **с PyPI** — это **upstream ActivityWatch**, не этот GFP-форк.

Нужно ставить **только из клонированного репозитория**, в режиме **editable** (`-e`), указывая пути **`./aw-core`** и **`./aw-server`**.

### Установка (рекомендуется на всех окружениях)

На Linux с **setuptools 80+** обычный `pip install -e ./aw-core` иногда даёт `ImportError: Datastore ... (unknown location)` — используйте **режим совместимости** editable:

```bash
cd /path/to/GFPS_AW_Server
python3 -m venv venv
source venv/bin/activate   # или: . venv/bin/activate
pip install -U pip wheel
pip uninstall -y aw-core aw-server 2>/dev/null || true
pip install --no-cache-dir -e ./aw-core --config-settings editable_mode=compat
pip install --no-cache-dir -e ./aw-server --config-settings editable_mode=compat
```

Из корня репозитория можно так: `bash scripts/install_editable_compat.sh` (с активированным venv).

Альтернатива при той же ошибке: **`pip install 'setuptools>=61,<70'`** и снова **`pip install -r requirements-all.txt`** (там `-e ./aw-core` и `-e ./aw-server`).

### 1.1 Ошибка `cannot import name 'Datastore' from 'aw_datastore' (unknown location)`

См. выше: **editable_mode=compat** или откат **setuptools** ниже 80, затем переустановка `-e` из **локальных** `./aw-core` и `./aw-server`.

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

Пример юнита: [deploy/systemd/gfps.service](../deploy/systemd/gfps.service).

- Правьте **`User`**, **`Group`**, **`WorkingDirectory`**, **`ExecStart`**, путь к **`EnvironmentFile=-/path/to/.env`**.
- После правок: `sudo systemctl daemon-reload`, `sudo systemctl enable --now gfps.service`.
- Логи: `journalctl -u gfps.service -f`.

Точка входа: консольная команда **`aw-server`** (появляется в venv после **`pip install -e ./aw-server`** из **этого** клона, не с PyPI) или **`python -m aw_server`**.

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
