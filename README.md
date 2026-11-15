# hermes-max-plugin

Шлюз мессенджера [MAX](https://max.ru) для [Hermes Agent](https://github.com/NousResearch/hermes-agent):
long polling, текст, медиа в обе стороны, группы, кнопки подтверждений и белый
список по `user_id`. Разработан под hermes v0.23.0 (коммит `2bd378ed1b`).

## Возможности

- Входящие: `GET /updates` с сохранённым `marker`, дедупликация, backoff, watchdog.
- Исходящие: `POST /messages` с HTML-разметкой и разбиением по 4000 символов.
- Медиа: приём `image`/`video`/`audio`/`file`, отправка через `POST /uploads`.
- Кнопки: подтверждения выполнения команд, clarify, choice picker (`POST /answers`).
- Группы: ответ только при обращении (упоминание, имя, алиас, reply, `/команда`).
- Белый список: `MAX_ALLOWED_USERS` — CSV `user_id`; незнакомец получает свой ID
  командой `/whoami`.

## Быстрая установка

```sh
# 1. CA Минцифры (без него platform-api2.max.ru не проходит проверку TLS)
mkdir -p ~/.hermes/max/certs
curl -fsSL https://gu-st.ru/content/lending/russian_trusted_root_ca_pem.crt \
  -o ~/.hermes/max/certs/russian_trusted_root_ca_pem.crt
echo "936a43fea6e8e525bcc0f81acd9c3d21b4fc4b9b68acea7906d698005afc6504  $HOME/.hermes/max/certs/russian_trusted_root_ca_pem.crt" | shasum -c -

# 2. Плагин
hermes plugins install zaharinea/hermes-max-plugin --ref <FULL_SHA> --enable --yes-deps
hermes plugins validate ~/.hermes/plugins/max-platform
hermes plugins doctor ~/.hermes/plugins/max-platform --ci

# 3. Переменные в ~/.hermes/.env
#    MAX_BOT_TOKEN=...
#    MAX_ALLOWED_USERS=111111111
#    MAX_HOME_CHANNEL=333333333

hermes gateway restart
hermes gateway status
```

Проверка API:

```sh
curl --cacert ~/.hermes/max/certs/russian_trusted_root_ca_pem.crt \
  -H "Authorization: $MAX_BOT_TOKEN" https://platform-api2.max.ru/me
```

## Переменные

| Переменная | По умолчанию | Смысл |
|---|---|---|
| `MAX_BOT_TOKEN` | — | Токен бота (обязателен) |
| `MAX_ALLOWED_USERS` | пусто | CSV `user_id`; `*` — все |
| `MAX_ALLOW_ALL_USERS` | `false` | Разрешить всех (только для отладки) |
| `MAX_GROUP_ALLOWED_CHATS` | пусто | CSV `chat_id` групп; sender всё равно должен быть в allowlist |
| `MAX_REQUIRE_MENTION` | `true` | В группах отвечать только при обращении |
| `MAX_BOT_ALIASES` | пусто | Дополнительные имена бота для групп |
| `MAX_CA_CERT_PATH` | `$HERMES_HOME/max/certs/russian_trusted_root_ca_pem.crt` | PEM Минцифры |
| `MAX_API_BASE` | `https://platform-api2.max.ru` | База API |
| `MAX_HOME_CHANNEL` | пусто | `chat_id` для cron и уведомлений |
| `MAX_FORMAT` | `html` | `html`, `markdown` или `plain` |
| `MAX_NOTIFY` | `true` | Просить клиент уведомлять о сообщении |
| `MAX_POLL_TIMEOUT` | `30` | Таймаут long polling (0–90 с) |
| `MAX_POLL_LIMIT` | `100` | Апдейтов за запрос |
| `MAX_POLL_PAUSE_SECONDS` | `0.5` | Пауза между запросами (мин. 0.3) |
| `MAX_DEDUP_TTL_SECONDS` | `300` | Окно дедупликации |
| `MAX_BOOTSTRAP_MAX_AGE_SECONDS` | `300` | Первый холодный старт: не обрабатывать старое |
| `MAX_DOWNLOAD_MAX_MB` | `100` | Лимит входящего вложения |
| `MAX_SEND_RATE_PER_CHAT` | `2` | Сообщений в секунду в чат |
| `MAX_GLOBAL_RPS` | `30` | Запросов в секунду всего |
| `MAX_HTTP_TIMEOUT` | `30` | Таймаут HTTP (не polling) |
| `MAX_BACKOFF_BASE_SECONDS` | `1` | Начальный backoff |
| `MAX_BACKOFF_MAX_SECONDS` | `60` | Максимальный backoff |
| `MAX_CALLBACK_TTL_SECONDS` | `3600` | Срок жизни токена кнопки |
| `MAX_REVEAL_USER_ID_COOLDOWN` | `3600` | Пауза между ответами `/whoami` |
| `MAX_REGISTER_COMMANDS` | `true` | Показывать меню команд бота (`PATCH /me/commands`) |
| `MAX_DEBUG_UPDATES` | `false` | Писать тип и структуру каждого update (без текста), только для отладки |
| `MAX_USE_BUILTIN_TRANSCRIPTION` | `true` | Брать расшифровку из вложения MAX; `false` — всегда локальный STT |
| `MAX_STATE_PATH` | `$HERMES_HOME/max/state.json` | Файл marker/журнала |

## config.yaml

```yaml
gateway:
  platforms:
    max:
      enabled: true
      extra:
        allowed_users: "111111111"
        require_mention: true
        format: html
        ca_cert_path: "~/.hermes/max/certs/russian_trusted_root_ca_pem.crt"
```

`plugins.enabled` должен содержать `max-platform` (это делает
`hermes plugins enable max-platform`; при установке с `--enable` — сразу).

## Белый список и `/whoami`

Bot API MAX не отдаёт номер телефона. Владелец пишет боту `/whoami` и получает
свой `user_id`; его нужно внести в `MAX_ALLOWED_USERS`. Остальные сообщения
незнакомцев игнорируются молча. Кнопки подтверждений авторизуются только по
`MAX_ALLOWED_USERS` — членство в разрешённой группе прав на них не даёт.

## Группы

- Бот должен быть **администратором** группы/канала, иначе MAX не доставляет все
  события (`bot_added` → предупреждение в логе).
- `MAX_GROUP_ALLOWED_CHATS` сужает список групп; sender проверяется в любом случае.
- `MAX_REQUIRE_MENTION=true`: срабатывают `@username`, имя бота, алиасы, ответ на
  сообщение бота и команды, начинающиеся с `/`.

## Гарантии доставки

`marker` сохраняется после обработки батча, а каждый обработанный update
записывается в журнал `state.json` сразу. Доставка — **at-least-once**: после
падения возможен повтор последнего update (дубли режутся журналом), exactly-once
не гарантируется.

## Ограничения

- Голосовые при long polling могут приходить разреженно или не приходить вовсе —
  это ограничение MAX. Когда аудио доставляется, шлюз сначала берёт готовую
  расшифровку из поля `transcription` вложения (если MAX её прислал), а иначе
  отдаёт `audio` в автоматический STT Hermes (`MessageType.VOICE`). Аудиофайлы,
  отправленные документом, тоже распознаются, если MIME начинается с `audio/`.
- Лимиты MAX: 4000 символов, ~2 сообщения/с в чат, 30 rps глобально.
- Webhook не используется: long polling выбран из-за NAT и требования CA.

## Troubleshooting

| Симптом | Причина / решение |
|---|---|
| `CERTIFICATE_VERIFY_FAILED`, `ssl_verify_result 20` | Нет Russian Trusted Root CA — скачать PEM и указать `MAX_CA_CERT_PATH` |
| `401` на `/me` | Неверный токен; в заголовке должен быть сырой токен без `Bearer` |
| Пустые `updates` | Бот не админ в группе; или `marker` ушёл вперёд (проверить `state.json`) |
| Кнопки не нажимаются | Проверить, что sender в `MAX_ALLOWED_USERS`; срок токена истёк |
| Плагин не загрузился | `hermes plugins list`, `hermes plugins doctor <путь> --ci` |

## Разработка

Коммиты — [Conventional Commits](https://www.conventionalcommits.org/) без ключа
задачи: `feat(max): …`, `fix(max): …`, `ci(release): …`. Релизный workflow
сам выбирает версию по типам коммитов (feat → minor, fix/perf/build/docs →
patch, `!` → major) и создаёт тег `vX.Y.Z` с заметками GitHub.

```sh
pytest -q
ruff check .
python3 -m compileall -q .
```

Тесты импортируют `gateway.*` из hermes-agent; полный прогон — на checkout того
же коммита в отдельном окружении (`scripts/run_tests.sh`, временный
`HERMES_HOME`). Матрица сквозных проверок — [`docs/e2e-scenarios.md`](docs/e2e-scenarios.md).

## Лицензия

MIT (см. `LICENSE`).
