# s3syncer

[English version](../README.md)

`s3syncer` — HTTP-прокси, который стоит перед несколькими S3-совместимыми хранилищами и поддерживает их синхронными. Клиенты работают с ним как с обычным S3. Запись идёт во все бэкенды, чтение при недоступности primary (или отсутствии объекта на нём) переключается на backup, а всё, что не удалось реплицировать сразу, попадает в очередь и досинхронизируется в фоне.

## Возможности

- **Отказоустойчивость** — автоматический переход на backup, если primary недоступен или в нём нет объекта.
- **Репликация записи** — небольшие PUT пишутся во все бэкенды параллельно; крупные стримятся на primary, а на backup их копирует фоновый worker. Неудавшиеся копии ставятся в очередь на повтор.
- **Приоритетная очередь** — приоритеты HIGH/MED/LOW, дедупликация, повторы, блокировки по объектам.
- **Два режима** — `single` (один экземпляр, локальная SQLite) и `cluster` (несколько экземпляров на общем Redis).
- **Resync / bisync** — односторонняя и двусторонняя полная синхронизация как фоновые задачи (CLI или REST API).
- **Совместимость с S3** — подписи AWS V2/V4 (включая presigned URL и потоковые загрузки `aws-chunked` с контрольными суммами в trailer), запросы `Range` и условные (`If-Match`, `If-None-Match`, `If-(Un)Modified-Since`), листинг с delimiter, CopyObject, симлинки GeeseFS (`--symlink-target`) и s3fs (`mode=41471`).
- **Контроль доступа** — один ключ на виртуальный бакет, действует только для него; опционально анонимное чтение.
- **Эксплуатация** — метрики Prometheus, JSON-логи, лимит памяти для загружаемых тел, Helm-чарт, multi-arch образ.
- Один статический бинарник (`CGO_ENABLED=0`, pure-Go SQLite).

## Как это работает

```
 клиент (aws cli / mc / s3fs / GeeseFS)
        │
        ▼
 ┌────────────────────────────────────────────┐
 │ s3syncer                                   │
 │  auth (SigV2/V4) ──► replicator            │
 │                         │        │         │
 │                   очередь        │         │
 │          SQLite (single) / Redis (cluster) │
 │                         │        │         │
 │                      worker  task manager  │
 └─────────────────────────┼────────┼─────────┘
                           ▼        ▼
                   primary S3     backup S3 ...
```

- **PUT** меньше `put_stream_threshold_bytes` (по умолчанию 32 МиБ): тело буферизуется в памяти и пишется во все бэкенды параллельно; успех, если принял хотя бы один, остальным ставится задача с приоритетом HIGH. Тела крупнее (или неизвестного размера) стримятся на primary через multipart с ограниченной памятью, на backup их копирует worker.
- **GET**: HEAD на primary (ограничен `operation_timeout_seconds`), затем тело стримится без таймаута. Если на primary объекта нет — перебираются backup и ставится задача восстановления. Если бэкенд ответил на HEAD, но не смог отдать тело, используется другая копия.
- **DELETE**: уходит во все бэкенды; неудачные ставятся в очередь как задачи удаления. Задача удаления не отбрасывается, а копия, которая ещё ждёт своего удаления, не отдаётся и не реплицируется, поэтому удалённый объект не возвращается.
- **Записи одного ключа сериализуются** (PUT, DELETE, COPY), в режиме `cluster` — между экземплярами, поэтому пересекающиеся записи не оставят на бэкендах разные версии.
- **Worker**: пул обрабатывает очередь с блокировками по объекту; медленная копия не задерживает остальные. Неудавшиеся задачи повторяются с экспоненциальной паузой (1 мин, удваивается до 30 мин); задачи копирования старше 12 ч удаляются, зависшие в `processing` более 5 минут возвращаются в `pending`. При остановке выполняемые задачи возвращаются в `pending`.
- Если задачу репликации не удалось записать (очередь недоступна), запись завершается ответом `503` с `Retry-After`: клиент повторит запрос, а не получит подтверждение записи, которая есть только на одном бэкенде.

## Быстрый старт

### Docker

```bash
cp deploy/config.example.yaml config.yaml   # поправьте endpoints и ключи
docker run -d --name s3syncer -p 8080:8080 \
  -v $PWD/config.yaml:/app/config.yaml:ro \
  -v s3syncer-data:/app/data \
  ghcr.io/spions/s3syncer:latest -config /app/config.yaml
```

или через Compose: [`deploy/docker/docker-compose.example.yml`](../deploy/docker/docker-compose.example.yml).

### Kubernetes (Helm)

```bash
helm install s3syncer oci://ghcr.io/spions/charts/s3syncer --version 0.1.0 -f values.yaml
```

Параметры: [`deploy/charts/s3syncer/values.yaml`](../deploy/charts/s3syncer/values.yaml). По умолчанию (`mode: single`) чарт запускает одну реплику с PVC под SQLite-очередь; с `mode: cluster` — несколько реплик на общем Redis. Конфиг (с ключами) хранится в Secret.

### Из исходников

```bash
make build                       # → bin/s3syncer
./bin/s3syncer -config config.yaml
```

### Локальное окружение (два MinIO-совместимых сервера + s3syncer)

```bash
make up      # minio1, minio2 и s3syncer на localhost:8080
make down
```

Креды dev-окружения: `testtest` / `testtest`.

### Использование

```bash
# AWS CLI (для больших файлов см. «Совместимость клиентов»)
aws s3 ls s3://mybucket/ --endpoint-url http://localhost:8080

# MinIO client
mc alias set myproxy http://localhost:8080 ACCESS_KEY SECRET_KEY
mc ls myproxy/mybucket/

# s3fs
s3fs mybucket /mnt/mybucket -o url=http://localhost:8080 -o use_path_request_style -o passwd_file=~/.passwd-s3fs
```

## Совместимость клиентов

Проверено с AWS CLI v2, `mc` и GeeseFS, в том числе загрузки `aws-chunked` по обычному HTTP.

**Multipart upload не поддерживается.** Клиенты переходят на него для больших файлов: AWS CLI — выше 8 МиБ, `mc` — выше 64 МиБ. Заставьте их использовать один PUT; крупные одиночные PUT s3syncer стримит сам (ограниченная память, без лимита 5 ГиБ):

```bash
aws configure set default.s3.multipart_threshold 5GB        # AWS CLI
mc cp --disable-multipart big.bin myproxy/mybucket/         # mc
```

Остальные неподдерживаемые возможности S3 (ACL, теги, версионирование, копирование между бакетами и конкретной версии, multi-range) отвечают заглушками, ошибкой или игнорируются.

## Конфигурация

YAML-файл, передаётся через `-config <path>` (пример: [`deploy/config.example.yaml`](../deploy/config.example.yaml)). Полный пример файла — в [английском README](../README.md#configuration).

| Параметр | По умолчанию | Описание |
|---|---|---|
| `mode` | `single` | `single`: один экземпляр, очередь в SQLite. `cluster`: несколько экземпляров на общем Redis ([подробнее](#несколько-экземпляров)) |
| `redis.addr` / `username` / `password` / `db` / `tls` / `key_prefix` | — | подключение к Redis, только для `cluster`. Пароль можно задать через `S3SYNCER_REDIS_PASSWORD` |
| `listen_addr` | `:8080` | адрес HTTP-сервера |
| `db_path` | `s3syncer.db` | файл SQLite: очередь в режиме `single`, локальное служебное состояние в `cluster` |
| `sync_interval_seconds` | `60` | интервал цикла worker |
| `operation_timeout_seconds` | `5` | таймаут HEAD/PUT/DELETE/LIST к бэкендам (не применяется к стримовым телам) |
| `log_level` | `info` | `debug`, `info`, `warn` или `error`. Лог каждого запроса — на уровне `debug` |
| `api_token` | пусто | Bearer-токен для `/api/*` (или `S3SYNCER_API_TOKEN`). Если пусто — API доступен только с loopback |
| `put_memory_budget_bytes` | `268435456` | лимит памяти для тел PUT в работе (минимум 48 МиБ); лишние загрузки ждут, а не исчерпывают RAM. Держите заметно меньше лимита памяти контейнера |
| `put_memory_wait_seconds` | `30` | сколько загрузка ждёт память, прежде чем получить `503` + `Retry-After` |
| `request_body_idle_timeout_seconds` | `60` | загрузка, от которой нет данных столько секунд, прерывается (это таймаут простоя: долгие загрузки, которые идут, не страдают) |
| `shutdown_grace_seconds` | `30` | на SIGTERM — сколько запросы и задачи в работе могут завершаться; `terminationGracePeriodSeconds` пода держите выше |
| `put_stream_threshold_bytes` | `33554432` | тела PUT от этого размера (или неизвестного размера) стримятся только на primary; на backup их копирует worker |
| `max_put_size_bytes` | `0` | максимальный размер одного PUT, `0` = без ограничения (выше — 413). В production задайте |
| `virtual_buckets[].virtualBucket` | — | имя бакета для клиентов (уникальное) |
| `virtual_buckets[].primaryBackendName` | — | должно совпадать с одним из `backends[].name` |
| `virtual_buckets[].access_key_id` / `secret_access_key` | — | ключи клиентов; обязательны, `access_key_id` уникален. Ключ действует только для своего бакета |
| `virtual_buckets[].public_read` | `false` | анонимный GET/HEAD объектов |
| `backends[].region` | — | только `[a-z0-9-]+` |
| `backends[].endpoint` | — | только ASCII |
| `backends[].force_path_style` | `false` | path-style адресация (нужна для MinIO) |

Конфиг валидируется при старте; при ошибке процесс завершается с понятным сообщением.

### Несколько экземпляров

По умолчанию (`mode: single`) очередь лежит в локальной SQLite, а блокировки по объектам — в памяти процесса, поэтому экземпляр должен быть ровно один. Чтобы масштабироваться или обеспечить HA, задайте `mode: cluster` и направьте все экземпляры на один Redis:

```yaml
mode: cluster
redis:
  addr: "redis:6379"
```

Очередь, блокировки по объектам и состояние задач `resync`/`bisync` тогда хранятся в Redis: любой экземпляр принимает запросы, обрабатывает задачи и отвечает на `/api/tasks`, а повторный `resync` того же бакета отклоняется с `409`, пока идёт предыдущий. Режим всегда задаётся явно и не угадывается. Структура данных в Redis, рекомендации по сохранности, метрики и примеры алертов — в [`docs/cluster-mode.md`](cluster-mode.md). Helm-чарт поддерживает режим через `mode: cluster` и `replicaCount`.

## CLI

CLI-команды обращаются к REST API запущенного сервера.

```bash
s3syncer -config config.yaml                    # запуск сервера
s3syncer -config config.yaml resync [bucket]    # односторонняя синхронизация primary → backup
s3syncer -config config.yaml bisync [bucket]    # двусторонняя синхронизация
s3syncer -config config.yaml task-list          # список задач
s3syncer -config config.yaml task-status <id>
s3syncer -config config.yaml task-cancel <id>
s3syncer -version
```

## REST API

| Метод | Путь | Описание |
|---|---|---|
| POST | `/api/tasks/resync?bucket=X` | запуск resync (`409`, если для бакета уже идёт) |
| POST | `/api/tasks/bisync?bucket=X` | запуск bisync (то же правило) |
| GET | `/api/tasks/` | список задач |
| GET | `/api/tasks/:id` | статус задачи |
| DELETE | `/api/tasks/:id` | отмена задачи |
| GET | `/metrics` | метрики Prometheus (без аутентификации) |
| GET | `/healthz` | liveness: процесс отвечает по HTTP (без аутентификации) |
| GET | `/readyz` | readiness: очередь синхронизации доступна (без аутентификации) |

## Безопасность

- Клиенты аутентифицируются подписью AWS V2/V4 или presigned URL (окно рассинхронизации часов 15 минут; подписанный `x-amz-content-sha256` сверяется с телом, поэтому перехваченный запрос нельзя повторить с другим содержимым). Отказы пишутся в лог с ограничением частоты. Ключ принимается только для своего виртуального бакета; `ListBuckets` показывает только его. Если сервис доступен из недоверенных сетей, терминируйте TLS перед s3syncer.
- `/api/*` требует `Authorization: Bearer <api_token>`. Без `api_token` доступен только с loopback (и не через reverse-proxy: запросы с `X-Forwarded-For` отклоняются): CLI работает внутри контейнера или на хосте, но не из сети. Helm-чарт не позволит включить ingress без токена.
- `/metrics` отдаётся без аутентификации, как принято у Prometheus-экспортеров. Ограничивайте доступ на уровне сети или reverse-proxy.
- Ответы об ошибках сервера содержат только «internal error», подробности — в логе.
- Ключи бэкендов лежат в конфиге; в Kubernetes чарт хранит его в Secret.
- Об уязвимостях сообщайте по [SECURITY.md](../.github/SECURITY.md).

## Мониторинг

Логи — JSON в stdout (`log_level`). Основные метрики: `s3syncer_sync_tasks_total{status}`, `s3syncer_request_duration_seconds{backend,operation}`, `s3syncer_http_requests_total{method,code}`, `s3syncer_http_request_duration_seconds{method}`, `s3syncer_http_inflight_requests`, `s3syncer_http_errors_total{code}`, `s3syncer_backend_errors_total{backend,operation}`, `s3syncer_upload_memory_bytes`, `s3syncer_overload_rejections_total{reason}`, `s3syncer_managed_tasks_active{type,bucket}`, `s3syncer_managed_task_progress{task_id,type,bucket}`, `s3syncer_managed_tasks_total{type,status}`, а также метрики очереди (`s3syncer_queue_tasks{state}`, `s3syncer_queue_oldest_pending_age_seconds`, `s3syncer_queue_up` и др.) — полный список и примеры алертов в [`docs/cluster-mode.md`](cluster-mode.md).

## Ограничения

- **Multipart upload не поддерживается** (см. «Совместимость клиентов»).
- **У крупных объектов есть окно без избыточности.** Тела от `put_stream_threshold_bytes` лежат только на primary, пока worker не скопирует их на backup. Задайте на бакетах бэкендов правило жизненного цикла `AbortIncompleteMultipartUpload`, чтобы удалять части оборванных загрузок.
- **Тела меньше порога буферизуются в памяти**; суммарно ограничены `put_memory_budget_bytes`. Размер объекта ограничивайте через `max_put_size_bytes`.
- **GET без общего таймаута**: тело стримится клиенту; бэкенду даётся `operation_timeout_seconds`, чтобы начать отвечать, затем пробуется следующая копия.
- **Один экземпляр в режиме `single`.** Очередь лежит в локальной SQLite, блокировки — в памяти процесса; для нескольких реплик используйте `mode: cluster`.
- Symlinks: максимум 10 уровней, циклы определяются.
- Для клиентов, отличных от GeeseFS, к листингу автоматически добавляется `delimiter=/`, чтобы не сканировать огромные бакеты.

## Планы

- Multipart upload.
- Tiered storage (hot/cold) с lifecycle-правилами.

## Лицензия

Apache License 2.0 — см. [LICENSE](../LICENSE).
