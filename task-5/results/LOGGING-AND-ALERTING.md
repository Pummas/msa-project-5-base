# Логирование и оповещение TradeWare

## Structured logging

API и Spring Batch worker пишут события в JSON в stdout или ротируемый локальный файл. Structured logging позволяет Filebeat надёжно разобрать поля, искать их в Kibana и связывать события одной операции без парсинга свободного текста. Для каждой операции передаётся correlation ID (например, `operationId` из API); при наличии distributed tracing он также прокидывается как trace/correlation context.

Минимальная схема события:

| Поле | Смысл |
|---|---|
| `timestamp` | Время события в UTC, ISO-8601 |
| `level` | `INFO`, `WARN`, `ERROR` и т. п. |
| `service` | `tradeware-api` или `batch-worker` |
| `operationId` | Идентификатор запуска/запроса для корреляции |
| `jobName` | Имя Spring Batch job, если применимо |
| `stepName` | Имя step, если применимо |
| `status` | `started`, `success`, `failed`, `skipped` и т. п. |
| `records` | Число обработанных/пропущенных записей |
| `duration` | Длительность в миллисекундах |
| `errorCode` | Стабильный код ошибки, без stack trace в поле |

Дополнительные поля: `environment`, `host`, `batchId`, `fileName` (только безопасный идентификатор), `exceptionClass` и `message`. Стек ошибки сохраняется для `ERROR`, но сообщения и контекст проходят redaction: нельзя логировать PII, токены, пароли, содержимое CSV и секреты GCS/БД. Имена файлов и идентификаторы нужно маскировать, если они содержат персональные данные.

## Pipeline

`Filebeat → Logstash → Elasticsearch → Kibana`.

Filebeat собирает JSON, добавляет metadata хоста/контейнера и доставляет события в Logstash. Logstash валидирует схему, нормализует timestamp/level, удаляет или маскирует запрещённые поля и маршрутизирует записи. Elasticsearch хранит индексируемые логи, Kibana предоставляет поиск, dashboard по job/step и расследование ошибок. Pipeline не должен быть единственным источником состояния batch: история запусков и checkpoint остаются в `JobRepository`.

Для повторной доставки следует настроить persistent queue/буфер Logstash и backpressure. Retention согласуется с эксплуатацией и политикой данных: например, горячие searchable-логи 30 дней и архив/удаление после этого; ERROR и audit-события могут иметь отдельный срок. Доступ к Kibana и Elasticsearch ограничивается ролями, а transport/storage шифруются.

## Prometheus Alertmanager

Метрики собираются Prometheus с `/actuator/prometheus`, а Alertmanager доставляет email и Slack. Grafana остаётся инструментом dashboards и анализа. Alertmanager выбран не только Grafana Alerting, потому что он естественно работает с Prometheus rules, поддерживает grouping/deduplication/inhibition, единый routing для API и worker и не связывает критичные уведомления с доступностью UI Grafana. Grafana Alerting можно добавить для визуальных/командных правил, но не использовать как единственный канал.

Рекомендуемые правила:

| Alert | Условие | Действие |
|---|---|---|
| `ApplicationDown` | `up == 0` для API/worker 2 минуты | Critical: Slack и email дежурному |
| `BatchFailed` | `increase(tradeware_batch_jobs_total{status="failure"}[5m]) > 0` | Critical: operationId/jobName в annotation |
| `ReportSlaBreach` | p95 report latency > 30 секунд 5 минут | Warning, эскалация при длительном нарушении |
| `HighHeap` | heap usage > 85% 10 минут; critical > 95% | Warning/Critical, проверить GC и лимит контейнера |
| `HighGC` | GC pause ratio/паузы выше согласованного порога 10 минут | Warning, при деградации — critical |
| `DatabaseUnavailable` | connection timeout или `up` DB exporter = 0 2 минуты | Critical |
| `DataQualitySkipRatio` | `(skipped + rejected) / read` выше порога, например 5%, для job | Warning; порог подтверждается бизнесом |
| `NoSuccessfulRun` | `time() - batch_last_success_timestamp_seconds` больше ожидаемого окна | Critical после пропуска планового запуска |

В annotation указываются severity, service, summary, runbook URL и labels `job_name`/`step_name`; высококардинальные `operationId` не используются как Prometheus labels. Повторные ошибки группируются по service/job, а восстановление (`resolved`) также отправляется в Slack/email.

## Нерешённые вопросы

Нужно утвердить retention и классификацию данных, точные пороги skip ratio/GC/DB latency, расписание успешного запуска и владельцев email/Slack-каналов. Также требуется решить, где именно выполняется redaction до Elasticsearch и как доставляются логи при недоступности Logstash.
