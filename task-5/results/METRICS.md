# Метрики и мониторинг TradeWare

## Цели и схема сбора

Наблюдаемость должна отвечать на четыре вопроса: доступен ли онлайн-контур, успевает ли отчёт в SLA 30 секунд для 2000 строк, выполняется ли ETL без потерь и достаточно ли ресурсов для роста до 400 000 строк в сутки и пиков x2–3. Метрики API и Spring Batch worker собираются через Micrometer и Spring Boot Actuator, а публикуются в формате Prometheus на endpoint `/actuator/prometheus`. Prometheus опрашивает endpoint, Grafana показывает dashboards и тренды, а Prometheus Alertmanager маршрутизирует оповещения.

Для всех прикладных метрик используются низкокардинальные labels: `service`, `environment`, `job_name`, `step_name`, `status`. Идентификатор операции не следует включать в labels: он остаётся в логах и трассировочном контексте.

## Метрики

| Область | Имя / пример | Источник | Назначение | Возможный alert |
|---|---|---|---|---|
| Availability | `up{job="tradeware-api"}`, `up{job="batch-worker"}` | Prometheus scrape Actuator | Доступность API и worker | `up == 0` дольше 2 минут |
| Availability | `http_server_requests_seconds_count` | Micrometer Actuator/Prometheus | Факт обработки запросов | Нет scrape или резкое падение трафика в рабочее окно |
| Availability | `batch_last_success_timestamp_seconds` | Spring Batch listener + Micrometer | Свежесть успешного запуска | Нет успешного запуска за согласованный интервал |
| JVM/runtime | `jvm_memory_used_bytes`, `jvm_memory_max_bytes` (heap) | Micrometer Actuator/Prometheus | Контроль heap и риска OOM | Heap > 85% 10 минут; критично > 95% |
| JVM/runtime | `jvm_gc_pause_seconds_count`, `jvm_gc_pause_seconds_sum` | Micrometer Actuator/Prometheus | Выявление частых/долгих GC | GC pause ratio или суммарные паузы выше порога |
| JVM/runtime | `process_cpu_usage`, `system_cpu_usage` | Micrometer Actuator/Prometheus | Нагрузка CPU и headroom | CPU > 85% 15 минут |
| JVM/runtime | `jvm_threads_live_threads`, `jvm_threads_states_threads` | Micrometer Actuator/Prometheus | Утечки потоков и насыщение пулов | Live threads близки к лимиту или растут без возврата |
| JVM/runtime | `process_uptime_seconds`, `process_start_time_seconds` | Micrometer Actuator/Prometheus | Рестарты и стабильность процесса | Частые рестарты; uptime меньше минимального порога |
| HTTP/API | `http_server_requests_seconds` (p50/p95/p99) | Micrometer Actuator/Prometheus | Контроль latency API и SLA отчёта | p95 отчёта > 30 секунд или p99 выше договорённого порога |
| HTTP/API | `http_server_requests_seconds_count` по `status` | Micrometer Actuator/Prometheus | Error rate и распределение ответов | 5xx > 1% за 5 минут; 4xx отслеживаются отдельно |
| HTTP/API | `tradeware_operation_requests_total` по operation/status | Micrometer в API | Запуски, статусы и отказ регистрации операции | Рост `failed`/`rejected`; аномально большой backlog |
| PostgreSQL | `hikaricp_connections_active`, `hikaricp_connections_pending`, `hikaricp_connections_max` | Micrometer Actuator/Prometheus | Насыщение DB connection pool | Pending > 0 5 минут или active/max > 90% |
| PostgreSQL | `jdbc_query_seconds` / histogram latency по безопасным группам операций | Micrometer instrumentation | Задержки чтения/записи и влияние на SLA | p95 запросов выше порога; рост timeout |
| PostgreSQL | `hikaricp_connections_timeout_total`, `hikaricp_connections_creation_seconds` | Micrometer Actuator/Prometheus | Недоступность или деградация БД | Таймауты соединений > 0; DB unavailable |
| PostgreSQL | `db_client_connections_usage`, `db_transaction_seconds` | Micrometer/Prometheus | Транзакции и использование ресурсов | Долгие транзакции или рост rollback |
| Spring Batch | `tradeware_batch_job_duration_seconds` | Spring Batch `JobExecutionListener` + Micrometer | Длительность полного job | Job duration выше SLA/операционного порога |
| Spring Batch | `tradeware_batch_jobs_total{status="success\|failure\|stopped"}` | `JobExecutionListener` | Успешность и отказоустойчивость запусков | Любой `failure`; рост stopped |
| Spring Batch | `tradeware_batch_step_duration_seconds` | `StepExecutionListener` + Micrometer | Поиск медленного step | p95 step duration выше baseline |
| Spring Batch | `tradeware_batch_read_total`, `..._write_total`, `..._skip_total` | `StepExecution` counters/listener | Контроль прочитанных, записанных и пропущенных строк | write < read при неожиданном расхождении; skip ratio выше порога |
| Spring Batch | `tradeware_batch_processing_rows_per_second` | Listener + duration/counts | Throughput и прогноз времени обработки | Ниже минимального throughput; не успевает к окну загрузки |
| Spring Batch | `tradeware_batch_retry_total`, `..._rollback_total` | Spring Batch listener | Повторные попытки и нестабильность | Рост retry/rollback; исчерпание retry limit |
| Spring Batch | `tradeware_batch_queue_depth` / `tradeware_batch_backlog_age_seconds` | API/JobRepository, если есть очередь запусков | Очередь ожидающих операций | Backlog выше лимита или возраст старейшей операции > SLA |
| Бизнес | `tradeware_import_rows_total{result="accepted\|rejected\|skipped"}` | Batch listener/business counters | Качество и результат загрузки | Доля rejected/skipped выше согласованного порога |
| Бизнес | `tradeware_import_files_total{status}` | GCS reader + job listener | Наличие и обработка входных файлов | Ожидаемый файл не найден или обработка не завершена |
| Бизнес | `tradeware_report_rows_total`, `tradeware_report_duration_seconds` | API/report operation | SLA отчёта 2000 строк | p95 duration > 30 секунд; rows сильно отличаются от ожидаемых |
| Бизнес | `tradeware_import_last_success_timestamp_seconds` | Job listener | Свежесть данных номенклатуры | No successful run в заданном окне |

### Интерпретация batch-счётчиков

`read`, `write` и `skip` берутся из `StepExecution` и фиксируются по завершении step; для длительного job допустимо также регистрировать периодические snapshots. Throughput рассчитывается как обработанные строки / elapsed time, с отдельным учётом пауз ожидания БД и GCS. Если очередь запусков фактически не используется, `queue_depth` и `backlog_age` не создаются: вместо них контролируется число одновременно выполняющихся job и возраст последнего успешного запуска.

## Dashboards, recording rules и alerts

В Grafana нужны отдельные dashboards: availability/API, JVM/runtime, PostgreSQL и batch/business. Для тяжёлых запросов создаются Prometheus recording rules, например `job:http_request_duration_seconds:p95`, `job:batch_skip_ratio:rate5m`, `job:batch_rows_per_second:rate15m` и `job:api_5xx_ratio:rate5m`. Alert rules используют эти агрегаты, чтобы не дублировать сложную PromQL-логику в каждом уведомлении; предупреждения и критические состояния имеют разные thresholds и `for`.

Минимальный набор правил: приложение down (2 минуты), batch failure (сразу после failure), SLA breach отчёта (p95 > 30 секунд 5 минут), heap > 85% (10 минут), чрезмерный GC, DB unavailable/connection timeout, skip/rejected ratio выше порога и no successful run в ожидаемом окне. Prometheus Alertmanager отвечает за deduplication, grouping, inhibition и доставку в email/Slack; Grafana используется прежде всего для анализа и визуализации.

## Ограничения

Prometheus хранит временные ряды, но метрики не являются журналом состояния: при рестарте приложения или потере scrape часть краткоживущих значений и in-memory counters может быть потеряна. Для истории job и checkpoint источником истины остаётся `JobRepository` в PostgreSQL, а для расследования отдельных операций — JSON-логи. Нужно отдельно согласовать retention Prometheus, частоту scrape, пороги и окно «ожидаемого запуска».
