# План реализации

## 1. Контракт и SQL

1. Зафиксировать формат CSV и XLS/XLSX, кодировку, разделитель, имена колонок, правила пустых значений и путь публикации.
2. Определить состав строки: идентификаторы и атрибуты товара/категории, клиент, согласованная цена и дату действия.
3. Реализовать параметризованный запрос с явными колонками и фильтрами активных записей. Логическая форма:

   ```sql
   SELECT c.id AS client_id, c.name AS client_name,
          p.id AS product_id, p.sku, p.name AS product_name,
          cat.id AS category_id, cat.name AS category_name,
          cp.price, cp.valid_from, cp.valid_to
   FROM client_prices cp
   JOIN clients c ON c.id = cp.client_id
   JOIN products p ON p.id = cp.product_id
   LEFT JOIN categories cat ON cat.id = p.category_id
   WHERE cp.is_active = true;
   ```

4. Проверить индексы на `client_prices(client_id, product_id)` и полях фильтра, план запроса и отсутствие неожиданного размножения строк. Выгрузку читать потоково/порциями, не удерживая весь набор в памяти.

## 2. Exporter

1. Создать stateless CLI/worker-сервис: прочитать параметры запуска, подключиться к PostgreSQL, получить клиентов и сформировать для каждого результат.
2. Записать CSV и XLS/XLSX во временный файл, проверить заголовок, число строк и возможность открыть файл библиотекой формата.
3. Загрузить файлы в object storage с ключом вида `prices/{run-date}/{client-id}/...`; сначала использовать временный key, затем выполнить атомарную публикацию/manifest.
4. Возвращать ненулевой код при ошибке SQL, формата или загрузки. Ввести correlation/run ID и идемпотентный ключ `{client_id, business_date, schema_version}`.

## 3. Контейнер и Kubernetes

1. Собрать минимальный непривилегированный контейнер с pinned-версиями зависимостей и health/exit-логикой.
2. В ConfigMap вынести timezone, bucket, retention hint, batch size и формат; в Secret — DSN/логин PostgreSQL, пароль и credentials object storage. Предпочтительно синхронизировать Secret с cloud secret manager.
3. Создать ServiceAccount с минимальным RBAC (только нужные config/secret и, если требуется, запись метрик); доступ к bucket ограничить отдельной cloud IAM role/workload identity.
4. Создать CronJob с `schedule: "0 6 * * *"`, явно заданным timezone кластера/полем `timeZone` при поддержке версии Kubernetes, `concurrencyPolicy: Forbid`, `backoffLimit: 3`, `startingDeadlineSeconds` (например, 3600), `activeDeadlineSeconds` и `ttlSecondsAfterFinished`.
5. Задать requests/limits CPU и памяти по замерам, `restartPolicy: OnFailure`, securityContext и отдельный временный volume. Не допускать параллельного запуска одной выгрузки.

## 4. Storage и публикация

Настроить bucket/prefix, серверное шифрование, ограниченный IAM, lifecycle retention и при необходимости versioning. Публиковать manifest с run ID, временем, версиями схемы, числом строк и checksum. Не помещать цены/секреты в логи; определить срок хранения и требования к персональным данным.

## 5. Наблюдаемость

- Структурированные логи: run ID, client ID (если допустимо), длительность этапа, строки, размер, status и error code.
- Метрики: `export_runs_total`, `export_failures_total`, `export_duration_seconds`, `export_rows_total`, `export_upload_errors_total`, timestamp последнего успеха.
- Дашборд по успешности, длительности, объёму и ресурсам pod; алерты на failure, deadline, отсутствие успешного запуска после 06:00, а также аномально малое/большое число строк.
- Согласовать с платформенной командой сбор логов и метрик из CronJob pods, так как сам CronJob не является постоянно работающим endpoint.

## 6. Тестирование и rollout

1. Unit-тесты SQL-маппинга, CSV/XLS(X) writer, checksum, retry и идемпотентности; тесты на пустого клиента, дубликат и некорректную категорию.
2. Интеграционно проверить PostgreSQL на тестовых данных, object storage, права Secret/IAM и повторный запуск после частичной ошибки.
3. Нагрузочно проверить 5–10 тыс. строк на клиента/выгрузку, память, время и лимиты соединений; проверить отказ PostgreSQL и storage.
4. В staging запустить вручную и через CronJob, сравнить контрольную выборку с SQL и открыть файлы целевыми потребителями.
5. Выполнить canary для ограниченного набора клиентов или отдельного prefix, затем включить полный rollout. Наблюдать минимум несколько ежедневных циклов; иметь rollback образа и безопасную повторную публикацию.
