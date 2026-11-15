import csv
import os
from datetime import datetime

# Пути к файлам (внутри Docker-контейнера Airflow)
ORDERS_FILE = "/opt/airflow/data/in/orders.csv"
PAYMENTS_FILE = "/opt/airflow/data/in/payments.csv"
OUTPUT_DIR = "/opt/airflow/data/out/"

DATE_FORMAT = "%d.%m.%Y"
FILE_DATETIME_FORMAT = "%Y%m%d_%H%M%S"
DELAY_THRESHOLD_DAYS = 14


def input_files_branch():
    """Проверяет, на месте ли исходные файлы.
    Возвращает ID следующей задачи для Airflow (ветвление).
    """
    if not os.path.exists(ORDERS_FILE) or not os.path.exists(PAYMENTS_FILE):
        print(f"⚠️ Ошибка: Один из файлов не найден ({ORDERS_FILE} или {PAYMENTS_FILE})")
        return "no_data_alert"

    print("✅ Файлы найдены, запускаем обработку.")
    return "process_data"


def read_csv_file(file_path):
    """Вспомогательная функция: читает CSV и возвращает список словарей (строк)."""
    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames or []
        rows = list(reader)
    return rows, fields


def save_csv_file(file_path, fields, rows):
    """Вспомогательная функция: сохраняет данные в новый CSV файл."""
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def merge_orders_and_payments(orders, payments):
    """Соединяет заказы и платежи по OrderId (Inner Join).
    Возвращает список оплаченных заказов и множество ID оплаченных заказов.
    """
    # Группируем платежи по OrderId для быстрого поиска
    payments_by_order = {}
    for p in payments:
        payments_by_order.setdefault(p["OrderId"], []).append(p)

    paid_orders = []
    paid_order_ids = set()

    for order in orders:
        oid = order["OrderId"]
        if oid in payments_by_order:
            paid_order_ids.add(oid)
            # Если у заказа может быть несколько платежей, создаем строку для каждого
            for p in payments_by_order[oid]:
                merged_row = dict(order)
                merged_row["PaidAt"] = p["PaidAt"]
                paid_orders.append(merged_row)

    return paid_orders, paid_order_ids


def find_unpaid_orders(orders, paid_order_ids, today):
    """Находит заказы без оплаты и считает, сколько дней они ждут оплаты."""
    unpaid_orders = []
    has_long_delay = False

    for order in orders:
        oid = order["OrderId"]
        if oid not in paid_order_ids:
            # Считаем разницу между сегодня и датой создания заказа
            created_date = datetime.strptime(order["CreatedAt"], DATE_FORMAT).date()
            delay_days = (today - created_date).days

            row = dict(order)
            row["PaymentDelayDays"] = str(delay_days)
            unpaid_orders.append(row)

            # Если задержка больше порога (14 дней), поднимаем флаг
            if delay_days > DELAY_THRESHOLD_DAYS:
                has_long_delay = True

    return unpaid_orders, has_long_delay


def find_orphan_payments(orders, payments):
    """Находит платежи, для которых нет заказов (платежи-сироты)."""
    existing_order_ids = {o["OrderId"] for o in orders}
    return [dict(p) for p in payments if p["OrderId"] not in existing_order_ids]


def evaluate_payment_delays(etl_result):
    """Логика ветвления: если есть долгие задержки, уходим на алерт."""
    if etl_result.get("has_long_delay"):
        return "payments_delayed_alert"
    return "no_delays"


def run_etl_pipeline(today=None):
    """Главная функция: выполняет ETL процесс и возвращает статистику."""
    if today is None:
        today = datetime.now().date()

    run_timestamp = datetime.now().strftime(FILE_DATETIME_FORMAT)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. EXTRACT (Чтение)
    orders, order_fields = read_csv_file(ORDERS_FILE)
    payments, payment_fields = read_csv_file(PAYMENTS_FILE)

    # 2. TRANSFORM (Преобразование)
    # А) Оплаченные заказы
    paid_data, paid_ids = merge_orders_and_payments(orders, payments)
    # Б) Неоплаченные заказы
    unpaid_data, has_long_delay = find_unpaid_orders(orders, paid_ids, today)
    # В) Платежи без заказов
    orphan_data = find_orphan_payments(orders, payments)

    # 3. LOAD (Сохранение результатов)
    paid_file = os.path.join(OUTPUT_DIR, f"paid_orders_{run_timestamp}.csv")
    save_csv_file(paid_file, list(order_fields) + ["PaidAt"], paid_data)

    unpaid_file = os.path.join(OUTPUT_DIR, f"unpaid_orders_{run_timestamp}.csv")
    save_csv_file(unpaid_file, list(order_fields) + ["PaymentDelayDays"], unpaid_data)

    orphan_file = os.path.join(OUTPUT_DIR, f"orphan_payments_{run_timestamp}.csv")
    save_csv_file(orphan_file, list(payment_fields), orphan_data)

    # Формируем отчет
    result = {
        "paid_count": len(paid_data),
        "unpaid_count": len(unpaid_data),
        "orphan_count": len(orphan_data),
        "has_long_delay": has_long_delay,
    }

    print(f"✅ ETL завершен: Оплачено={result['paid_count']}, Ожидают={result['unpaid_count']}, "
          f"Ошибочных={result['orphan_count']}, Критичные задержки={result['has_long_delay']}")

    return result
