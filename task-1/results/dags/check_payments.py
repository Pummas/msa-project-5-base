"""Small CSV ETL demo showing branching, retries and local email notifications."""

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.email import send_email
from airflow.utils.trigger_rule import TriggerRule

from payment_logic import (
    evaluate_payment_delays,
    run_etl_pipeline,
    input_files_branch,
)

EMAIL_TO = os.getenv("DEMO_EMAIL_TO", "demo@example.local")


def process_data(**context):
    result = run_etl_pipeline()
    context["ti"].xcom_push(key="process_result", value=result)
    logging.info("ETL result: %s", result)
    return result


def branch_on_delays(**context):
    result = context["ti"].xcom_pull(
        task_ids="process_data", key="process_result"
    ) or {}
    return evaluate_payment_delays(result)


def retry_demo(**context):
    """Fail once by default so the Airflow retry counter is visible in the UI."""
    if os.getenv("DEMO_RETRY", "true").lower() == "true" and context["ti"].try_number == 1:
        raise RuntimeError("Intentional first-attempt failure for retry demonstration")
    logging.info("Retry demonstration completed on attempt %s", context["ti"].try_number)


def failure_notification(context):
    dag_run = context.get("dag_run")
    subject = f"[Airflow] DAG failed: {dag_run.dag_id if dag_run else 'unknown'}"
    send_email(
        to=EMAIL_TO,
        subject=subject,
        html_content=(
            "<p>The demo pipeline failed after retries.</p>"
            f"<p>Task: {context.get('task_instance')}</p>"
        ),
    )


default_args = {
    "owner": "student",
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
}

with DAG(
    dag_id="check_payments",
    description="CSV payment ETL with branching, retry and MailHog notifications",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    on_failure_callback=failure_notification,
    tags=["poc", "payments", "csv"],
) as dag:
    check_input = BranchPythonOperator(
        task_id="check_input_files",
        python_callable=input_files_branch,
    )
    process = PythonOperator(task_id="process_data", python_callable=process_data)
    retry = PythonOperator(task_id="retry_demo", python_callable=retry_demo)
    no_data = PythonOperator(
        task_id="no_data_alert",
        python_callable=lambda: logging.warning("Input CSV files are missing"),
    )
    choose_delay_branch = BranchPythonOperator(
        task_id="check_delays", python_callable=branch_on_delays
    )
    delayed = PythonOperator(
        task_id="payments_delayed_alert",
        python_callable=lambda: logging.warning("Long payment delays detected"),
    )
    no_delays = PythonOperator(
        task_id="no_delays",
        python_callable=lambda: logging.info("No long payment delays detected"),
    )
    join = EmptyOperator(
        task_id="join_success_paths",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    success_email = EmailOperator(
        task_id="pipeline_success_email",
        to=EMAIL_TO,
        subject="[Airflow] Payment pipeline succeeded",
        html_content=(
            "<h3>Payment CSV pipeline completed</h3>"
            "<p>See the Airflow task log and data/out for generated reports.</p>"
        ),
    )

    check_input >> [process, no_data]
    process >> retry >> choose_delay_branch
    choose_delay_branch >> [delayed, no_delays]
    [delayed, no_delays, no_data] >> join >> success_email
