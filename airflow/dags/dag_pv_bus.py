from datetime import datetime, timedelta
import os

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

alert_email = os.getenv("AIRFLOW_EMAIL")

default_args = {
    "owner": os.getenv("AIRFLOW_USER"),
    "start_date": datetime(2025, 11, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email": [alert_email] if alert_email else [],
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    dag_id="pv_bus_pipeline",
    default_args=default_args,
    description="Run PV Bus pipeline",
    schedule="0 1 12 * *",
    catchup=False,
) as dag:
    api_call = BashOperator(
        task_id="api_call",
        bash_command="cd /opt/airflow/pipeline/src && python -m pv_bus.api_pv_bus",
    )

    wait_after_api = BashOperator(
        task_id="wait_1h_after_api",
        bash_command="sleep 3600",
    )

    extract = BashOperator(
        task_id="extract",
        bash_command="cd /opt/airflow/pipeline/src && python -m pv_bus.extract_pv_bus",
    )

    wait_after_extract = BashOperator(
        task_id="wait_1h_after_extract",
        bash_command="sleep 3600",
    )

    data_import = BashOperator(
        task_id="import",
        bash_command="cd /opt/airflow/pipeline/src && python -m pv_bus.import_pv_bus",
    )

    api_call >> wait_after_api >> extract >> wait_after_extract >> data_import
