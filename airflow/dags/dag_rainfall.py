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
    dag_id="rainfall_pipeline",
    default_args=default_args,
    description="Run Rainfall pipeline",
    schedule="0 */2 * * *",
    catchup=False,
) as dag:
    api_call = BashOperator(
        task_id="api_call",
        bash_command="cd /opt/airflow/pipeline/src && python -m rainfall.api_rainfall",
    )

    wait_after_api = BashOperator(
        task_id="wait_5m_after_api",
        bash_command="sleep 300",
    )

    data_import = BashOperator(
        task_id="import",
        bash_command="cd /opt/airflow/pipeline/src && python -m rainfall.import_rainfall",
    )

    api_call >> wait_after_api >> data_import
