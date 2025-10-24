from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "analytics",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="customer_analytics_pipeline",
    start_date=datetime(2025, 7, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args=default_args,
    tags=["analytics","glue","s3"],
) as dag:

    upload = BashOperator(
        task_id="upload_raw_to_s3",
        bash_command="python /opt/airflow/dags/src/local_ingest/upload_batch.py --bucket $S3_BUCKET --date {{ ds }}"
    )

    glue = BashOperator(
        task_id="run_glue_batch",
        bash_command="aws glue start-job-run --job-name customer_batch --arguments '{"--S3_BUCKET":"'$S3_BUCKET'","--RAW_PREFIX":"raw","--PROCESSED_PREFIX":"processed"}'"
    )

    crawl = BashOperator(
        task_id="run_glue_crawler",
        bash_command="aws glue start-crawler --name customer-processed-crawler"
    )

    dq = BashOperator(
        task_id="run_dq_checks",
        bash_command="pytest -q src/dq"
    )

    upload >> glue >> crawl >> dq
