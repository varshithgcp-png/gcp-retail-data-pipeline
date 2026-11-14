import os
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

PROJECT_ID=os.environ.get('REVOLUT_PROJECT_ID','YOUR_PROJECT_ID')
BUCKET=os.environ.get('REVOLUT_BUCKET','YOUR_BUCKET')
REPO_ROOT=os.environ.get('REVOLUT_REPO_ROOT','/home/airflow/gcs/data/revolut-banking-analytics')
DBT_DIR=os.path.join(REPO_ROOT,'dbt')
DBT_PROFILES=os.environ.get('REVOLUT_DBT_PROFILES','/home/airflow/gcs/data/dbt_profiles')

SOURCE_OBJECT="{{ dag_run.conf.get('source_object', 'revolut/source/day0/EvalDB_Day0_Interleaved.txt') }}"
BUSINESS_DATE="{{ dag_run.conf.get('business_date', ds) }}"
LOAD_TYPE="{{ dag_run.conf.get('load_type', 'FULL') }}"
SOURCE_URI=f"gs://{BUCKET}/{SOURCE_OBJECT}"

with DAG(
    dag_id='revolut_banking_bronze_silver_gold',
    start_date=pendulum.datetime(2026,11,1,tz='Asia/Kolkata'),
    schedule=None,
    catchup=False,
    default_args={'retries':1,'retry_delay':timedelta(minutes=3)},
    tags=['banking','bigquery','dbt','python'],
) as dag:
    wait_for_file=GCSObjectExistenceSensor(
        task_id='wait_for_interleaved_file',bucket=BUCKET,object=SOURCE_OBJECT,
        poke_interval=30,timeout=1800,mode='reschedule'
    )

    seed_metadata=BashOperator(
        task_id='seed_metadata',
        bash_command=(f"cd {REPO_ROOT} && python -m banking_pipeline.seed_metadata "
                      f"--project-id {PROJECT_ID}")
    )

    bronze_load=BashOperator(
        task_id='python_split_and_load_bronze',
        bash_command=(f"cd {REPO_ROOT} && python -m banking_pipeline.bronze_loader "
                      f"--project-id {PROJECT_ID} --source '{SOURCE_URI}' "
                      f"--business-date '{BUSINESS_DATE}' --load-type '{LOAD_TYPE}'")
    )

    dbt_silver=BashOperator(
        task_id='dbt_build_silver',
        bash_command=f"cd {DBT_DIR} && dbt build --select tag:silver --profiles-dir {DBT_PROFILES}",
        env={'DBT_BIGQUERY_PROJECT':PROJECT_ID},append_env=True
    )
    dbt_snapshot=BashOperator(
        task_id='dbt_snapshot_customer_account',
        bash_command=f"cd {DBT_DIR} && dbt snapshot --profiles-dir {DBT_PROFILES}",
        env={'DBT_BIGQUERY_PROJECT':PROJECT_ID},append_env=True
    )
    dbt_gold=BashOperator(
        task_id='dbt_build_gold',
        bash_command=f"cd {DBT_DIR} && dbt build --select tag:gold --profiles-dir {DBT_PROFILES}",
        env={'DBT_BIGQUERY_PROJECT':PROJECT_ID},append_env=True
    )
    mark_success=BashOperator(
        task_id='mark_pipeline_success',
        bash_command=(f"cd {REPO_ROOT} && python -m banking_pipeline.mark_pipeline "
                      f"--project-id {PROJECT_ID} --source '{SOURCE_URI}' "
                      f"--business-date '{BUSINESS_DATE}' --status SUCCESS")
    )

    wait_for_file >> seed_metadata >> bronze_load >> dbt_silver >> dbt_snapshot >> dbt_gold >> mark_success
