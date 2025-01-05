from datetime import datetime,timedelta

from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor

from cosmos import DbtDag, ProjectConfig

from airflow.models.baseoperator import chain

from include.profiles import airflow_db
from include.constants import user_path

with DAG(
    dag_id="example_airbyte_operator",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["example"],
    catchup=False,
) as dag:
    
    sync_source_destination = AirbyteTriggerSyncOperator(
        task_id="airbyte_sync_source_dest_example",
        airbyte_conn_id="airbyte_conn",
        connection_id='80fc48a7-a9bb-48eb-aeb4-ed97565bbb99',
    )


    async_source_destination = AirbyteTriggerSyncOperator(
        task_id="airbyte_async_source_dest_example",
        connection_id='80fc48a7-a9bb-48eb-aeb4-ed97565bbb99',
        asynchronous=True,
    )
    airbyte_sensor = AirbyteJobSensor(
        task_id="airbyte_sensor_source_dest_example",
        airbyte_job_id=async_source_destination.output,
    )



    async_source_destination >> airbyte_sensor

