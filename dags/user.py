from airflow.decorators import dag, task
from airflow.operators.bash_operator import BashOperator
from airflow.models.baseoperator import chain

from datetime import datetime
        
DBT_PROJECT_DIR = "/usr/local/airflow/"

@dag(
    start_date=datetime(2024, 10, 10),
    schedule=None,
    catchup=False,
    tags=['user'],
)
def user():
    
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"dbt seed --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}"
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"dbt deps --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}"
    )

    dbt_raw = BashOperator(
        task_id="dbt_raw",
        bash_command=f"dbt run -s models/raw --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}"
    )

    dbt_snap = BashOperator(
        task_id="dbt_snap",
        bash_command=f"dbt snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}"
    )

    dbt_stg = BashOperator(
        task_id="dbt_stg",
        bash_command=f"dbt run -s models/staging --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}"
    )

    dbt_int = BashOperator(
        task_id="dbt_int",
        bash_command=f"dbt run -s models/intermediate --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}"
    )

    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=f"dbt run -s models/marts --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}"
    )

    chain(
        dbt_seed,
        dbt_deps,
        dbt_raw,
        dbt_snap,
        dbt_stg,
        dbt_int,
        dbt_marts
    )

user()