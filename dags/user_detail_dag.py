from datetime import datetime
from cosmos import DbtDag, ProjectConfig
from include.profiles import airflow_db
from include.constants import user_path

simple_dag = DbtDag(
    project_config=ProjectConfig(user_path),
    profile_config=airflow_db,
    schedule_interval=None,
    start_date=datetime(2024,10,10),
    catchup=False,
    dag_id="user_detail",
    tags=["detail"],

    )