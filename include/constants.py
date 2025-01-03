from pathlib import Path
from cosmos import ExecutionConfig

user_path = Path("/usr/local/airflow")
dbt_executable = Path("/usr/local/airflow/.venv/bin/dbt")

venv_execution = ExecutionConfig(
    dbt_executable_path=str(dbt_executable)
)