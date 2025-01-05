FROM quay.io/astronomer/astro-runtime:12.6.0

#RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    #pip install --no-cache-dir astronomer-cosmos==1.8.1 && deactivate

# install pandas into a virtual environment
#RUN python -m venv pandas_venv && source pandas_venv/bin/activate && \
    #pip install --no-cache-dir apache-airflow-providers-airbyte && deactivate