from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='etl_load_sessions_hook',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='ETL using SnowflakeHook to load session data',
    tags=['etl', 'snowflake'],
) as dag:

    @task
    def load_user_session_channel():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn_wau')
        sql = """
            COPY INTO dev.raw.user_session_channel
            FROM @dev.raw.blob_stage/user_session_channel.csv;
        """
        hook.run(sql)

    @task
    def load_session_timestamp():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn_wau')
        sql = """
            COPY INTO dev.raw.session_timestamp
            FROM @dev.raw.blob_stage/session_timestamp.csv;
        """
        hook.run(sql)

    # Define task execution order
    load_user_session_channel() >> load_session_timestamp()
