from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='elt_build_summary',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Join raw tables and create session_summary table in Snowflake',
    tags=['elt', 'snowflake'],
) as dag:

    @task
    def create_session_summary():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn_wau')
        sql = """
        CREATE SCHEMA IF NOT EXISTS dev.analytics;

        CREATE OR REPLACE TABLE dev.analytics.session_summary AS
        SELECT 
            usc.userId,
            usc.sessionId,
            usc.channel,
            st.ts
        FROM dev.raw.user_session_channel usc
        JOIN dev.raw.session_timestamp st
        ON usc.sessionId = st.sessionId
        WHERE usc.sessionId IN (
            SELECT sessionId
            FROM dev.raw.user_session_channel
            GROUP BY sessionId
            HAVING COUNT(*) = 1
        );
        """
        hook.run(sql)

    create_session_summary()
