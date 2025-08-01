from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import logging
import sys
sys.path.append('/opt/airflow/project')


default_args = {
    'owner': 'karthik',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2025, 7, 31),
    'email_on_failure': False
}


with DAG(
    dag_id = 'logistics_dbt_dag',
    description = 'a simple dag to trigger logistics elt pipeline',
    schedule = '@daily',
    catchup = False,
    default_args = default_args
) as my_dag:
    
    start = DummyOperator(task_id = 'start')

    load_raw = SnowflakeOperator(
        task_id = 'load_raw_data',
        snowflake_conn_id = 'snowflake_conn',
        sql = '''
        call logistics_dwh.raw.load_raw_data();
        '''
    )

    run_dbt = BashOperator(
        task_id = 'run_dbt_cmd',
        bash_command = 'cd /opt/airflow/project/dbt_logistics && /home/airflow/.local/bin/dbt run --profiles-dir .dbt'
    )

    test_dbt = BashOperator(
        task_id = 'test_dbt_cmd',
        bash_command = 'cd /opt/airflow/project/dbt_logistics && /home/airflow/.local/bin/dbt test --profiles-dir .dbt'
    )


    send_email = EmailOperator(
        task_id='send_alert',
        to='karthikesan.in@gmail.com',
        subject='DAG run status',
        html_content="DAG ran successfully",
        conn_id='email_conn',
        trigger_rule='all_done'
    )

    end = DummyOperator(task_id = 'end')


    start >> load_raw >> run_dbt >> test_dbt >> send_email >> end