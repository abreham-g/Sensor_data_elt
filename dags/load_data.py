from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator


default_args = {"owner":"abreham",
                'depends_on_past': False,
                'email': ['aynuyeabresh@gmail.com'],
                'email_on_failure': True,
                'email_on_retry': True,
                'retries': 1,
                "start_date":datetime(2021,10,4)
                }
with DAG('create_tables',
                dag_id="workflow",
                default_args=default_args,
                description='An Airflow DAG to create tables',
                schedule_interval='@once',
                ) as dag:
    
    check_file = BashOperator(
        task_id="check_file",
        bash_command="shasum ~/ip_files/or.csv",
        retries = 2,
        retry_delay=timedelta(seconds=15))

    create_stations_table = MySqlOperator(
        task_id='create_table_I80_stations',
        mysql_conn_id='mysql_conn_id',
        sql='/mysql/I80stations_schema.sql',
        dag=dag,
    )
    create_richards_table = MySqlOperator(
        task_id='create_table_richards',
        mysql_conn_id='mysql_conn_id',
        sql='/mysql/richards_schema.sql',
        dag=dag,
    )
    create_station_summary_table = MySqlOperator(
        task_id='create_table_station_summary',
        mysql_conn_id='mysql_conn_id',
        sql='/mysql/station_schema.sql',
        dag=dag,
    )

    email = EmailOperator(task_id='send_email',
        to='aynuyeabresh@gmail.com',
        subject='Daily report generated',
        html_content=""" <h1>Congratulations! Tables created successfully!.</h1> """,
        )
        
    check_file >> create_stations_table >> insert_stations  >> email
    check_file >>  create_richards_table  >> insert_richard >> email
    check_file >> create_station_summary_table >>  insert_station_summary >> email
