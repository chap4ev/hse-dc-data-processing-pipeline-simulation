
import pandas
import logging
import psycopg2

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

try:
    conn = psycopg2.connect(
        host="host",
        database="database",
        user="user",
        password="password"
    )   
except:
    logger.error("Airflow is unable to connect to greenplum database")


default_args = {
    'owner': 'gr8',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False
}

@dag(dag_id='dag_data_processsing', 
     default_args=default_args, 
     start_date=datetime(2024, 6, 1), 
     schedule_interval=timedelta(minutes=1),
     catchup=False)
def process_data_dag():

    @task()
    def data_selection():
        cursor = conn.cursor()
        cursor.execute("select datetime, sensor_id, latitude, longitude, temperature, controller_id from table")
        rows = cursor.fetchall()
        return rows
        

    @task()
    def data_processing(raw_data):
        processed = []
        for row in raw_data:
            if(row[4]) > 0: #positive temerature
                processed.append(row)
        return processed

    @task()
    def insert_data(processed):
        cursor = conn.cursor()
        for row in processed:
            cursor.execute("insert into table2 (datetime, sensor_id, latitude, longitude, temperature, controller_id) values() {}, {}, {}, {}, {}, {} from table".format(row[0], row[1], row[2], row[3], row[4]))
        

    data = data_selection()
    processed_data = data_processing(data)
    insert_data(process_data)

process = process_data_dag()
