import logging
import psycopg2

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
# from airflow.providers.sqlite.operators.sqlite import SqliteOperator


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)


# try:
# conn = psycopg2.connect(
#     host="greenplum",
#     database="gpdb",
#     user="greenplum",
#     password="greenplum"
# )
# except:
#     logger.exception("Airflow is unable to connect to greenplum database")


default_args = {
    'owner': 'gr8',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False
}

@dag(dag_id='dag_data_processsing', 
     default_args=default_args, 
     start_date=datetime(2024, 6, 1), 
     schedule_interval=timedelta(minutes=5),
     catchup=False)
def process_data_dag():

    transfer_data_query = """
    INSERT INTO sensor_data_processed (sensor_id, controller_id, latitude, longitude, temperature, event_datetime)
    SELECT sensor_id, controller_id, latitude, longitude, temperature, event_datetime
    FROM sensor_data
    WHERE event_datetime > COALESCE((SELECT MAX(event_datetime) FROM sensor_data_processed), '1970-01-01 00:00:00')
    AND temperature > 0;
    """

    transfer_sensor_data = PostgresOperator(
        task_id='transfer_sensor_data',
        sql=transfer_data_query,
        postgres_conn_id='postgres_default',
    )

    transfer_sensor_data

    # @task()
    # def data_selection():
    #     cursor = conn.cursor()
    #     cursor.execute("select event_datetime, sensor_id, latitude, longitude, temperature, controller_id from sensor_data")
    #     rows = cursor.fetchall()
    #     return rows

    # @task()
    # def data_processing(raw_data):
    #     processed = []
    #     for row in raw_data:
    #         if(row[4]) > 0: #positive temerature
    #             processed.append(row)
    #     return processed

    # @task()
    # def insert_data(processed):
    #     cursor = conn.cursor()
    #     for row in processed:
    #         cursor.execute("insert into processed_sensor_data (event_datetime, sensor_id, latitude, longitude, temperature, controller_id) values( {}, {}, {}, {}, {}, {} ) from table".format(row[0], row[1], row[2], row[3], row[4]))

    # data = data_selection()
    # processed_data = data_processing(data)
    # insert_data(processed_data)

process = process_data_dag()
