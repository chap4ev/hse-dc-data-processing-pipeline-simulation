import psycopg2
import logging

from configs import greenplum_config


class GreenplumDriver:

    def __init__(self):

        self.logger = logging.getLogger(__name__)
        self._data_columns = columns = [
            "event_datetime", "sensor_id", "latitude", "longitude", "temperature", "controller_id"
        ]

        try:
            self._conn = psycopg2.connect(
                host=greenplum_config["host"],
                database=greenplum_config["dbname"],
                user=greenplum_config["user"],
                password=greenplum_config["password"]
            )
        except:
            self.logger.exception("Unable to connect to greenplum database")

    def save_row(self, data):
        cursor = self._conn.cursor()
        values = [data[column] for column in self._data_columns]
        query = (
            f"""
            insert into {greenplum_config["table"]} 
            ({", ".join(self._data_columns)}) 
            values ('{data["event_datetime"]}', '{data["sensor_id"]}', {data["latitude"]}, {data["longitude"]}, {data["temperature"]}, '{data["controller_id"]}')
            """
        )
        cursor.execute(query)
        self._conn.commit()
        cursor.close()

    def close(self):
        self._conn.close()