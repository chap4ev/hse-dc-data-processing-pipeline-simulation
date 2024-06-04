import psycopg2
import logging

from configs import greenplum_config


class GreenplumDriver:

    def __init__(self):

        self.logger = logging.getLogger(__name__)
        self._data_columns = columns = [
            "datetime", "sensor_id", "latitude", "longitude", "temperature", "controller_id"
        ]

        try:
            self._conn = psycopg2.connect(
                host=greenplum_config["host"],
                database=greenplum_config["database"],
                user=greenplum_config["user"],
                password=greenplum_config["password"]
            )
        except:
            self.logger.error("Unable to connect to greenplum database")

    def save_row(self, data):
        cursor = self._conn.cursor()
        values = [data[column] for column in self._data_columns]
        query = (
            f"""
            insert into {greenplum_config["table"]()} 
            ({", ".join(self._data_columns)}) 
            values ({", ".join(values)})
            """
        )
        cursor.execute(query)
        self._conn.commit()
        cursor.close()

    def close(self):
        self._conn.close()