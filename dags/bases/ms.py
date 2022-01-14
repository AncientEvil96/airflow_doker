import pyodbc
import pandas as pd
import datetime
from save_to_file import SaveFile


class MsSQL:
    def __init__(self, **kwargs):
        """

        :param kwargs:

        host:
        password:
        login:
        database:

        """
        conn = kwargs.pop('params')
        self.host = conn.pop('host')
        self.__password = conn.pop('password')
        self.__login = conn.pop('login')
        self.database = conn.pop('database')

        self.conn_ms = "DRIVER={ODBC Driver 17 for SQL Server};" \
                       f"SERVER={self.host}" \
                       f";DATABASE={self.database}" \
                       f";UID={self.__login}" \
                       f";PWD={self.__password}"

    def select_db_df(self, query) -> str:
        """
        получение данных в формате DataFrame
        :return: file path
        """

        with pyodbc.connect(self.conn_ms) as cnxn:
            sf = SaveFile()
            return sf.create_file_parquet(
                df=pd.read_sql(query, cnxn),
                file_name=f'{datetime.date.today().strftime("%Y_%m_%d_%H_%M_%S")}'
            )
