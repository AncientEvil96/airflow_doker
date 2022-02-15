import pandas as pd
from base.operations_to_files import File
from mysql.connector import connect, Error


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
        self.port = conn.pop('port')
        self.__password = conn.pop('password')
        self.__login = conn.pop('login')
        self.database = conn.pop('database')

    def select_to_df(self, query) -> pd.DataFrame:
        """
        получение данных в формате DataFrame
        :return: DataFrame
        """
        try:
            with connect(
                    host=self.host,
                    user=self.__login,
                    password=self.__password,
                    port=self.port,
                    database=self.database
            ) as cnxn:
                return pd.read_sql(query, cnxn)
        except Error as e:
            print(e)

    def load_csv_to_base(self, query, file_name) -> str:
        """
        получение данных в файле parquet
        :return: file path
        """
        try:
            with connect(
                    host=self.host,
                    user=self.__login,
                    password=self.__password,
                    port=self.port,
                    database=self.database
            ) as cnxn:
                cnxn.
        except Error as e:
            print(e)

    def select_to_dict(self, query) -> list:
        """
        получение данных в list of dict
        :return: list
        """

        with pyodbc.connect(self.conn_ms) as cnxn:
            df = pd.read_sql(query, cnxn)
            return df.to_dict('records')
