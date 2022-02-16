import pandas as pd
from mysql.connector import connect, Error


class MySQL:
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
        self.connect = None

    # def connection_close(self):
    #     self.connect.close()

    def init_connection(self):
        self.connect = connect(
            host=self.host,
            user=self.__login,
            password=self.__password,
            port=self.port,
            database=self.database)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connect.close()

    def select_to_df(self, query: str) -> pd.DataFrame:
        """
        получение данных в формате DataFrame
        :return: DataFrame
        """
        return pd.read_sql(query, self.connect)

    # def load_csv_to_base(self, query: str, file_name) -> str:
    #     """
    #     получение данных в файле parquet
    #     :return: file path
    #     """
    #
    #     query = """
    #
    #     """
    #
    #     try:
    #         with connect(
    #                 host=self.host,
    #                 user=self.__login,
    #                 password=self.__password,
    #                 port=self.port,
    #                 database=self.database
    #         ) as cnxn:
    #             cursor = cnxn.cursor()
    #             cursor.execute(query)
    #     except Error as e:
    #         print(e)

    def load_many_to_base(self, query: str, load_list):
        """
        загрузка данных в файле list
        """
        if self.connect is None:
            print('use init_connection function')
            return
        cursor = self.connect.cursor()
        cursor.executemany(query, load_list)
        self.connect.commit()

    def query_to_base(self, query: str):
        """
        выполнение любого запроса
        """
        if self.connect is None:
            print('use init_connection function')
            return

        cursor = self.connect.cursor()
        cursor.execute(query)
