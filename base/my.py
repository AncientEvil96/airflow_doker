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

    def connection_close(self):
        self.connect.close()

    def connection_init(self):
        self.connect = connect(
            host=self.host,
            user=self.__login,
            password=self.__password,
            port=self.port,
            database=self.database)

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

        iter_ = len(load_list) // 10000 + 2
        for i in range(1, iter_):
            cursor.executemany(query, load_list[(i - 1) * 10000:i * 10000])
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
        self.connect.commit()
