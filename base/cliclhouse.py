import requests


class ClickHouse:

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
        self.__user = conn.pop('username')
        self.connect = None

    def http_query(self, query, default_format='JSON', **kwargs):
        url = f'http://{self.host}:{self.port}'
        params = {
            'user': self.__user,
            'password': self.__password,
            'default_format': default_format
        }

        return requests.post(url=url, data=query, params=params)
