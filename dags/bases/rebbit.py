import pika
import json


class Rebbit:

    def __init__(self, **kwargs):
        """
        rebbitmq:
        :param kwargs:
        host:
        queue:
        password:
        login:

        """
        rebbitmq = kwargs.pop('params')
        self.rebbit_host = rebbitmq.pop('host')
        self.__rebbit_password = rebbitmq.pop('password')
        self.__rebbit_login = rebbitmq.pop('login')
        self.rebbit_queue = rebbitmq.pop('queue')

    def set_queue(self, queue):
        self.rebbit_queue = queue

    def _on_message(self, rmq_channel) -> list:
        """
        :param rmq_channel:
        :return:
        """
        queue_list = []
        while True:
            method_frame, header_frame, body = rmq_channel.basic_get(self.rebbit_queue)
            if method_frame:
                try:
                    # Если журнала нет, объект, возвращаемый в кортеж, будет None
                    queue_list.append(json.loads(body.decode('UTF-8')))
                    rmq_channel.basic_ack(method_frame.delivery_tag)
                except Exception as err:
                    print(err, json.loads(body.decode('UTF-8')))
            else:
                return queue_list

    def callback_rebbit(self):
        """

        :return: list
        """
        rmq_url_connection_str = f'amqp://{self.__rebbit_login}:{self.__rebbit_password}@{self.rebbit_host}:5672'
        rmq_parameters = pika.URLParameters(rmq_url_connection_str)
        with pika.BlockingConnection(rmq_parameters) as rmq_connection:
            rmq_channel = rmq_connection.channel()
            return self._on_message(rmq_channel)
