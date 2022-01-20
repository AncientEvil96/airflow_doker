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

    def _on_message_mongo(self, rmq_channel, target):
        """
        :param rmq_channel:
        :return:
        """

        basic_ack = []

        while True:
            method_frame, header_frame, body = rmq_channel.basic_get(self.rebbit_queue)
            if method_frame:
                try:
                    # Если журнала нет, объект, возвращаемый в кортеж, будет None
                    target.update_mongo([json.loads(body.decode('UTF-8'))])
                    basic_ack.append(method_frame.delivery_tag)
                except Exception as err:
                    print(err, json.loads(body.decode('UTF-8')))
            else:
                break

        for ack in basic_ack:
            rmq_channel.basic_ack(ack)

    def callback_rebbit(self, target):
        """

        :return: list
        """
        rmq_url_connection_str = f'amqp://{self.__rebbit_login}:{self.__rebbit_password}@{self.rebbit_host}:5672'
        rmq_parameters = pika.URLParameters(rmq_url_connection_str)
        with pika.BlockingConnection(rmq_parameters) as rmq_connection:
            rmq_channel = rmq_connection.channel()
            return self._on_message_mongo(rmq_channel, target)
