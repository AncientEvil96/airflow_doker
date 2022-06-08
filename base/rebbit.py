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

    def send_rebbit(self, send_list):
        rmq_url_connection_str = f'amqp://{self.__rebbit_login}:{self.__rebbit_password}@{self.rebbit_host}:5672'
        rmq_parameters = pika.URLParameters(rmq_url_connection_str)
        with pika.BlockingConnection(rmq_parameters) as rmq_connection:
            rmq_channel = rmq_connection.channel()
            for row in send_list:
                rmq_channel.basic_publish(
                    exchange=self.rebbit_queue,
                    routing_key=self.rebbit_queue,
                    body=json.dumps(row, ensure_ascii=False)
                )

    def _on_message(self, rmq_channel):
        """
        :param rmq_channel:
        :return:
        """

        data_list = []

        while True:
            method_frame, header_frame, body = rmq_channel.basic_get(self.rebbit_queue)
            if method_frame:
                try:
                    # Если журнала нет, объект, возвращаемый в кортеж, будет None
                    data_list.append((json.loads(body.decode('UTF-8')), method_frame.delivery_tag))
                except Exception as err:
                    print(err, json.loads(body.decode('UTF-8')))
            else:
                break

        return data_list

    def callback_rebbit_mongo(self, target):

        rmq_url_connection_str = f'amqp://{self.__rebbit_login}:{self.__rebbit_password}@{self.rebbit_host}:5672'
        rmq_parameters = pika.URLParameters(rmq_url_connection_str)
        with pika.BlockingConnection(rmq_parameters) as rmq_connection:
            rmq_channel = rmq_connection.channel()
            early_ask = self._on_message(rmq_channel)
            basic_ask = target.update_mongo_rabbit(early_ask)
            for ask in basic_ask:
                rmq_channel.basic_ack(ask)

            if len(basic_ask) != len(early_ask):
                exit(1)
