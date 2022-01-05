import pika
import json
from pymongo import MongoClient


# def on_message(channel, method, properties, body):
#     message = json.loads(body.decode('UTF-8'))
#     print(message)
#     ack = update_pymongo(message)
#     if ack:
#         channel.basic_ack(method.delivery_tag)

class RebbitMongoETL:
    def __init__(self, **kwargs):
        """
        rebbitmq:
        :param kwargs:
        host:
        queue:
        password:
        login:

        mongodb:
        :param kwargs:
        host:
        password:
        login:
        port:
        schema:
        database:

        """
        rebbitmq = kwargs.pop('rebbitmq')
        self.rebbit_host = rebbitmq.pop('host')
        self.__rebbit_password = rebbitmq.pop('password')
        self.__rebbit_login = rebbitmq.pop('login')
        self.rebbit_queue = rebbitmq.pop('queue')

        mongodb = kwargs.pop('mongodb')
        self.mongo_host = mongodb.pop('host')
        self.__mongo_password = mongodb.pop('password')
        self.__mongo_login = mongodb.pop('login')
        self.mongo_port = mongodb.pop('port')
        self.mongo_schema = mongodb.pop('schema')
        self.mongo_database = mongodb.pop('database')

    def on_message(self, rmq_channel) -> list:
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
        rmq_url_connection_str = f'amqp://{self.__rebbit_login}:{self.__rebbit_password}@{self.rebbit_host}:5672'
        rmq_parameters = pika.URLParameters(rmq_url_connection_str)
        with pika.BlockingConnection(rmq_parameters) as rmq_connection:
            rmq_channel = rmq_connection.channel()
            return self.on_message(rmq_channel)

    def load_pymongo(self, load_list: list):
        url = f'mongodb://{self.__mongo_login}:{self.__mongo_password}@{self.mongo_host}:{self.mongo_port}/{self.mongo_schema}'

        with MongoClient(url) as client:
            base = client[self.mongo_schema]
            collection = base[self.mongo_database]
            for load_doc in load_list:
                try:
                    collection.update_one({'uuid': {'$eq': load_doc['uuid']}}, {'$set': load_doc}, upsert=True)
                except Exception as err:
                    print(err, load_doc)

    def update_pymongo(self, load_doc: dict):
        url = f'mongodb://{self.__mongo_login}:{self.__mongo_password}@{self.mongo_host}:{self.mongo_port}/{self.mongo_schema}'
        try:
            with MongoClient(url) as client:
                base = client[self.mongo_schema]
                collection = base[self.mongo_database]
                collection.update_one({'uuid': {'$eq': load_doc['uuid']}}, load_doc, upsert=True)
            return True
        except Exception as err:
            print(err, load_doc)
            return False


if __name__ == '__main__':
    pass
    # my_list = callback_rebbit()
    # print(len(my_list))
    # load_pymongo(
    #     my_list,
    #     host='84.38.187.211',
    #     port=27017,
    #     schema='info_checks',
    #     database='checks',
    #     login='transfer',
    #     password='QXm6ditoC06BaoA6iZbS'
    # )
