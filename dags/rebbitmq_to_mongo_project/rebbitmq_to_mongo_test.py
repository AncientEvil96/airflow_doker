import pika
import json
from pymongo import MongoClient


# def on_message(channel, method, properties, body):
#     message = json.loads(body.decode('UTF-8'))
#     print(message)
#     ack = update_pymongo(message)
#     if ack:
#         channel.basic_ack(method.delivery_tag)


def on_message(rmq_channel, queue: str, customer: list = list()) -> list:
    i = 0
    while True:
        i += 1
        method_frame, header_frame, body = rmq_channel.basic_get(queue)
        if method_frame:
            try:
                # Если журнала нет, объект, возвращаемый в кортеж, будет None
                customer.append(json.loads(body.decode('UTF-8')))
                # rmq_channel.basic_ack(method_frame.delivery_tag)
                # on_message(rmq_channel, queue, customer_set)
            except Exception as err:
                print(err, json.loads(body.decode('UTF-8')))

            if i == 100:
                return customer
        else:
            return customer


def callback_rebbit(srv, login, password):
    rmq_url_connection_str = f'amqp://{login}:{password}@{srv}:5672'
    rmq_parameters = pika.URLParameters(rmq_url_connection_str)
    with pika.BlockingConnection(rmq_parameters) as rmq_connection:
        rmq_channel = rmq_connection.channel()
        # rmq_channel.basic_consume('MDB_WhoIs_queue_customer_v1', on_message)
        # try:
        #     rmq_channel.start_consuming()
        #     # Постоянно контролировать очередь в методе блокировки
        # except KeyboardInterrupt:
        #     rmq_channel.stop_consuming()

        # customer_set = on_message(rmq_channel, 'MDB_WhoIs_queue_customer_v1', set())
        return on_message(rmq_channel, 'MDB_WhoIs_queue_customer_v1')


def load_pymongo(load_list: list, **connection):
    host = connection.pop('host')  # '84.38.187.211'
    port = connection.pop('port')  # 27017
    schema = connection.pop('schema')  # 'info_checks'
    database = connection.pop('database')  # 'customers'
    login = connection.pop('login')  # 'transfer'
    password = connection.pop('password')  # 'QXm6ditoC06BaoA6iZbS'

    url = f'mongodb://{login}:{password}@{host}:{port}/{schema}'

    with MongoClient(url) as client:
        base = client[schema]
        collection = base[database]
        # collection.insert_many(load_list)
        for load_doc in load_list:
            try:
                collection.update_one({'uuid': {'$eq': load_doc['uuid']}}, {'$set': load_doc}, upsert=True)
            except Exception as err:
                print(err, load_doc)


def update_pymongo(load_doc: dict):
    host = '84.38.187.211'
    port = 27017
    schema = 'info_checks'
    database = 'checks'
    login = 'transfer'
    password = 'QXm6ditoC06BaoA6iZbS'

    url = f'mongodb://{login}:{password}@{host}:{port}/{schema}'

    try:
        with MongoClient(url) as client:
            base = client[schema]
            collection = base[database]
            collection.update_one({'uuid': {'$eq': load_doc['uuid']}}, load_doc, upsert=True)

        return True
    except Exception as err:
        print(err, load_doc)
        return False


# def load_2(list_message: list):
#     host = '84.38.187.211'
#     port = 27017
#     schema = 'info_checks'
#     database = 'checks'
#     login = 'transfer'
#     password = 'QXm6ditoC06BaoA6iZbS'
#
#     url = f'mongodb://{login}:{password}@{host}:{port}/{schema}'
#
#     mongo = MongoHook(
#         'mongodb',
#         connection={
#             'port': port,
#             'host': host,
#             'login': login,
#             'password': password,
#             'database': database
#         }
#     )
#     mongo.uri = url
#     #     '',
#     #     connection={
#     #         'port': port,
#     #         'host': server_mongo,
#     #         'login': login,
#     #         'password': password,
#     #         'database': database
#     #     },
#     #     # extras={
#     #     #     'srv': schema
#     #     # }
#     # )
#     mongo.insert_many(list_message)


if __name__ == '__main__':
    my_list = callback_rebbit()
    print(len(my_list))
    load_pymongo(
        my_list,
        host='84.38.187.211',
        port=27017,
        schema='info_checks',
        database='checks',
        login='transfer',
        password='QXm6ditoC06BaoA6iZbS'
    )
