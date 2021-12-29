import pika


def on_message(rmq_channel, queue: str, customer_set: set) -> set:
    while True:
        method_frame, header_frame, body = rmq_channel.basic_get(queue)
        if method_frame:
            # Если журнала нет, объект, возвращаемый в кортеж, будет None
            # print(method_frame, header_frame, body)
            customer_set.add(body.decode('UTF-8'))
            rmq_channel.basic_ack(method_frame.delivery_tag)
            on_message(rmq_channel, queue, customer_set)
        else:
            return customer_set

        # print(body.decode('UTF-8'))
    # print(header_frame)
    # channel.basic_ack(delivery_tag=method_frame.delivery_tag)


def callback_rebbit():
    rmq_url_connection_str = 'amqp://guest:F1aw2Qh33beV@prod-srv1.tkvprok.ru:5672'
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
        return on_message(rmq_channel, 'test_1_1', set())


if __name__ == '__main__':
    pass
    # callback_rebbit()
    # print(1)
