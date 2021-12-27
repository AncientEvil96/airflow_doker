import pika


def start():
    rmq_url_connection_str = 'amqp://guest:F1aw2Qh33beV@prod-srv1.tkvprok.ru:5672'

    rmq_parameters = pika.URLParameters(rmq_url_connection_str)
    with pika.BlockingConnection(rmq_parameters) as rmq_connection:
        rmq_channel = rmq_connection.channel()


        rmq_channel.queue_declare(
            queue='test_1_1',
            durable=True
        )
        rmq_channel.queue_delete('test_1_1')

if __name__=='__main__':
    # start()
    print(1)