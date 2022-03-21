import asyncio
import json
import multiprocessing

from async_api import ApiAsync
from tests.old_tests.async_send_test import test_messages, test_method

loop = asyncio.get_event_loop()
api: ApiAsync = loop.run_until_complete(ApiAsync.create_api_async('RECOGNIZE'))


# Создаем в фоне задачу на чтение определенной очереди и запись в редис
# loop.create_task(redis_read_worker.start_listening('testQuenue', ApiAsync.amqp_url_from_method(test_method)))


# def test_read_redis():
#     # Проверка на "неблокируемость"
#     loop.create_task(foo())
#     message_with_date = test_messages[0]
#     date = str(datetime.datetime.now())
#     message_with_date['date'] = date
#     # Отправляем в эту же очередь сообщение
#     id = loop.run_until_complete(api.make_request_api_amqp(test_method,
#                                                            message_with_date))
#     # Читаем из редис наличие сообщения
#     callback_message = loop.run_until_complete(api.read_redis(id))
#
#     assert isinstance(callback_message, dict) and callback_message == {'test': 1232, 'test2': 'ffdsf',
#                                                                        'service_callback': 'RECOGNIZE',
#                                                                        'method': 'test',
#                                                                        'date': date}
#
#
# def test_read_redis_hard():
#     # Проверка на "неблокируемость"
#     loop.create_task(foo())
#     message_with_date = test_messages[0]
#     for _ in range(0, 100):
#         date = str(datetime.datetime.now())
#         message_with_date['date'] = date
#         # Отправляем в эту же очередь сообщение
#         id = loop.run_until_complete(api.make_request_api_amqp(test_method,
#                                                                message_with_date))
#         # Читаем из редис наличие сообщения
#         callback_message = loop.run_until_complete(api.read_redis(id))
#
#         assert isinstance(callback_message, dict) and callback_message == {'test': 1232, 'test2': 'ffdsf',
#                                                                            'service_callback': 'RECOGNIZE',
#                                                                            'method': 'test',
#                                                                            'date': date}
#
#
# def test_read_redis_timeout():
#     async def main():
#         # Проверка на "неблокируемость"
#         loop.create_task(foo())
#         # Читаем из редиса наличие сообщения
#         key = 'not-existed-key'
#         try:
#             callback_message = await api.read_redis(key, timeout=3)
#         except TimeoutError:
#             assert True
#             return
#         except Exception as e:
#             pass
#
#         assert False
#
#     loop.run_until_complete(main())


def test_rpc_timeout():
    try:
        result = loop.run_until_complete(api.rpc_amqp(test_method, test_messages[0], '12312'))
    except TimeoutError:
        assert True


test_method = {
    'config': {
        'username': 'guest',
        'password': 'guest',
        'address': '192.168.0.216',
        'port': 5672,
        'virtualhost': '',
        'exchange': 'testExchange',
        'quenue': 'fines_parsing'
    },
    'MethodName': 'test'
}


def test_rpc():
    def run_psevdoservice():
        """ Эмуляция сервиса """
        import pika
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            virtual_host='/',
            host='192.168.0.216'))
        channel = connection.channel()
        channel.queue_declare(queue='callback_queue')
        channel.queue_declare(queue='fines_parsing')

        def on_request(ch, method, props: pika.BasicProperties, body):
            response = {'text': "test_callback_message", 'id': json.loads(body)['id']}
            response = json.dumps(response)
            ch.basic_publish(exchange='',
                             routing_key='callback_queue',
                             body=response)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_consume(on_message_callback=on_request, queue='fines_parsing')
        channel.start_consuming()
    process = multiprocessing.Process(target=run_psevdoservice)
    process.start()
    result = loop.run_until_complete(api.rpc_amqp(test_method, test_messages[0], 'callback_queue'))
    assert result['text'] == 'test_callback_message'
    process.terminate()
    process.join()

