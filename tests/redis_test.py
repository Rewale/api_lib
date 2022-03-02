import asyncio
import redis_read_worker
import datetime
from async_api import ApiAsync
from tests.async_send_test import foo, test_messages, test_method

loop = asyncio.get_event_loop()
api: ApiAsync = loop.run_until_complete(ApiAsync.create_api_async('RECOGNIZE'))

# Создаем в фоне задачу на чтение определенной очереди и запись в редис
loop.create_task(redis_read_worker.start_listening('testQuenue', ApiAsync.amqp_url_from_method(test_method)))


def test_read_redis():
    # Проверка на "неблокируемость"
    loop.create_task(foo())
    message_with_date = test_messages[0]
    date = str(datetime.datetime.now())
    message_with_date['date'] = date
    # Отправляем в эту же очередь сообщение
    id = loop.run_until_complete(api.make_request_api_amqp(test_method,
                                                           message_with_date))
    # Читаем из редис наличие сообщения
    callback_message = loop.run_until_complete(api.read_redis(id))

    assert isinstance(callback_message, dict) and callback_message == {'test': 1232, 'test2': 'ffdsf',
                                                                       'service_callback': 'RECOGNIZE',
                                                                       'method': 'test',
                                                                       'date': date}


def test_read_redis_timeout():
    async def main():
        # Проверка на "неблокируемость"
        loop.create_task(foo())
        # Читаем из редиса наличие сообщения
        key = 'not-existed-key'
        try:
            callback_message = await api.read_redis(key, timeout=3)
        except TimeoutError:
            assert True
            return
        except Exception as e:
            pass

        assert False

    loop.run_until_complete(main())
