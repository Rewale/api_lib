import asyncio
from async_api import ApiAsync

loop = asyncio.get_event_loop()
api: ApiAsync = loop.run_until_complete(ApiAsync.create_api_async('RECOGNIZE'))

test_method = {
    'config': {
        'username': 'guest',
        'password': 'guest',
        'address': '192.168.0.216',
        'port': 5672,
        'exchange': 'testExchange',
        'quenue': 'testQuenue'
    },
    'MethodName': 'test'
}

test_messages = [
    {'test': 1232, 'test2': 'ffdsf'},
    {'test': 12312, 'test2': 'gfg'},
    {'test': 5435, 'test2': 'asd'},
    {'test': 456, 'test2': 'zx'},
    {'test': 767, 'test2': 'lk'},
]


def test_multiple_rabbit_messages():
    loop.run_until_complete(api.make_request_api_amqp(test_method,
                                                      test_messages))
    # loop.close()
    # TODO добавить проверку на сообщения
    assert True
