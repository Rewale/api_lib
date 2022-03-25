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
        'virtualhost': '',
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
k: int = 1
test_messages *= k
test_method_http = {
    'Config': {
        'address': '192.168.0.42',
        'port': 0,
        'type': 'POST',
        'auth': False,
        'endpoint': '/',
    },
    'MethodName': 'getApiStruct'
}

test_messages_http = [
    {'format': 'json'},
    {'format': 'json'},
    {'format': 'json'},
    {'format': 'json'},
    {'format': 'json'},

]


async def foo():
    """
        Неблокирующая функция для проверки
        неблокирующего выполнения чтения
    """
    while True:
        await asyncio.sleep(0.1)
        print('Foo')


def test_multiple_rabbit_messages():
    list = loop.run_until_complete(api.make_request_api_amqp(test_method,
                                                             test_messages))
    assert len(list) == 5*k


def test_multiple_rabbit_messages_close_conn():
    list = loop.run_until_complete(api.make_request_api_amqp(test_method,
                                                             test_messages, close_connection=True))
    assert len(list) == 5*k

    list = loop.run_until_complete(api.make_request_api_amqp(test_method,
                                                             test_messages, close_connection=True))

    assert len(list) == 5 * k


def test_multiple_rabbit_messages_not_close_conn():
    list = loop.run_until_complete(api.make_request_api_amqp(test_method,
                                                             test_messages, close_connection=False))
    assert len(list) == 5 * k

    list = loop.run_until_complete(api.make_request_api_amqp(test_method,
                                                             test_messages, use_open_connection=True))

    assert len(list) == 5 * k


def test_multiple_http_messages():
    res = loop.run_until_complete(api.make_request_api_http(test_method_http,
                                                            test_messages_http))
    assert len(res) == 5


def test_single_http_messages():
    res = loop.run_until_complete(api.make_request_api_http(test_method_http,
                                                            test_messages_http[0]))
    assert res


def test_single_rabbit_messages():
    id = loop.run_until_complete(api.make_request_api_amqp(test_method,
                                                           test_messages[0]))
    assert id
