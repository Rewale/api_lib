import json

from sync_api import ApiSync

api_client = ApiSync('GPSPROGR')

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
k: int = 1
test_messages *= k
url = 'http://192.168.0.42/getApiStruct'
test_method_http = {
    'Config': {
        'address': '192.168.0.42',
        'port': 0,
        'type': 'POST',
        'auth': False,
        'connstring': '/',
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

# Не работают сервисы
# def test_send_request():
#     # Запрос через схему апи
#     res = api_client.send_request_api('getApiStruct', {'format': "json"}, 'API', False)
#     assert res == api_client.schema


def test_http_multiple_messages():
    # Несколько запросов на схему апи
    res = api_client.make_request_api_http(test_method_http, test_messages_http)
    assert len(res) == 5 and json.loads(res[0]) == api_client.schema


def test_multiple_message_rabbit():
    list = api_client.make_request_api_amqp(test_method, test_messages)
    assert len(list) == 5 * k


def test_http_single_messages():
    res = api_client.make_request_api_http(test_method_http, test_messages_http[0])
    assert res


def test_multiple_single_rabbit():
    id = api_client.make_request_api_amqp(test_method, test_messages[0])
    assert id
