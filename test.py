import json

from sync_api import ApiSync
import custom_exceptions

api_client = ApiSync('test_service')

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


# def test_send_request():
#     # Запрос через схему апи
#     res = api_client.send_request_api('getApiStruct', {'format': "json"}, 'API', False)
#     assert res == api_client.schema
#
#
# def test_http_multiple_messages():
#     # Несколько запросов на схему апи
#     res = api_client.make_request_api_http(test_method_http, test_messages_http)
#     assert len(res) == 5 and json.loads(res[0]) == api_client.schema
#

def test_allowed_method():
    method = api_client.find_method('getApiStruct', api_client.schema['API'])
    assert api_client.check_method_available(method, api_client.schema['API'], api_client.service_name)


def test_allowed_method_no_arrays():
    method = api_client.find_method('newuser', api_client.schema['GPSPROGR'])
    assert api_client.check_method_available(method, api_client.schema['GPSPROGR'], api_client.service_name)


def test_allowed_method_in_array():
    method = api_client.find_method('method1', api_client.schema['FILESPROGR'])
    assert api_client.check_method_available(method, api_client.schema['GPSPROGR'], 'FILESPROGR')


def test_disallowed_method_in_array():
    method = api_client.find_method('method1', api_client.schema['FILESPROGR'])
    try:
        api_client.check_method_available(method, api_client.schema['GPSPROGR'], 'FILESPROGR')
    except custom_exceptions.ServiceMethodNotAllowed:
        assert True


def test_wrong_params():
    try:
        api_client.send_request_api('newuser', {'fio': "12312", 'organizations': ['111', '111']}, 'API', False)
    except custom_exceptions.MethodNotFound:
        assert True


def test_multiple_message_rabbit():
    assert api_client.make_request_api_amqp(test_method, test_messages)


