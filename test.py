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


def test_send_request():
    res = api_client.send_request_api('getApiStruct', {'format': "json"}, 'API', False)
    assert res == api_client.schema


def test_wrong_params():
    try:
        api_client.send_request_api('newuser', {'fio': "12312", 'organizations': ['111', '111']}, 'API', False)
    except custom_exceptions.MethodNotFound:
        assert True


def test_multiple_message_rabbit():
    assert api_client.make_request_api_amqp(test_method, test_messages)


def test_multiple_data_send_fail():
    try:
        res = api_client.send_request_api('getApiStruct', [{'format': "json"},
                                                           {'format': 'json', 'newuser': "json"}],
                                          'API', False)
    except custom_exceptions.MethodNotFound:
        assert True
