from main import ApiSync
import custom_exceptions

api_client = ApiSync('test_service', is_sync_get_schema=True)


def test_send_request():
    res = api_client.send_request_api('getApiStruct', {'format': "json"}, 'API', False)
    assert res == api_client.schema


def test_wrong_params():
    try:
        api_client.send_request_api('newuser', {'fio': "12312", 'organizations': ['111', '111']}, 'API', False)
    except custom_exceptions.MethodNotFound:
        assert True


def test_multiple_data_send():
    res = api_client.send_request_api('getApiStruct', [{'format': "json"}, {'format': "json"}], 'API', False)
    assert res


def test_multiple_data_send_fail():
    try:
        res = api_client.send_request_api('getApiStruct', [{'format': "json"},
                                                           {'format': 'json', 'newuser': "json"}],
                                          'API', False)
    except custom_exceptions.MethodNotFound:
        assert True
