import api_lib.utils.utils
from api_lib.async_api import ApiAsync
from api_lib.sync_api import ApiSync
import api_lib.custom_exceptions as custom_exceptions

import api_lib.utils.utils as utils
from api_lib.utils.utils import check_method_available, find_method

api_client = ApiSync('GPSPROGR')


def test_allowed_method():
    method = find_method('getApiStruct', api_client.schema['API'])
    assert check_method_available(method, api_client.schema['API'], api_client.service_name)


def test_allowed_method_no_arrays():
    method = find_method('newuser', api_client.schema['GPSPROGR'])
    assert check_method_available(method, api_client.schema['GPSPROGR'], api_client.service_name)


def test_allowed_method_in_array():
    method = find_method('method1', api_client.schema['FILESPROGR'])
    assert check_method_available(method, api_client.schema['GPSPROGR'], 'FILESPROGR')


def test_disallowed_method_in_array():
    method = find_method('method1', api_client.schema['FILESPROGR'])
    try:
        check_method_available(method, api_client.schema['GPSPROGR'], 'FILESPROGR')
    except custom_exceptions.ServiceMethodNotAllowed:
        assert True


def test_check_params_method_not_found():
    try:
        api_client.send_request_api('newuser', {'fio': "12312", 'organizations': ['111', '111']}, 'API', False)
    except custom_exceptions.MethodNotFound:
        assert True


def test_check_params_wrong_type():
    try:
        api_client.send_request_api('getApiStruct', {'format': 123123}, 'API', False)
    except AssertionError:
        assert True


def test_check_params_require_param_not_set():
    try:
        api_client.send_request_api('getApiStruct', {}, 'API', False)
    except custom_exceptions.RequireParamNotSet:
        assert True


def test_check_params_require_param_not_exist():
    try:
        api_client.send_request_api('getApiStruct', {'fio': '123', 'format': '4444'}, 'API', False)
    except custom_exceptions.ParamNotFound:
        assert True


def test_amqp():
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

    assert ApiAsync.amqp_url_from_method(method=test_method) == 'amqp://guest:guest@192.168.0.216:5672/'


def test_response_json():
    # TODO: написать нормальный тест совпадения хеша 1с
    response = utils.json_to_response('{"data":"ok"}', 111, True)
    assert response


def test_queue_name():
    assert api_lib.utils.utils.get_queue_service("BDVPROGR", api_client.schema) == 'bdvprogr'
