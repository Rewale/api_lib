from sync_api import ApiSync
from utils import custom_exceptions
from utils.rabbit_utils import service_amqp_url
from utils.validation_utils import check_method_available, find_method, check_date

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

    assert service_amqp_url(service_name=test_method) == 'amqp://guest:guest@192.168.0.216:5672/'


def test_date_check():
    # date = '2000-02-15T15:13:00±00:00'
    date = '2012-10-06T04:13:00+00:00'
    check_date(date)


def test_date_check_wrong_len():
    date = '2000-02-15T15:13:00+00:00'
    try:
        check_date(date)
    except AssertionError:
        assert True


def test_date_check_fail_format():
    date = '2000-02-15T15:13:00±00:00'
    try:
        check_date(date)
    except AssertionError:
        assert True
