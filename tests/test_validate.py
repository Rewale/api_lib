from sync_api import ApiSync
import custom_exceptions


api_client = ApiSync('GPSPROGR')


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
        api_client.send_request_api('getApiStruct', {'format': '4444', 'fio': '123'}, 'API', False)
    except custom_exceptions.ParamNotFound:
        assert True
