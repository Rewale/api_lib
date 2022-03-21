import unittest

from sync_api import ApiSync
from tests.test_data import test_schema_rpc
from utils.custom_exceptions import *
from utils.validation_utils import find_method, MethodApi, InputParam, check_rls


class TestValidate(unittest.TestCase):

    def setUp(self) -> None:
        self.method = find_method('test_method', test_schema_rpc['CallbackService'])

    def test_check_params_not_set(self):
        self.assertTrue(self.method.check_params([
            InputParam(name='test_str', value='fff'),
        ]))

    def test_check_params(self):
        res = self.method.check_params([
            InputParam(name='test_str', value='fff'),
            InputParam(name='test_not_set', value='fff'),
        ])

        self.assertTrue(res)

    def test_check_params_wrong_type(self):
        test_params = [
            InputParam(name='test_str', value=30404),
            InputParam(name='test_not_set', value='fff'),
        ]
        self.assertRaises(ParamValidateFail, self.method.check_params, test_params)

    def test_check_params_required_not_set(self):
        test_data = [
            InputParam(name='test_not_set', value='fff'),
        ]
        self.assertRaises(RequireParamNotSet, self.method.check_params, test_data)

    def test_check_rls(self):
        api = ApiSync(service_name='BDVPROGR',
                      user_api='user',
                      pass_api='Ef-PgjJ3',
                      methods={'add_client': '123', 'update_client': '123'})
        check_rls(service_from_schema=api.schema[api.service_name], service_to_name='API',
                  service_from_name='BDVPROGR', method_name='getApiStruct')

        with self.assertRaises(AllServiceMethodsNotAllowed):
            check_rls(service_from_schema=api.schema[api.service_name], service_to_name='TAFPROGR',
                      service_from_name='BDVPROGR', method_name='add_client')
        with self.assertRaises(ServiceMethodNotAllowed):
            # TODO: написать кейс с disallowed/allowed
            pass

    def test_check_params_not_found(self):
        test_data = [
            InputParam(name='test_str', value='123'),
            InputParam(name='test_not_set', value='fff'),
            InputParam(name='test_not_exist', value=333)
        ]
        self.assertRaises(ParamNotFound, self.method.check_params, test_data)

