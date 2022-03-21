import unittest

from tests.test_data import test_schema_rpc
from utils.custom_exceptions import *
from utils.validation_utils import find_method, MethodApi, InputParam


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

    def test_check_params_not_found(self):
        test_data = [
            InputParam(name='test_str', value='123'),
            InputParam(name='test_not_set', value='fff'),
            InputParam(name='test_not_exist', value=333)
        ]
        self.assertRaises(ParamNotFound, self.method.check_params, test_data)

