import unittest

from tests.test_data import test_schema_rpc
from utils.validation_utils import find_method, MethodApi


class FindMethodTest(unittest.TestCase):

    def test_find_method(self):
        method = find_method('test_method', test_schema_rpc['CallbackService'])
        assert isinstance(method, MethodApi)
