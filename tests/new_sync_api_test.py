import datetime
import unittest

from sync_api import ApiSync
from tests.test_data import test_schema_rpc


class TestSyncApi(unittest.TestCase):
    # Test Data
    test_schema = test_schema_rpc


    @staticmethod
    def method(params):
        assert True
        return str(datetime.datetime.now()) + params['test_str']

    api_callback = ApiSync(service_name='CallbackService', schema=test_schema, methods={'test_method': method}, pass_api='test')
    api_sending = ApiSync('SendService', schema=test_schema, user_api='test', pass_api='test')

    def test_start_reading(self):
        """ Начать слушать очередь """

        self.api_callback.listen_queue()

    # def test_send_callback(self):
    #     """ Отправить на сервис запрос, получить корректный ответ"""
    #     pass
    #
    # def test_send_callback_error(self):
    #     """ Отправить на сервис не соответсвующий запрос """
    #     pass
