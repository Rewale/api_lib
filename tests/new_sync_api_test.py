""" Тест полного цикла работы с синхронной АПИ двух сервисов"""
import datetime
import threading
import unittest
from time import sleep
from utils.custom_exceptions import *

from sync_api import ApiSync
from tests.test_data import test_schema_rpc
from utils.validation_utils import InputParam

answer = ''


def method(params):
    global answer
    answer = params
    return str(datetime.datetime.now()) + str(params)


class TestAMQPSyncApi(unittest.TestCase):
    # Test Data
    test_schema = test_schema_rpc

    api_callback = ApiSync(service_name='CallbackService', schema=test_schema,
                           methods={'test_method': method}, pass_api='test', user_api='test')
    api_sending = ApiSync('SendService', schema=test_schema, user_api='test', pass_api='test')
    thread: threading.Thread

    def test_method_not_set(self):
        with self.assertRaises(MethodNotSet):
            api_callback = ApiSync(service_name='CallbackService', schema=self.test_schema,
                                   methods={'test_not_exist': method}, pass_api='test', user_api='test')

    def test_methods_not_set(self):
        api_sending = ApiSync('SendService', schema=self.test_schema, user_api='test', pass_api='test')
        with self.assertRaises(MethodsNotSet):
            api_callback = ApiSync(service_name='CallbackService', schema=self.test_schema,
                                   pass_api='test', user_api='test')

    def test_send_message(self):
        """ Начать слушать очередь """
        # Запуск "Сервиса№1"
        self.thread = threading.Thread(target=self.api_callback.listen_queue)
        self.thread.start()

        # Клиент отправляет сообщение в очередь сервиса#1
        self.api_sending.send_request_api(method_name='test_method',
                                          requested_service='CallbackService',
                                          params=[InputParam(name='test_str', value='123')])

        sleep(0.01)

        # Проверяем что сообщение было проверено серверов
        # и попало в пользовательский обработчик
        self.assertTrue(answer == {'test_str': '123'})

        # TODO: чтение очереди колбеков

    # def tearDown(self) -> None:
    #     self.thread.join(2)
