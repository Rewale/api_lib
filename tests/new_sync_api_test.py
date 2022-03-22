""" Тест полного цикла работы с синхронной АПИ двух сервисов"""
import datetime
import threading
import unittest
import uuid
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

    def test_send_message_amqp(self):
        # Запуск "Сервиса№1"
        self.thread = threading.Thread(target=self.api_callback.listen_queue)
        self.thread.start()

        # Клиент отправляет сообщение в очередь сервиса#1
        self.api_sending.send_request_api(method_name='test_method',
                                          requested_service='CallbackService',
                                          params=[
                                              InputParam(name='test_str', value='123'),
                                              InputParam(name='guid', value=str(uuid.uuid4())),
                                              InputParam(name='bin', value=b'123123'),
                                              InputParam(name='float', value=3333.3333),
                                              InputParam(name='int', value=3333),
                                              InputParam(name='bool', value=True),
                                              InputParam(name='base64', value='base64=312fdvfbg2tgt'),
                                              InputParam(name='date', value='2002-12-12T05:55:33±05:00'),
                                          ])

        sleep(0.01)

        # Проверяем что сообщение было проверено серверов
        # и попало в пользовательский обработчик
        self.assertTrue(answer != '')
        # TODO: чтение очереди колбеков

    def test_send_message_http(self):
        api = ApiSync(service_name='BDVPROGR',
                      user_api='user',
                      pass_api='Ef-PgjJ3',
                      methods={'add_client': '123', 'update_client': '123'})

        assert api.send_request_api(method_name='getApiStruct',
                                    requested_service='API',
                                    params=[InputParam(name='format', value='json')])['API']
        
    def test_send_message_http_rls_fail(self):
        api = ApiSync(service_name='BDVPROGR',
                      user_api='user',
                      pass_api='Ef-PgjJ3',
                      methods={'add_client': '123', 'update_client': '123'})
        with self.assertRaises(AllServiceMethodsNotAllowed):
            var = api.send_request_api(method_name='update_client',
                                       requested_service='TAFPROGR',
                                       params=[InputParam(name='format', value='json')])['API']

    def test_send_message_http_not_exist_method(self):
        api = ApiSync(service_name='BDVPROGR',
                      user_api='user',
                      pass_api='Ef-PgjJ3',
                      methods={'add_client': '123', 'update_client': '123'})
        with self.assertRaises(MethodNotFound):
            var = api.send_request_api(method_name='del_user',
                                       requested_service='TAFPROGR',
                                       params=[InputParam(name='format', value='json')])['API']
