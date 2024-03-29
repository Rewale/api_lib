""" Тест полного цикла работы с синхронной АПИ двух сервисов"""
import datetime
import threading
import unittest
import uuid
from time import sleep
from api_lib.utils.custom_exceptions import *

from api_lib.sync_api import ApiSync
from api_lib.tests.test_data import test_schema_rpc
from api_lib.utils.messages import create_callback_message_amqp, IncomingMessage
from api_lib.utils.validation_utils import InputParam, convert_date_into_iso

answer = ''
count_recheck = 0


def method(message: IncomingMessage):
    global answer
    answer = message.params
    return message.callback_message({'key': 'value'}, True)


def recheck_method(message: IncomingMessage):
    global count_recheck
    count_recheck += 1
    date_recheck = datetime.datetime.now() + datetime.timedelta(seconds=2)
    return message.recheck_message(date_recheck)


class TestAMQPSyncApi(unittest.TestCase):
    # Test Data
    test_schema = test_schema_rpc

    api_callback = ApiSync(service_name='CallbackService', schema=test_schema,
                           methods={'test_method': method}, pass_api='test', user_api='test', is_test=False)
    api_sending = ApiSync('SendService', schema=test_schema, user_api='test', pass_api='test', is_test=False)
    thread: threading.Thread

    def test_method_not_set(self):
        with self.assertRaises(MethodNotSet):
            api_callback = ApiSync(service_name='CallbackService', schema=self.test_schema,
                                   methods={'test_not_exist': method}, pass_api='test', user_api='test', is_test=False)

    def test_methods_not_set(self):
        api_sending = ApiSync('SendService', schema=self.test_schema, user_api='test', pass_api='test', is_test=False)
        with self.assertRaises(MethodsNotSet):
            api_callback = ApiSync(service_name='CallbackService', schema=self.test_schema,
                                   pass_api='test', user_api='test', is_test=False)

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
                                              InputParam(name='float', value=3333.33),
                                              InputParam(name='int', value=3333),
                                              InputParam(name='bool', value=True),
                                              InputParam(name='base64', value='base64=312fdvfbg2tgt'),
                                              InputParam(name='date', value='2002-12-12T05:55:33±05:00'),
                                          ])

        sleep(0.01)

        # Проверяем что сообщение было проверено библиотекой
        # и попало в пользовательский обработчик
        self.assertTrue(answer['date'] == '2002-12-12T05:55:33±05:00')

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

    # def test_recheck(self):
    #     """ Каждый раз откладываем сообщение на 2 секунды """
    #     test_schema = test_schema_rpc
    #     api_callback = ApiSync(service_name='CallbackService', schema=test_schema,
    #                            methods={'test_method': recheck_method}, pass_api='test', user_api='test')
    #     api_sending = ApiSync('SendService', schema=test_schema, user_api='test', pass_api='test')
    #     self.thread = threading.Thread(target=api_callback.listen_queue)
    #     self.thread.start()
    #
    #     # Клиент отправляет сообщение в очередь сервиса#1
    #     api_sending.send_request_api(method_name='test_method',
    #                                  requested_service='CallbackService',
    #                                  params=[
    #                                      InputParam(name='test_str', value='123'),
    #                                      InputParam(name='guid', value=str(uuid.uuid4())),
    #                                      InputParam(name='bin', value=b'123123'),
    #                                      InputParam(name='float', value=3333.33),
    #                                      InputParam(name='int', value=3333),
    #                                      InputParam(name='bool', value=True),
    #                                      InputParam(name='base64', value='base64=312fdvfbg2tgt'),
    #                                      InputParam(name='date', value='2002-12-12T05:55:33±05:00'),
    #                                  ])
    #
    #     sleep(10)
    #     global count_recheck
    #     # Проверяем что сообщение было проверено библиотекой
    #     # и попало в пользовательский обработчик несколько раз
    #     self.assertTrue(2 <= count_recheck <= 5)
    #     print(f"Сообщений: {count_recheck}")
