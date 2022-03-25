import unittest
import asyncio
import uuid
from datetime import datetime
from time import sleep

from api_lib.utils.rabbit_utils import CallbackMessage
from test_data import test_schema_rpc as test_schema
from api_lib.async_api3 import ApiAsync
from api_lib.utils.validation_utils import InputParam, convert_date_into_iso, create_callback_message_amqp

answer = ''
callback = ''


async def method(params: dict, response_id: str, service_callback: str, method: str, method_callback: str):
    # TODO тип для входящего сообщения
    global answer
    answer = params
    params['CreateDate'] = convert_date_into_iso(datetime.now())
    return create_callback_message_amqp(params, True, response_id), True


async def callbackMethod(message: CallbackMessage):
    global callback
    callback = message.response
    return


class TestCase(unittest.TestCase):
    loop = asyncio.get_event_loop()
    api_callback = loop.run_until_complete(
        ApiAsync.create_api_async(service_name='CallbackService',
                                  schema=test_schema,
                                  methods={'test_method': method},
                                  pass_api='test',
                                  user_api='test',
                                  ))

    api_sending: ApiAsync = loop.run_until_complete(
        ApiAsync.create_api_async(service_name='SendService',
                                  schema=test_schema,
                                  user_api='test',
                                  pass_api='test'))

    def test_send_message_amqp(self):
        async def main():
            # Запуск "Сервиса№1"
            asyncio.create_task(self.api_callback.listen_queue())
            # Клиент отправляет сообщение в очередь сервиса#1
            asyncio.create_task(self.api_sending.send_request_api(method_name='test_method',
                                                                  requested_service='CallbackService',
                                                                  params=[
                                                                      InputParam(name='test_str', value='123'),
                                                                      InputParam(name='guid',
                                                                                 value=str(uuid.uuid4())),
                                                                      InputParam(name='bin', value=b'123123'),
                                                                      InputParam(name='float',
                                                                                 value=3333.3333),
                                                                      InputParam(name='int', value=3333),
                                                                      InputParam(name='bool', value=True),
                                                                      InputParam(name='base64',
                                                                                 value='base64=312fdvfbg2tgt'),
                                                                      InputParam(name='date',
                                                                                 value='2002-12-12T05:55:33±05:00'),
                                                                  ]))
            await asyncio.sleep(0.01)
            # Проверяем что функция обработки колбека отработала
            self.assertTrue(answer['date'] == '2002-12-12T05:55:33±05:00')

        self.loop.run_until_complete(main())

    def test_send_message_amqp_callback(self):
        async def main():
            # Запуск "Сервиса№1"
            asyncio.create_task(self.api_callback.listen_queue())
            asyncio.create_task(self.api_sending.listen_queue())
            self.api_sending.methods_callback = {'callbackMethod': callbackMethod}
            # Клиент отправляет сообщение в очередь сервиса#1
            asyncio.create_task(self.api_sending.send_request_api(method_name='test_method',
                                                                  requested_service='CallbackService',
                                                                  callback_method_name='callbackMethod',
                                                                  params=[
                                                                      InputParam(name='test_str', value='123'),
                                                                      InputParam(name='guid',
                                                                                 value=str(uuid.uuid4())),
                                                                      InputParam(name='bin', value=b'123123'),
                                                                      InputParam(name='float',
                                                                                 value=3333.3333),
                                                                      InputParam(name='int', value=3333),
                                                                      InputParam(name='bool', value=True),
                                                                      InputParam(name='base64',
                                                                                 value='base64=312fdvfbg2tgt'),
                                                                      InputParam(name='date',
                                                                                 value='2002-12-12T05:55:33±05:00'),
                                                                  ]))
            await asyncio.sleep(0.1)
            # Проверяем что функция обработки колбека отработала
            print(callback)
            self.assertTrue(callback != '')

        self.loop.run_until_complete(main())
