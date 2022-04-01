import asyncio
import unittest
import uuid

from async_api import ApiAsync
from utils.messages import CallbackMessage, IncomingMessage
from utils.validation_utils import InputParam
from test_data import test_schema_rpc as test_schema

# Переменные для учета обработки колбека
answer = ''
callback = ''
callback_true = ''
callback_add_data = ''


# Методы - обработчики
async def method(message: IncomingMessage):
    global answer
    answer = message.params
    # возврат колбека
    return message.callback_message({'key': 'value'}, True)


async def callback_method(message: CallbackMessage):
    global callback_true
    callback_true = message.response
    return


async def callback_additional_data(message: CallbackMessage):
    global callback_add_data
    callback_add_data = message.incoming_message.additional_data
    return


async def not_found_callback(message: CallbackMessage):
    global callback
    callback = message.response
    return


class TestCase(unittest.TestCase):
    loop = asyncio.get_event_loop()

    # инициализация экземпляров для работы с API
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

    def setUp(self) -> None:
        # Запускаем чтение очередей сервисов один раз
        self.loop.create_task(self.api_callback.listen_queue())
        self.loop.create_task(self.api_sending.listen_queue())

    def test_send_message_amqp(self):
        """ Отправка сообщения без обработки """

        async def main():
            # Клиент отправляет сообщение в очередь сервиса#1
            asyncio.create_task(self.api_sending.send_request_api(method_name='test_method',
                                                                  requested_service='CallbackService',
                                                                  params=[
                                                                      InputParam(name='test_str', value='123'),
                                                                      InputParam(name='guid',
                                                                                 value=str(uuid.uuid4())),
                                                                      InputParam(name='bin', value=b'123123'),
                                                                      InputParam(name='float',
                                                                                 value=3333.33),
                                                                      InputParam(name='int', value=3333),
                                                                      InputParam(name='bool', value=True),
                                                                      InputParam(name='base64',
                                                                                 value='base64=312fdvfbg2tgt'),
                                                                      InputParam(name='date',
                                                                                 value='2002-12-12T05:55:33±05:00'),
                                                                  ]))
            await asyncio.sleep(0.1)
            # Проверяем что функция обработки колбека отработала
            self.assertTrue(answer['date'] == '2002-12-12T05:55:33±05:00')

        self.loop.run_until_complete(main())

    def test_send_message_amqp_callback(self):
        """ Отправка сообщения, обработка сообщения, отправка сообщения между 2 сервисами """
        async def main():
            self.api_sending.methods_callback = {'callbackMethod': callback_method}
            asyncio.create_task(self.api_sending.send_request_api(method_name='test_method',
                                                                  requested_service='CallbackService',
                                                                  callback_method_name='callbackMethod',
                                                                  params=[
                                                                      InputParam(name='test_str', value='123'),
                                                                      InputParam(name='guid',
                                                                                 value=str(uuid.uuid4())),
                                                                      InputParam(name='bin', value=b'123123'),
                                                                      InputParam(name='float',
                                                                                 value=3333.33),
                                                                      InputParam(name='int', value=3333),
                                                                      InputParam(name='bool', value=True),
                                                                      InputParam(name='base64',
                                                                                 value='base64=312fdvfbg2tgt'),
                                                                      InputParam(name='date',
                                                                                 value='2002-12-12T05:55:33±05:00'),
                                                                  ]))
            await asyncio.sleep(0.1)
            # Проверяем что функция обработки колбека отработала
            self.assertTrue(callback_true['key'] == 'value')
            print(callback_true)

        self.loop.run_until_complete(main())

    def test_not_found_callback_method(self):
        """ Отправка сообщения без указания метода колбека """

        async def main():
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
                                                                                 value=3333.33),
                                                                      InputParam(name='int', value=3333),
                                                                      InputParam(name='bool', value=True),
                                                                      InputParam(name='base64',
                                                                                 value='base64=312fdvfbg2tgt'),
                                                                      InputParam(name='date',
                                                                                 value='2002-12-12T05:55:33±05:00'),
                                                                  ]))
            await asyncio.sleep(0.1)
            # Проверяем что функция обработки колбека не отработала
            self.assertTrue(callback == '')

        self.loop.run_until_complete(main())

    def test_send_message_amqp_callback_add_data(self):
        """ Отправка сообщения с доп. данными """

        async def main():
            self.api_sending.methods_callback = {'callbackMethod': callback_additional_data}
            asyncio.create_task(self.api_sending.send_request_api(method_name='test_method',
                                                                  requested_service='CallbackService',
                                                                  callback_method_name='callbackMethod',
                                                                  params=[
                                                                      InputParam(name='test_str', value='123'),
                                                                      InputParam(name='guid',
                                                                                 value=str(uuid.uuid4())),
                                                                      InputParam(name='bin', value=b'123123'),
                                                                      InputParam(name='float',
                                                                                 value=3333.33),
                                                                      InputParam(name='int', value=3333),
                                                                      InputParam(name='bool', value=True),
                                                                      InputParam(name='base64',
                                                                                 value='base64=312fdvfbg2tgt'),
                                                                      InputParam(name='date',
                                                                                 value='2002-12-12T05:55:33±05:00'),
                                                                  ], additional_data={'user_id': 123123}))
            await asyncio.sleep(0.1)
            # Проверяем что функция обработки колбека отработала
            self.assertTrue(callback_true['key'] == 'value')

        self.loop.run_until_complete(main())
