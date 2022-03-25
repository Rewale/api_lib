import asyncio
import json
from typing import List, Union

from api_lib.utils.rabbit_utils import *
import aio_pika

import aiohttp

from api_lib.utils.validation_utils import MethodApi, InputParam, serialize_message, check_rls, \
    create_callback_message_amqp, find_method


class ApiAsync(object):
    @classmethod
    async def create_api_async(cls, user_api, pass_api, service_name: str,
                               methods: dict = None,
                               methods_callback: dict = None,
                               url='http://apidev.mezex.lan/getApiStructProgr',
                               redis_url: str = "redis://localhost",
                               schema: dict = None):
        r"""
         Создание экземпляра класса
         Args:
              service_name(str): Название текущего сервиса
              redis_url(str): Строка подключения для чтения редис
            user_api: логин для получения схемы
            pass_api: пароль для получения схемы
            url: адрес для схемы
            service_name: Название текущего сервиса
            methods: Словарь обработчиков для каждого метода сервиса {название метода: функция}
        функция(params: dict,
        response_id: str,
        service_callback: str,
        method: str,
        method_callback: str)
        -> (сообщение: dict, результат: bool):
        :param pass_api:
        :param methods:
        :param schema:
        :param url:
        :param redis_url:
        :param methods_callback:
         """
        self = ApiAsync(service_name, user_api, pass_api, redis_url, methods, url, schema, methods_callback)
        # Для тестов можно загружать словарь
        self.schema = None
        if schema is not None:
            self.schema = schema
        else:
            await self.get_schema()

        return self

    def __init__(self, service_name: str,
                 user_api,
                 pass_api,
                 redis_url: str,
                 methods: dict = None,
                 url='http://apidev.mezex.lan/getApiStructProgr',
                 schema: dict = None,
                 methods_callback = None):
        r"""
        НИЗЯ!
        Args:
            user_api: логин для получения схемы
            pass_api: пароль для получения схемы
            url: адрес для схемы
            service_name: Название текущего сервиса
            methods: Словарь обработчиков для каждого метода сервиса {название метода: функция}
        функция(params: dict,
        response_id: str,
        service_callback: str,
        method: str,
        method_callback: str)
        -> (сообщение: dict, результат: bool):
        """
        self.channel = None
        self.connection = None
        self.service_name = service_name
        self.redis_url = redis_url
        self.redis = None

        # Данные для получения схемы апи
        self.url = url
        self.user_api = user_api
        self.pass_api = pass_api

        # Обработчики методов сервиса
        self.methods_service = methods

        # AMQP
        self.port = None
        self.pass_amqp = None
        self.queue = None
        self.exchange = None
        self.address_amqp = None
        self.user_amqp = None
        self.credentials = None
        self.schema = schema

    async def get_schema(self) -> dict:
        """ Асинхронное получение схемы """
        async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(self.user_api, self.pass_api)) as session:
            async with session.post(self.url, data={'format': 'json'}) as response:
                text_json = await response.text()
        content_json = json.loads(text_json)
        self.schema = content_json
        return content_json

    async def listen_queue(self):
        connection = await self.make_connection()
        channel: aio_pika.Channel = await connection.channel()
        queue_name = get_queue_service(self.service_name, self.schema)
        queue: aio_pika.Queue = await channel.get_queue(queue_name)
        async with queue.iterator() as queue_iter:
            # TODO: добавить обработку коллбеков от других сервисов
            async for message in queue_iter:
                message: aio_pika.IncomingMessage
                async with message.process():
                    # TODO: обработка колбеков
                    # Проверка серилизации
                    try:
                        data = json.loads(message.body.decode('utf-8'))
                    except Exception as e:
                        return
                    if 'response_id' in data:
                        try:
                            await self.process_callback_message(data, channel)
                        except Exception as e:
                            pass
                    else:
                        try:
                            await self.process_incoming_message(data, channel)
                        except:
                            pass


    async def api_http_request(self, method: MethodApi, params: List[InputParam]):
        url = method.get_url_http()
        if method.config.auth:
            auth = aiohttp.BasicAuth(method.config.username,
                                     method.config.password)
        else:
            auth = None

        message = method.get_message_http(params)
        async with aiohttp.ClientSession(auth=auth) as session:
            if method.config.type == 'POST':
                async with session.post(url, data=message) as resp:
                    return await resp.json()
            elif method.config.type == 'GET':
                async with session.get(url, data=message) as resp:
                    return await resp.json()

    async def api_amqp_request(self, method: MethodApi, params: List[InputParam], callback_method_name: str = ''):
        connection = await self.make_connection()
        channel: aio_pika.Channel = await connection.channel()
        message = method.get_message_amqp(params, self.service_name, callback_method_name)

        exchange = await channel.get_exchange(name=method.config.exchange)
        await exchange.publish(message=aio_pika.Message(serialize_message(message).encode('utf-8')),
                               routing_key=get_route_key(method.config.quenue))

        return message['id']

    async def make_connection(self, service_name: str = None) -> aio_pika.Connection:
        """ Создаем подключение """
        if service_name is not None:
            url = service_amqp_url(self.schema[service_name])
        else:
            url = service_amqp_url(self.schema[self.service_name])

        conn = await aio_pika.connect_robust(
            url,
            loop=asyncio.get_event_loop()
        )
        return conn

    async def send_request_api(self, method_name: str,
                               params: List[InputParam], requested_service: str) -> str:
        r"""
        Отправка запроса на сервис

        Args:
           method_name: имя метод апи.
           params: Параметры.
           requested_service: Имя сервиса - адресата.
        Raises:
           ServiceMethodNotAllowed - Метод сервиса не доступен из текущего метода.
           AssertionError - тип параметра не соответствует типу в методе.
           RequireParamNotSet - не указан обязательный параметр.
           ParamNotFound - параметр не найден
        Returns:
            При AMQP - id сообщения (его хеш-сумма). При HTTP - текст сообщения
        """

        if requested_service not in self.schema:
            raise ServiceNotFound
        method = find_method(method_name, self.schema[requested_service])
        check_rls(self.schema[self.service_name], requested_service, self.service_name, method_name)

        if method.type_conn == 'HTTP':
            return await self.api_http_request(method, params)

        if method.type_conn == 'AMQP':
            return await self.api_amqp_request(method, params)

    async def process_incoming_message(self, data: dict, channel):
        queue_callback = None

        # Проверка наличия такого сервиса в схеме АПИ
        service_callback = data['service_callback']
        try:
            config_service = self.schema[service_callback]['AMQP']['config']
            exchange_name = config_service['exchange']
            exchange = await channel.get_exchange(exchange_name)
        except:
            return
        # Проверка доступности метода
        try:
            check_rls(service_from_schema=self.schema[service_callback], service_to_name=self.service_name,
                      service_from_name=service_callback, method_name=data['method'])
        except (ServiceMethodNotAllowed, AllServiceMethodsNotAllowed):
            error_message = {'error': f"Метод {data['method']} не доступен из сервиса {service_callback}"}
            # ch.basic_publish(exchange=config_service['exchange'],
            #                  routing_key=get_route_key(config_service['quenue']),
            #                  body=create_callback_message_amqp(message=error_message,
            #                                                    result=False, response_id=data['id']))
            # ch.basic_ack(delivery_tag=method_request.delivery_tag)
            body_message = create_callback_message_amqp(error_message, False, data['id'],
                                                        service_name=self.service_name).encode('utf-8')
            out_message = aio_pika.Message(body_message)
            await exchange.publish(
                out_message,
                routing_key=config_service['quenue']
            )
            return

        # Вызов функции для обработки метода
        callback_message = None
        try:
            params_method, response_id, service_callback, method, method_callback = check_params_amqp(
                self.schema[self.service_name],
                data
            )
            callback_message = self.methods_service[data['method']](*check_params_amqp(
                self.schema[self.service_name],
                data))
        except KeyError as e:
            error_message = {'error': f"Метод {data['method']} не поддерживается"}
            body_message = create_callback_message_amqp(error_message, False, data['id']).encode('utf-8')
            out_message = aio_pika.Message(body_message)
            await exchange.publish(
                out_message,
                routing_key=config_service['quenue']
            )
            return
        except Exception as e:
            error_message = {'error': f"Ошибка {str(e)}"}
            body_message = create_callback_message_amqp(error_message, False, data['id']).encode('utf-8')
            out_message = aio_pika.Message(body_message)
            await exchange.publish(
                out_message,
                routing_key=config_service['quenue']
            )
            return

        if callback_message is None:
            return
        try:
            json_callback = create_callback_message_amqp(callback_message[0], callback_message[1],
                                                         response_id,
                                                         service_callback,
                                                         method_callback)
        except TypeError:
            raise TypeError(
                'Функция-обработчик должна возвращать tuple(сообщение, результат сообщения) или None')

        out_message = aio_pika.Message(json_callback.encode('utf-8'))
        await exchange.publish(
            out_message,
            routing_key=get_route_key(config_service['quenue'])
        )

    async def process_callback_message(self, message, channel):
        callback_message = CallbackMessage.from_dict(message.body)
        # self.me callback_message.method

