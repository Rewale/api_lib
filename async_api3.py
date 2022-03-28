import asyncio
import json
import traceback
from typing import List, Optional

import aio_pika
import aiohttp
import aioredis
import loguru
from aio_pika import Message

from utils.messages import create_callback_message_amqp, CallbackMessage
from utils.rabbit_utils import *
from utils.validation_utils import MethodApi, InputParam, check_rls, \
    find_method


class ApiAsync(object):
    @classmethod
    async def create_api_async(cls, user_api, pass_api, service_name: str,
                               methods: dict = None,
                               methods_callback: dict = None,
                               url='http://apidev.mezex.lan/getApiStructProgr',
                               redis_url: str = 'redis://127.0.0.1:6379',
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
                 methods_callback=None):
        r"""
        НИЗЯ!
        Args:
            user_api: логин для получения схемы
            pass_api: пароль для получения схемы
            url: адрес для схемы
            service_name: Название текущего сервиса
            methods_callback: Словарь обработчиков колбеков {название метода: функция}
            methods: Словарь обработчиков для каждого метода сервиса {название метода: функция}
        функция(params: dict,
        response_id: str,
        service_callback: str,
        method: str,
        method_callback: str)
        -> (сообщение: dict, результат: bool):
        """
        if methods_callback is None:
            methods_callback = {}
        self.methods_callback = methods_callback
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

        # Redis
        self.redis: aioredis.Redis
        self.redis_connection(redis_url)

        # logger
        self.logger = loguru.logger

    def redis_connection(self, redis_url: str):
        self.redis = aioredis.from_url(redis_url)

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
            async for message in queue_iter:
                message: aio_pika.IncomingMessage
                async with message.process():
                    # Проверка серилизации
                    try:
                        data = json.loads(message.body.decode('utf-8'))
                        exchange_name_callback = get_exchange_service(data['service_callback'], self.schema)
                        queue_name_callback = get_queue_service(data['service_callback'], self.schema)
                        self.logger.info(f"[SER] Серилизован {data=}")
                    except Exception as e:
                        self.logger.info(f"[SER] Ошибка серилизации {message.body.decode('utf-8')}")
                        continue
                    if 'response_id' in data:
                        try:
                            self.logger.info("[Callback] Начало обработки коллбека")
                            body = await self.process_callback_message(data)
                            if body is None:
                                self.logger.info(f"[Callback] Сообщение отработано без ответа!")
                                continue
                            out_message = Message(body.encode('utf-8'))
                            self.logger.info(f"[Callback] Конец обработки колбека{body=}")
                        except KeyError as e:
                            self.logger.error(f"[Callback] не найден метод {e}")
                            continue
                        except TypeError as e:
                            self.logger.error(f"[Callback] не указаны доступные методы для обработки колбека")
                            continue
                        except Exception as e:
                            self.logger.error(e)
                            continue
                    else:
                        try:
                            self.logger.info("[Message] Начало обработки сообщения")
                            out = await self.process_incoming_message(data)
                            out_message = Message(out.encode('utf-8'))
                            self.logger.info(f"[Message] Конец обработки сообщения{out=}")
                        except Exception as e:
                            self.logger.info(f"[Message] {e}")
                            continue
                    exchange_callback = await channel.get_exchange(exchange_name_callback)
                    await exchange_callback.publish(out_message, routing_key=get_route_key(queue_name_callback))

                    self.logger.info(f"Сообщение отправлено в очередь {queue_name_callback}")

    @staticmethod
    async def api_http_request(method: MethodApi, params: List[InputParam]):
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
        json_message = message.json()
        await exchange.publish(message=aio_pika.Message(json_message.encode('utf-8')),
                               routing_key=get_route_key(method.config.quenue))
        if callback_method_name and callback_method_name in self.methods_callback:
            self.redis: aioredis.Redis
            await self.redis.set(message.id, message.json())
        await connection.close()
        return message.id

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
                               params: List[InputParam],
                               requested_service: str,
                               callback_method_name: str = None) -> str:
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
            :param requested_service:
            :param params:
            :param method_name:
            :param callback_method_name:
        """

        if requested_service not in self.schema:
            raise ServiceNotFound
        method = find_method(method_name, self.schema[requested_service])
        check_rls(self.schema[self.service_name], requested_service, self.service_name, method_name)

        if method.type_conn == 'HTTP':
            return await self.api_http_request(method, params)

        if method.type_conn == 'AMQP':
            return await self.api_amqp_request(method, params, callback_method_name)

    async def process_incoming_message(self, data: dict) -> Optional[str]:
        # Проверка наличия такого сервиса в схеме АПИ
        service_callback = data['service_callback']
        try:
            config_service = self.schema[service_callback]['AMQP']['config']
        except:
            return
        # Проверка доступности метода
        try:
            check_rls(service_from_schema=self.schema[service_callback], service_to_name=self.service_name,
                      service_from_name=service_callback, method_name=data['method'])
        except (ServiceMethodNotAllowed, AllServiceMethodsNotAllowed):
            error_message = {'error': f"Метод {data['method']} не доступен из сервиса {service_callback}"}
            body_message = create_callback_message_amqp(error_message, False, data['id'],
                                                        service_name=self.service_name)
            return body_message

        # Вызов функции для обработки метода
        try:
            callback_message = await self.methods_service[data['method']](check_params_amqp(
                self.schema[self.service_name],
                data))
        except KeyError as e:
            error_message = {'error': f"Метод {data['method']} не поддерживается"}
            body_message = create_callback_message_amqp(error_message, False, data['id'])
            return body_message
        except Exception as e:
            self.logger.error(traceback.format_exc())
            error_message = {'error': f"Ошибка {str(e)}"}
            body_message = create_callback_message_amqp(error_message, False, data['id'])
            return body_message

        if callback_message is None:
            return

        json_callback = callback_message.json()

        return json_callback

    async def process_callback_message(self, message: dict):
        callback_message = CallbackMessage.from_dict(message)
        self.redis: aioredis.Redis
        redis_message = await self.redis.get(callback_message.response_id)
        if not redis_message:
            return
        message: IncomingMessage = IncomingMessage.from_dict(json.loads(redis_message.decode('utf-8')))
        if not message or message.method_callback not in self.methods_callback:
            return
        callback_message.incoming_message = message
        return await self.methods_callback[message.method_callback](callback_message)
