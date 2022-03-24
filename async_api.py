import asyncio
import datetime
import json
import time
import uuid
from typing import Union, List, Callable

import aio_pika
import aiohttp
import aioredis
from aioredis import Redis

import redis_read_worker
from utils.custom_exceptions import (ServiceNotFound)
from sync_api import ApiSync
from utils.rabbit_utils import get_route_key, service_amqp_url
from utils.validation_utils import check_method_available, find_method


# TODO: вынести проверки
# TODO: отправка сообщения через http и кролика с проверками
# TODO: обработка колбеков как в 1с (редис=регистр)
# TODO: разобраться с колбеком о непройденной валидации данных!
# TODO: вынести все проверки в общий модуль
class ApiAsync(object):
    url = ApiSync.url
    connection: aio_pika.Connection = None
    channel: aio_pika.Channel = None

    @classmethod
    async def create_api_async(cls, service_name: str, redis_url: str = "redis://localhost"):
        r"""
         Создание экземпляра класса

         Args:
              service_name(str): Название текущего сервиса
              redis_url(str): Строка подключения для чтения редис
         """
        self = ApiAsync(service_name, redis_url)
        await self.get_schema()
        return self

    def __init__(self, service_name: str, redis_url: str):
        """ Низя. Создание объекта через create_api_async """
        self.redis_url = redis_url
        self.redis = None
        self.redis: Redis
        self.service_name = service_name
        self.schema = None

    async def send_request_api(self, method_name: str,
                               params: Union[dict, List[dict]], requested_service: str, is_rpc=False, timeout=3):
        r"""
        Отправка запроса на сервис

        Args:
           method_name: имя метод апи.
           params: Схема апи текущего сервиса.
           requested_service: Имя сервиса - адресата.
           is_rpc: Ожидать ответа от сервиса при запросе AMQP (Удаленный вызов процедуры).
           timeout: Время ожидания запроса сервиса при is_rpc=True
        Raises:
           ServiceMethodNotAllowed - Метод сервиса не доступен из текущего метода.
           AssertionError - тип параметра не соответствует типу в методе.
           RequireParamNotSet - не указан обязательный параметр.
           ParamNotFound - параметр не найден
           TimeoutError - истекло время ожидания ответа от стороннего сервиса
        Returns:
           Ответ сообщений (или сообщения, в зависимости от params) - при HTTP или is_rpc=True,
           id сообщений (или сообщения, в зависимости от params) - при AMQP,
        """
        if requested_service not in self.schema:
            raise ServiceNotFound
        # TODO: перенести проверки в методы вызовов
        method = find_method(method_name, self.schema[requested_service])
        if method.type_conn != 'AMQP' and is_rpc:
            raise Exception('Только AMQP может иметь параметры is_callback и rpc')
        check_params(method, params)
        check_method_available(method, self.schema[requested_service], self.service_name)
        if method.type_conn == 'HTTP':
            return await self.make_request_api_http(method, params)
        if method.type_conn == 'AMQP':
            if is_rpc:
                return await self.rpc_amqp(method, params, timeout=3)
            else:
                return await self.make_request_api_amqp(method, params)

    def send_callback(self, current_service_method, ):
        # TODO: коллбек на сервис, проверка метода ТЕКУЩЕГО СЕРВИСА
        pass

    async def listen_queue(self, on_request: Callable):
        """ Начало опроса очереди текущего сервиса """
        pass

    async def get_schema(self) -> dict:
        """ Асинхронное получение схемы """
        async with aiohttp.ClientSession(aiohttp.BasicAuth(self.use)) as session:
            async with session.post(self.url, data={'format': 'json'}) as response:
                text_json = await response.text()
        content_json = json.loads(text_json)
        self.schema = content_json
        return content_json

    @staticmethod
    async def make_request_api_http(method: dict, params: Union[List[dict], dict]) -> Union[str, list]:
        r"""
        Запрос на определенный метод сервиса через кролика.

        Args:
            method (dict): Метод отправки.
            params (List[dict], dict): Параметры, может быть набором параметров.
        Returns:
            str, list: Ответы сообщений (или сообщения, в зависимости от params)
        """

        async def make_single_request(param):
            # TODO: Подготовка binary в base64, отправка параметров(проверить на пхп скрипте), заголовки
            if config['type'] == 'POST':
                async with session.post(url, data=param) as resp:
                    return await resp.text()
            elif config['type'] == 'GET':
                async with session.get(url, data=param) as resp:
                    return await resp.text()

        config = method['Config']
        url = f"http://{config['address']}:{config['port']}{config['connstring']}{method['MethodName']}"
        if config['auth']:
            auth = aiohttp.BasicAuth(config['username'],
                                     config['password'])
        else:
            auth = None
        async with aiohttp.ClientSession(auth=auth) as session:
            if isinstance(params, dict):
                return await make_single_request(params)
            if isinstance(params, list):
                response = list()
                for param in params:
                    response.append(await make_single_request(param))
                return response

        raise RuntimeError

    async def make_request_api_amqp(self,
                                    method, params: Union[List[dict], dict],
                                    close_connection: bool = True,
                                    use_open_connection: bool = False) -> Union[str, list]:
        r"""
        Запрос на определенный метод сервиса через кролика.

        Args:
            method (dict): Метод отправки.
            params (List[dict], dict): Параметры, может быть набором параметров.
            close_connection (bool): Закрыть соединение после отправки сообщений
            use_open_connection (bool): Использовать открытое соединение


        Returns:
            str, list: id сообщений (или сообщения, в зависимости от params)
        """
        if not use_open_connection or self.connection.is_closed:
            await self.make_connection(method)

        exchange = await self.channel.declare_exchange(name=method['config']['exchange'])

        async def send_message(param):
            param['id'] = str(uuid.uuid4())
            param['service_callback'] = self.service_name
            param['method'] = method['MethodName']
            json_param = json.dumps(param).encode()
            await exchange.publish(aio_pika.Message(body=json_param),
                                   routing_key=get_route_key(method['config']['quenue']))
            return param['id']

        if isinstance(params, dict):
            result = await asyncio.gather(send_message(params))
            return result[0]
        elif isinstance(params, list):
            return await asyncio.gather(*[send_message(param) for param in params])

        if close_connection:
            await self.close_connection()

    async def make_connection(self, method):
        """ Создаем подключение и открываем канал """

        conn = await aio_pika.connect_robust(
            service_amqp_url(self.schema[self.service_name]),
            loop=asyncio.get_event_loop()
        )
        ch = await conn.channel()
        self.connection, self.channel = conn, ch

    async def close_connection(self):
        await self.channel.close()
        await self.channel.close()

    def redis_connection(self, redis_url: str):
        self.redis = aioredis.from_url(redis_url)

    async def rpc_amqp(self, method, params, callback_queue=None, timeout=3):
        """
        Удаленный вызов процедуры.
        Запрос по AMQP и ожидание ответа
        от стороннего сервиса в течении определенного количества времени
        """
        if callback_queue is None:
            callback_queue = self.schema[self.service_name]['AMQP']['config']['quenue']
        date = str(datetime.datetime.now())
        params['date'] = date
        task_read_rabbit = asyncio.create_task(
            redis_read_worker.start_listening(callback_queue,
                                              service_amqp_url(method)))
        # Запрос на сервис
        message_uuid = await self.make_request_api_amqp(method=method, params=params)
        try:
            return await self.read_redis(message_uuid, timeout)
        except Exception as e:
            raise e
        finally:
            task_read_rabbit.cancel()

    async def read_redis(self, uuid_correlation, timeout=3) -> dict:
        """ Читаем из редиса пока результат равен null с определенным таймаутом """
        start_time = time.monotonic()
        if self.redis is None:
            self.redis_connection(self.redis_url)

        res = await self.redis.get(uuid_correlation)
        while res is None:
            res = await self.redis.get(uuid_correlation)
            if (time.monotonic() - start_time) >= timeout:
                raise TimeoutError
        return json.loads(res)
