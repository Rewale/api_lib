import asyncio
import json
import uuid
from typing import Union, List

import aio_pika
import aiohttp
import aioredis
from aioredis import Redis

from custom_exceptions import (ServiceNotFound)
from sync_api import ApiSync


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

    def send_request_api(self, method_name: str,
                         params: Union[dict, List[dict]], requested_service: str):
        r"""
        Проверка доступности метода

        Args:
           method_name: имя метод апи.
           params: Схема апи текущего сервиса.
           requested_service: Имя сервиса - адресата.
        Raises:
           ServiceMethodNotAllowed - Метод сервиса не доступен из текущего метода.
           AssertionError - тип параметра не соответствует типу в методе.
           RequireParamNotSet - не указан обязательный параметр.
           ParamNotFound - параметр не найден
        Returns:
           Ответ сообщений (или сообщения, в зависимости от params) - при HTTP или айдишники сообщений (или сообщения, в зависимости от params) - при AMQP
        """
        if requested_service not in self.schema:
            raise ServiceNotFound
        method = ApiSync.find_method(method_name, self.schema[requested_service])
        ApiSync.check_params(method, params)
        ApiSync.check_method_available(method, self.schema[requested_service], self.service_name)

        if method['TypeConnection'] == 'HTTP':
            return self.make_request_api_http(method, params)

        if method['TypeConnection'] == 'AMQP':
            return self.make_request_api_amqp(method, params)

    async def get_schema(self) -> dict:
        """ Асинхронное получение схемы """
        async with aiohttp.ClientSession() as session:
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
            if config['type'] == 'POST':
                async with session.post(url, data=param) as resp:
                    return await resp.text()
            elif config['type'] == 'GET':
                async with session.get(url, params=param) as resp:
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
            str, list: Айдишники сообщений (или сообщения, в зависимости от params)
        """
        if not use_open_connection or self.connection.is_closed:
            await self.make_connection(method)

        exchange = await self.channel.declare_exchange(name=method['config']['exchange'])
        queue = await self.channel.declare_queue(name=method['config']['quenue'])
        # Биндим очередь
        await queue.bind(exchange)

        async def send_message(param):
            param['id'] = str(uuid.uuid4())
            param['service_callback'] = self.service_name
            param['method'] = method['MethodName']
            json_param = json.dumps(param).encode()
            await exchange.publish(aio_pika.Message(body=json_param),
                                   routing_key=method['config']['quenue'])
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
            self.amqp_url_from_method(method),
            loop=asyncio.get_event_loop()
        )
        ch = await conn.channel()
        self.connection, self.channel = conn, ch

    async def close_connection(self):
        await self.channel.close()
        await self.channel.close()

    def redis_connection(self, redis_url: str):
        self.redis = aioredis.from_url(redis_url)

    async def read_redis(self, uuid_correlation) -> dict:
        """ Читаем из редиса пока результат равен null"""
        self.redis: Redis
        if self.redis is None:
            self.redis_connection(self.redis_url)

        res = await self.redis.get(uuid_correlation)
        while res is None:
            res = await self.redis.get(uuid_correlation)

        return json.loads(res)

    @staticmethod
    def amqp_url_from_method(method: dict):
        login = method['config']['username']
        password = method['config']['password']
        address = method['config']['address']
        port = 5672 if method['config']['port'] == '' else method['config']['port']
        vhost = method['config']['virtualhost']
        amqp = f'amqp://{login}:{password}@{address}:{port}/{vhost}'
        return amqp