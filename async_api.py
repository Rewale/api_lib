import asyncio
import datetime
import json
import uuid
from typing import Union, List
import aio_pika
import aiohttp
from sync_api import ApiSync
from custom_exceptions import (MethodNotFound,
                               ServiceNotFound,
                               ServiceMethodNotAllowed,
                               RequireParamNotSet)


class ApiAsync(object):
    url = ApiSync.url
    connection: aio_pika.Connection = None
    channel: aio_pika.Channel = None

    @classmethod
    async def create_api_async(cls, service_name):
        self = ApiAsync(service_name)
        await self.get_schema()
        return self

    def __init__(self, service_name: str):
        self.service_name = service_name
        self.schema = None

    def send_request_api(self, method_name: str,
                         params: Union[dict, List[dict]], requested_service: str, wait_answer: bool):
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
            Запрос на определенный метод сервиса
            :arg method Метод отправки
            :arg params Параметры, может быть набором параметров
        """

        async def make_single_request(param):
            config = method['Config']
            url = f"http://{config['address']}:{config['port']}{config['connstring']}"
            if config['auth']:
                auth = aiohttp.BasicAuth(config['username'],
                                         config['password'])
            else:
                auth = None
            # TODO: создавать сессию единожды!
            if config['type'] == 'POST':
                async with aiohttp.ClientSession(auth=auth) as session:
                    async with session.post(url, data=param) as resp:
                        return await resp.text()
            elif config['type'] == 'GET':
                async with aiohttp.ClientSession(auth=auth) as session:
                    async with session.get(url, params=param) as resp:
                        return await resp.text()

        if isinstance(params, dict):
            return await make_single_request(params)
        if isinstance(params, list):
            response = list()
            for param in params:
                response.append(await make_single_request(param))
            return response

        raise ValueError

    async def make_request_api_amqp(self, method, params: Union[List[dict], dict]) -> None:
        await self.make_connection(method['config']['username'],
                                   method['config']['password'],
                                   method['config']['address'],
                                   method['config']['port'])
        # exchange = await self.channel.declare_exchange(method['config']['AMQPexchange'])
        exchange = await self.channel.declare_exchange(name=method['config']['exchange'])
        queue = await self.channel.declare_queue(name=method['config']['quenue'])
        await queue.bind(exchange)

        async def send_message(param):
            print('start send message ' + str(datetime.datetime.now()))
            param['id'] = str(uuid.uuid4())
            param['service_callback'] = self.service_name
            param['method'] = method['MethodName']
            json_param = json.dumps(param).encode()
            await exchange.publish(aio_pika.Message(body=json_param),
                                   routing_key=method['config']['quenue'])
            print('end message sending ' + str(datetime.datetime.now()))

        if isinstance(params, dict):
            await asyncio.gather(send_message(params))
        elif isinstance(params, list):
            await asyncio.gather(*[send_message(param) for param in params])

    async def make_connection(self, login, password, address, port=None, vhost=None):
        """ Создаем подключение и открываем канала """
        conn = await aio_pika.connect_robust(
            f"amqp://{login}:{password}@{address}/",
            loop=asyncio.get_event_loop()
        )
        ch = await conn.channel()
        self.connection, self.channel = conn, ch
