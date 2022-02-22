import asyncio

import aiohttp
import json

import requests


class ServiceNotFound(Exception):
    pass


class ParamNotFound(Exception):
    pass


class RequireParamNotSet(Exception):
    pass


class ServiceMethodNotAllowed(Exception):
    pass


class MethodNotFound(Exception):
    pass


def asyncinit(cls):
    __new__ = cls.__new__

    async def init(obj, *arg, **kwarg):
        await obj.__init__(*arg, **kwarg)
        return obj

    def new(cls, *arg, **kwarg):
        obj = __new__(cls, *arg, **kwarg)
        coro = init(obj, *arg, **kwarg)
        return coro

    cls.__new__ = new
    return cls


@asyncinit
class ApiSync:
    url = 'http://192.168.0.42/getApiStructProgr'

    def __init__(self, service_name: str, is_sync_get_schema=False):
        """
            :arg service_name  Название текущего сервиса
            :arg is_sync_get_schema  Загрузить схему api синхронно (get_schema_sync)
            , для загрузки асинхронно - get_schema
        """
        self.service_schema = None
        self.schema = None
        self.service_name = service_name
        if is_sync_get_schema:
            self.get_schema_sync()

    def get_schema_sync(self) -> dict:
        self.schema = json.loads(requests.post(self.url, data={'format': 'json'}).text)
        if self.service_name not in self.schema:
            raise ServiceNotFound

        self.service_schema = self.schema[self.service_name]
        return self.schema

    async def get_schema(self) -> dict:
        """ Асинхронное получение схемы """
        async with aiohttp.ClientSession() as session:
            async with session.post(self.url, data={'format': 'json'}) as response:
                text_json = await response.text()
        content_json = json.loads(text_json)
        self.schema = content_json
        if self.service_name not in self.schema:
            raise ServiceNotFound

        self.service_schema = self.schema[self.service_name]
        return content_json

    def send_request_api(self, method_name: str,
                         params: dict, requested_service: str, wait_answer: bool):
        method = self.find_method(method_name)
        self.check_params(method, params)
        self.check_method_available(method, requested_service)

        if method['TypeConnection'] == 'HTTP':
            pass

        if method['TypeConnection'] == 'AMQP':
            pass

    def check_method_available(self, method, requested_service):
        """ Проверка доступности метода """
        if 'RLS' not in self.service_schema:
            return True

        if requested_service not in self.service_schema['RLS']:
            return True

        if 'allowed' in self.service_schema['RLS'][requested_service] \
                and len(self.service_schema['RLS'][requested_service]['RLS']['allowed']):
            if method not in self.service_schema['RLS'][requested_service]:
                raise ServiceMethodNotAllowed
        elif 'disallowed' in self.service_schema['RLS'][requested_service] \
                and self.service_schema['RLS'][requested_service]['disallowed']:
            if method in self.service_schema['RLS'][requested_service]['disallowed']:
                raise ServiceMethodNotAllowed
        else:
            raise ServiceMethodNotAllowed

        return True

    def check_params(self, method_params: dict, params: dict):
        """ Проверка доступности метода """
        for method_param, requirements in method_params.items():
            if method_param == 'TypeConnection':
                continue
            value_type, size, is_req = requirements
            if method_param not in params and is_req:
                raise RequireParamNotSet
            value = params[method_param]
            if value_type == 'str':
                assert isinstance(value, str)
                if size is not None:
                    assert len(value) <= size
            elif value_type == 'int':
                assert isinstance(value, int)
                if size is not None:
                    assert value <= 10 ** size
            elif value_type == 'guid':
                pass
            elif value_type == 'md5':
                pass
            elif value_type == 'json':
                json.loads(value)

        return True

    def make_request_api_http(self, method: dict):
        pass

    def make_request_api_amqp(self):
        pass

    def find_method(self, method_name):
        """ Поиск метода в схеме, возвращает метод с типом подключения """
        for key, value in self.service_schema.items():
            if 'methods' not in value:
                continue
            if 'write' in value['methods']:
                for method_key, method_value in value['methods']['write'].items():
                    if method_key == method_name:
                        method_value['TypeConnection'] = key
                        method_value['TypeMethod'] = 'write'
                        return method_value
            if 'read' in value['methods']:
                for method_key, method_value in value['methods']['read'].items():
                    if method_key == method_name:
                        method_value['TypeConnection'] = key
                        method_value['TypeMethod'] = 'read'
                        return method_value

        raise MethodNotFound
