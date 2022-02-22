import asyncio
from typing import Union, List

import aiohttp
import json
import requests
from requests.auth import HTTPBasicAuth

from custom_exceptions import (MethodNotFound,
                               ServiceNotFound,
                               ServiceMethodNotAllowed,
                               RequireParamNotSet)


class ApiSync:
    url = 'http://192.168.0.42/getApiStructProgr'

    def __init__(self, service_name: str):
        r"""
        :param service_name: Название текущего сервиса
        """
        self.schema = None
        self.service_name = service_name
        self.get_schema_sync()

    def get_schema_sync(self) -> dict:
        self.schema = json.loads(requests.post(self.url, data={'format': 'json'}).text)
        return self.schema

    async def get_schema(self) -> dict:
        """ Асинхронное получение схемы """
        async with aiohttp.ClientSession() as session:
            async with session.post(self.url, data={'format': 'json'}) as response:
                text_json = await response.text()
        content_json = json.loads(text_json)
        self.schema = content_json
        return content_json

    def send_request_api(self, method_name: str,
                         params: Union[dict, List[dict]], requested_service: str, wait_answer: bool):
        if requested_service not in self.schema:
            raise ServiceNotFound
        method = self.find_method(method_name, self.schema[requested_service])
        self.check_params(method, params)
        self.check_method_available(method, self.schema[requested_service])

        if method['TypeConnection'] == 'HTTP':
            return self.make_request_api_http(method, params)

        if method['TypeConnection'] == 'AMQP':
            return self.make_request_api_amqp(method, params)

    def check_method_available(self, method, service_schema):
        """ Проверка доступности метода """
        if 'RLS' not in service_schema:
            return True

        if self.service_name not in service_schema['RLS']:
            return True

        if 'allowed' in service_schema['RLS'][self.service_name] \
                and len(service_schema['RLS'][self.service_name]['RLS']['allowed']):
            if method not in service_schema['RLS'][self.service_name]:
                raise ServiceMethodNotAllowed
        elif 'disallowed' in service_schema['RLS'][self.service_name] \
                and service_schema['RLS'][self.service_name]['disallowed']:
            if method in service_schema['RLS'][self.service_name]['disallowed']:
                raise ServiceMethodNotAllowed
        else:
            raise ServiceMethodNotAllowed

        return True

    def check_params(self, method_params: dict, params: Union[dict, list]):
        r"""
        Валидация параметров.
        :param method_params Параметры метода api
        :param params Передаваемые параметры в api
        :raise AssertionError
        :raise RequireParamNotSet
        """


        def check_param(param: dict):
            for method_param, requirements in method_params['Params'].items():
                value_type, size, is_req = requirements
                if method_param not in param and is_req:
                    raise RequireParamNotSet
                value = param[method_param]
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

        if isinstance(params, dict):
            params = [params]

        for param in params:
            check_param(param)

        return True

    @staticmethod
    def make_request_api_http(method: dict, params: Union[List[dict], dict]) -> Union[str, list]:
        r"""
            Запрос на определенный метод сервиса
        :arg method Метод отправки
        :arg params Параметры, может быть набором параметров
        """
        def make_single_request(param):
            config = method['Config']
            url = f"http://{config['HTTPaddress']}:{config['HTTPport']}{config['HTTPconnstring']}"
            if config['HTTPauth']:
                auth = HTTPBasicAuth(config['HTTPusername'],
                                     config['HTTPpassword'])
            else:
                auth = None

            if config['HTTPtype'] == 'POST':
                return requests.post(url, data=param, auth=auth).text
            elif config['HTTPtype'] == 'GET':
                return requests.get(url, data=param, auth=auth).text

        if isinstance(params, dict):
            return make_single_request(params)
        if isinstance(params, list):
            response = list()
            for param in params:
                response.append(make_single_request(param))
            return response

        raise ValueError

    def make_request_api_amqp(self, method, params):
        pass

    def find_method(self, method_name, service_schema):
        """ Поиск метода в схеме, возвращает метод с типом подключения """
        for key, value in service_schema.items():
            if 'methods' not in value:
                continue
            if 'write' in value['methods']:
                for method_key, method_value in value['methods']['write'].items():
                    if method_key == method_name:
                        method = {'Params': method_value.copy(), 'TypeConnection': key, 'TypeMethod': 'read',
                                  'Config': value['config']}
                        return method
            if 'read' in value['methods']:
                for method_key, method_value in value['methods']['read'].items():
                    if method_key == method_name:
                        method = {'Params': method_value.copy(), 'TypeConnection': key, 'TypeMethod': 'write',
                                  'Config': value['config']}
                        return method

        raise MethodNotFound
