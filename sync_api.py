import json
import uuid

import pika
from typing import Union, List
import aiohttp
import requests
from requests.auth import HTTPBasicAuth

from custom_exceptions import (MethodNotFound,
                               ServiceNotFound,
                               ServiceMethodNotAllowed,
                               RequireParamNotSet, ParamNotFound)


class NotFoundParams(Exception):
    pass


class ApiSync:
    url = 'http://192.168.0.42/getApiStruct'

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

    def send_request_api(self, method_name: str,
                         params: Union[dict, List[dict]], requested_service: str, wait_answer: bool):
        if requested_service not in self.schema:
            raise ServiceNotFound
        method = self.find_method(method_name, self.schema[requested_service])
        self.check_params(method, params)
        self.check_method_available(method, self.schema[self.service_name], requested_service)

        if method['TypeConnection'] == 'HTTP':
            return self.make_request_api_http(method, params)

        if method['TypeConnection'] == 'AMQP':
            return self.make_request_api_amqp(method, params)

    @staticmethod
    def check_method_available(method, curr_service_schema, parent_service_name):
        """ Проверка доступности метода """

        if 'RLS' not in curr_service_schema:
            return True

        if parent_service_name not in curr_service_schema['RLS']:
            return ServiceMethodNotAllowed

        if 'allowed' not in curr_service_schema['RLS'] \
                and 'disallowed' not in curr_service_schema['RLS']:
            return True

        if 'allowed' in curr_service_schema['RLS'][parent_service_name] \
                and len(curr_service_schema['RLS'][parent_service_name]['RLS']['allowed']):
            if method not in curr_service_schema['RLS'][parent_service_name]:
                raise ServiceMethodNotAllowed
        elif 'disallowed' in curr_service_schema['RLS'][parent_service_name] \
                and curr_service_schema['RLS'][parent_service_name]['disallowed']:
            if method in curr_service_schema['RLS'][parent_service_name]['disallowed']:
                raise ServiceMethodNotAllowed
        else:
            raise ServiceMethodNotAllowed

        return True

    @staticmethod
    def check_params(method_params: dict, params: Union[dict, list]):
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
                elif value_type == 'bin':
                    pass
            if len(param) != method_params['Params']:
                raise ParamNotFound
        if isinstance(params, dict):
            params = [params]

        for param in params:
            check_param(param)

        return True

    @staticmethod
    def make_request_api_http(method: dict, params: Union[List[dict], dict]) -> Union[str, list]:
        r"""
            Запрос на определенный метод сервиса.
            :arg method Метод отправки.
            :arg params Параметры, может быть набором параметров.
        """

        def make_single_request(param):
            config = method['Config']
            url = f"http://{config['address']}:{config['port']}{config['connstring']}{method['MethodName']}"
            if config['auth']:
                auth = HTTPBasicAuth(config['username'],
                                     config['password'])
            else:
                auth = None

            if config['type'] == 'POST':
                return requests.post(url, data=param, auth=auth).text
            elif config['type'] == 'GET':
                return requests.get(url, params=param, auth=auth).text

        if isinstance(params, dict):
            return make_single_request(params)
        if isinstance(params, list):
            response = list()
            for param in params:
                response.append(make_single_request(param))
            return response

        raise ValueError

    def make_request_api_amqp(self, method, params: Union[List[dict], dict]) -> Union[str, List[str]]:
        r"""
            Отправка через кролика.
            Возвращает id сообщения или массив id сообщений
        """
        credentials = pika.PlainCredentials(method['config']['username'],
                                            method['config']['password'])
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=method['config']['address'], virtual_host='/',
            credentials=credentials,
            port=method['config']['port']))

        channel = connection.channel()
        channel.queue_declare(queue=method['config']['quenue'])
        channel.exchange_declare(exchange=method['config']['exchange'])
        # !!! нужно биндить очередь к обменнику
        channel.queue_bind(method['config']['quenue'], method['config']['exchange'])

        def send_message(_param, correlation_id):
            _param['id'] = str(correlation_id)
            _param['service_callback'] = self.service_name
            _param['method'] = method['MethodName']
            channel.basic_publish(exchange=method['config']['exchange'],
                                  routing_key=method['config']['quenue'],
                                  body=bytes(json.dumps(params), 'utf-8'))

        if isinstance(params, dict):
            id = str(uuid.uuid4())
            send_message(params, id)
            return id
        elif isinstance(params, list):
            ids = []
            for param in params:
                id = str(uuid.uuid4())
                send_message(param, str(uuid.uuid4()))
                ids.append(id)
            return ids
        channel.close()
        connection.close()

    @staticmethod
    def find_method(method_name, service_schema):
        """ Поиск метода в схеме, возвращает метод с типом подключения """
        for key, value in service_schema.items():
            if 'methods' not in value:
                continue
            if 'write' in value['methods']:
                for method_key, method_value in value['methods']['write'].items():
                    if method_key == method_name:
                        method = {'Params': method_value.copy(), 'TypeConnection': key, 'TypeMethod': 'read',
                                  'Config': value['config'], 'MethodName': method_name}
                        return method
            if 'read' in value['methods']:
                for method_key, method_value in value['methods']['read'].items():
                    if method_key == method_name:
                        method = {'Params': method_value.copy(), 'TypeConnection': key, 'TypeMethod': 'write',
                                  'Config': value['config'], 'MethodName': method_name}
                        return method

        raise MethodNotFound
