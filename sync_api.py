import json
import uuid
from typing import Union, List

import pika
import requests
from requests.auth import HTTPBasicAuth

from custom_exceptions import (ServiceNotFound)
from utils.utils import find_method, check_params, check_method_available


class NotFoundParams(Exception):
    pass


class ApiSync:
    """ Синхронный класс для работы с апи и другими сервисами """
    url = 'http://192.168.0.42/getApiStruct'

    def __init__(self, service_name: str):
        r"""
        Args:
            service_name: Название текущего сервиса
        """
        self.schema = None
        self.service_name = service_name
        self.get_schema_sync()

    def get_schema_sync(self) -> dict:
        self.schema = json.loads(requests.post(self.url, data={'format': 'json'}).text)
        return self.schema

    def send_request_api(self, method_name: str,
                         params: Union[dict, List[dict]], requested_service: str, wait_answer: bool):
        r"""
        Проверка доступности метода

        Args:
           method_name: имя метод апи.
           params: Схема апи текущего сервиса.
           requested_service: Имя сервиса - адресата.
           wait_answer: дожидаться ответа[Не используется]
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
        method = find_method(method_name, self.schema[requested_service])
        check_params(method, params)
        check_method_available(method, self.schema[self.service_name], requested_service)

        if method['TypeConnection'] == 'HTTP':
            return self.make_request_api_http(method, params)

        if method['TypeConnection'] == 'AMQP':
            return self.make_request_api_amqp(method, params)

    @staticmethod
    def make_request_api_http(method: dict, params: Union[List[dict], dict]) -> Union[str, list]:
        r"""
        Запрос на определенный метод сервиса через http.

        Args:
            method (dict): Метод отправки.
            params (List[dict], dict): Параметры, может быть набором параметров.

        Returns:
            str, list: Ответ сообщений (или сообщения, в зависимости от params)
        """

        def make_single_request(param):
            config = method['Config']
            url = f"http://{config['address']}:{config['port']}{config['endpoint']}{method['MethodName']}"
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
        Запрос на определенный метод сервиса через кролика.

        Args:
            method (dict): Метод отправки.
            params (List[dict], dict): Параметры, может быть набором параметров.

        Returns:
            str, list: Айдишники сообщений (или сообщения, в зависимости от params)
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
