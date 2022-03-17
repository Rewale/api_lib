import copy
import uuid
from typing import Union, List

import pika
import requests
from requests.auth import HTTPBasicAuth

from utils.custom_exceptions import (ServiceNotFound)
from utils.rabbit_utils import *
from utils.validation_utils import find_method, check_params, check_method_available


class NotFoundParams(Exception):
    pass


class ApiSync:
    """ Синхронный класс для работы с апи и другими сервисами """

    def __init__(self, service_name: str, url='http://apidev.mezex.lan/getApiStructProgr'):
        r"""
        Args:
            service_name: Название текущего сервиса
        """
        self.port = None
        self.pass_amqp = None
        self.queue = None
        self.exchange = None
        self.address_amqp = None
        self.user_amqp = None
        self.credentials = None
        self.credentials: pika.PlainCredentials
        self.url = url
        self.schema = None
        self.service_name = service_name
        self.get_schema_sync()

    def get_schema_sync(self) -> dict:
        self.schema = json.loads(requests.post(self.url, data={'format': 'json'}).text)

        self.queue = get_queue_service(self.service_name, self.schema)
        self.exchange = get_exchange_service(self.service_name, self.schema)
        self.address_amqp = get_amqp_address_service(self.service_name, self.schema)
        self.user_amqp = get_amqp_username_service(self.service_name, self.schema)
        self.pass_amqp = get_amqp_password_service(self.service_name, self.schema)
        self.port = get_port_amqp_service(self.service_name, self.schema)
        self.credentials = pika.PlainCredentials(self.user_amqp, self.pass_amqp)

        return self.schema

    def listen_queue(self, **kwargs):
        """
            БЛОКИРУЮЩАЯ ФУНКЦИЯ
            Начать слушать очередь сообщений сервиса
            methodName=Callable
        """

        def on_request(ch, method, props, body):
            queue_callback = None
            # Подтверждаем получение
            ch.basic_ack(delivery_tag=method.delivery_tag)
            try:
                data = json.loads(body.decode('utf-8'))
            except Exception as e:
                return

            params_method = self.check_input_params_amqp(data)

            service_callback = data['service_callback']
            config_service = self.schema[service_callback]['AMQP']['config']
            # Вызов функции для обработки метода
            callback_message = kwargs[data['method']](params_method)

            ch.basic_publish(exchange=self.exchange,
                             routing_key=get_route_key(config_service['quenue']),
                             body=callback_message)

        connection = self._open_amqp_connection_current_service()
        channel = connection.channel()

        channel.basic_consume(on_message_callback=on_request, queue=self.queue)
        channel.start_consuming()

    def check_input_params_amqp(self, params: dict):
        """ Проверка входящих параметров """
        # Проверка наличия id, response_id, method, method_callback
        assert 'id' in params.keys() \
               and 'service_callback' in params.keys() \
               and 'method' in params.keys() \
               and 'method_callback'

        method = get_method_service(self.service_name,
                                    params['method'],
                                    self.schema)
        params_method = copy.copy(params)
        del params_method['id']
        del params_method['service_callback']
        del params_method['method']
        del params_method['method_callback']

        check_params(method, params_method)
        return params_method

    def send_request_api(self, method_name: str,
                         params: Union[dict, List[dict]], requested_service: str):
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

    def _open_amqp_connection_current_service(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=self.address_amqp,
            credentials=self.credentials,
            port=self.port))

        return connection

    def make_request_api_amqp(self, method, params: Union[List[dict], dict]) -> Union[str, List[str]]:
        r"""
        Запрос на определенный метод сервиса через кролика.

        Args:
            method (dict): Метод отправки.
            params (List[dict], dict): Параметры, может быть набором параметров.

        Returns:
            str, list: Айдишники сообщений (или сообщения, в зависимости от params)
        """
        connection = self._open_amqp_connection_current_service()
        channel = connection.channel()
        channel.queue_declare(queue=method['config']['quenue'])
        channel.exchange_declare(exchange=method['config']['exchange'])

        def send_message(_param, correlation_id):
            _param['id'] = str(correlation_id)
            _param['service_callback'] = self.service_name
            _param['method'] = method['MethodName']
            channel.basic_publish(exchange=method['config']['exchange'],
                                  routing_key=get_route_key(method['config']['quenue']),
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
