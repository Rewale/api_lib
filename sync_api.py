import copy
import uuid
from typing import Union, List

import pika
import requests
from requests.auth import HTTPBasicAuth

from utils.custom_exceptions import (ServiceNotFound)
from utils.rabbit_utils import *
from utils.validation_utils import find_method, check_params, check_method_available, InputParam, MethodApi, ConfigAMQP


class NotFoundParams(Exception):
    pass


class ApiSync:
    """ Синхронный класс для работы с апи и другими сервисами """

    def __init__(self, service_name: str,
                 user_api,
                 pass_api,
                 methods: dict = None,
                 url='http://apidev.mezex.lan/getApiStructProgr',
                 schema: dict = None):
        r"""
        Args:
            user_api: логин для получения схемы
            pass_api: пароль для получения схемы
            url: адрес для схемы
            service_name: Название текущего сервиса
            methods: Словарь обработчиков для каждого метода сервиса
        """
        self.service_name = service_name

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
        self.credentials: pika.PlainCredentials

        # Для тестов можно загружать словарь
        self.schema = None
        if schema is not None:
            self.schema = schema
        self.get_schema_sync()

        check_methods_handlers(schema[service_name], methods)

    def get_schema_sync(self) -> dict:
        if self.schema is None:
            # TODO: get или post?
            self.schema = json.loads(requests.get(self.url, auth=HTTPBasicAuth(self.user_api, self.pass_api),
                                                  data={'format': 'json'}).text)

        self.queue = get_queue_service(self.service_name, self.schema)
        self.exchange = get_exchange_service(self.service_name, self.schema)
        self.address_amqp = get_amqp_address_service(self.service_name, self.schema)
        self.user_amqp = get_amqp_username_service(self.service_name, self.schema)
        self.pass_amqp = get_amqp_password_service(self.service_name, self.schema)
        self.port = get_port_amqp_service(self.service_name, self.schema)
        self.credentials = pika.PlainCredentials(self.user_amqp, self.pass_amqp)

        return self.schema

    def listen_queue(self):
        """
            БЛОКИРУЮЩАЯ ФУНКЦИЯ
            Начать слушать очередь сообщений сервиса
        """

        def on_request(ch, method, props, body):
            queue_callback = None
            # Подтверждаем получение
            ch.basic_ack(delivery_tag=method.delivery_tag)
            try:
                data = json.loads(body.decode('utf-8'))
            except Exception as e:
                return

            try:
                params_method = self.check_params_amqp(data)
            except AssertionError:
                return

            service_callback = data['service_callback']
            config_service = self.schema[service_callback]['AMQP']['config']
            # Вызов функции для обработки метода
            try:
                callback_message = self.methods_service[data['method']](params_method)
            except Exception as e:
                # TODO метод не поддерживается
                ch.basic_publish(exchange=self.exchange,
                                 routing_key=get_route_key(config_service['quenue']),
                                 body=callback_message)
                print(e)

            # TODO проверка после обработки

            ch.basic_publish(exchange=self.exchange,
                             routing_key=get_route_key(config_service['quenue']),
                             body=callback_message)

        connection = self._open_amqp_connection_current_service()
        channel = connection.channel()

        channel.basic_consume(on_message_callback=on_request, queue=self.queue)
        channel.start_consuming()

    def check_params_amqp(self, params: dict):
        """ Проверка входящих/исходящих параметров """
        # Проверка наличия id, response_id, method, method_callback
        assert 'id' in params.keys() \
               and 'service_callback' in params.keys() \
               and 'method' in params.keys() \
               and 'method_callback' in params.keys()

        try:
            schema_service = self.schema[params['service_callback']]
        except KeyError:
            raise ServiceNotFound

        method = find_method(params['method'], schema_service)
        params_method = copy.copy(params)
        del params_method['id']
        del params_method['service_callback']
        del params_method['method']
        del params_method['method_callback']

        method.check_params(InputParam.from_dict(params_method))

        return params_method

    def send_request_api(self, method_name: str,
                         params: Union[InputParam, List[InputParam]], requested_service: str):
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
        """

        if requested_service not in self.schema:
            raise ServiceNotFound
        method = find_method(method_name, self.schema[requested_service])
        method.check_params(params)
        # TODO: RLS
        # check_method_available(method, self.schema[self.service_name], requested_service)

        if method.type_conn == 'HTTP':
            return self.make_request_api_http(method, params)

        if method.type_conn == 'AMQP':
            return self.make_request_api_amqp(method, params)

    @staticmethod
    def make_request_api_http(method: dict, params: Union[List[InputParam], InputParam]) -> Union[str, list]:
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

    def make_request_api_amqp(self, method: MethodApi, params: List[InputParam]):
        r"""
        Запрос на определенный метод сервиса через кролика.

        Args:
            method (dict): Метод отправки.
            params (List[dict], dict): Параметры, может быть набором параметров.

        Returns:
            str, list: Айдишники сообщений (или сообщения, в зависимости от params)
        """
        method.config: ConfigAMQP
        method.check_params(params)
        connection = self._open_amqp_connection_current_service()
        channel = connection.channel()
        # channel.queue_declare(queue=method.config.quenue)
        # channel.exchange_declare(exchange=method.config.exchange)

        message: dict = {'service_callback': self.service_name, 'method': method.name, 'method_callback': ''}
        for i in params:
            message[i.name] = i.value

        hash_id = hashlib.md5(json.dumps(message).encode('utf-8'))
        message['id'] = str(hash_id)

        channel.basic_publish(exchange=method.config.exchange,
                              routing_key=get_route_key(method.config.quenue),
                              body=bytes(json.dumps(message), 'utf-8'))

        channel.close()
        connection.close()
