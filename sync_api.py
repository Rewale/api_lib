import copy
import json
import uuid
from typing import Union, List

import pika
import requests
from requests.auth import HTTPBasicAuth

from utils.custom_exceptions import (ServiceNotFound)
from utils.rabbit_utils import *
from utils.validation_utils import find_method, check_method_available, InputParam, MethodApi, ConfigAMQP, \
    check_rls, ConfigHTTP, create_callback_message_amqp, serialize_message, create_hash


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

        check_methods_handlers(self.schema[service_name], methods)

    def get_schema_sync(self) -> dict:
        if self.schema is None:
            self.schema = json.loads(requests.post(self.url, auth=HTTPBasicAuth(self.user_api, self.pass_api),
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

        def on_request(ch, method_request, props, body):
            queue_callback = None
            # Подтверждаем получение
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
            try:
                check_rls(service_from_schema=self.schema[service_callback], service_to_name=self.service_name,
                          service_from_name=service_callback, method_name=data['method'])
            except (ServiceMethodNotAllowed, AllServiceMethodsNotAllowed):
                error_message = {'error': f"Метод {data['method']} не доступен из сервиса {service_callback}"}
                ch.basic_publish(exchange=config_service['exchange'],
                                 routing_key=get_route_key(config_service['quenue']),
                                 body=create_callback_message_amqp(message=error_message,
                                                                   result=False, response_id=data['id']))

            # Вызов функции для обработки метода
            callback_message = None

            params_method, response_id, service_callback, method, method_callback = self.check_params_amqp(data)
# 158.46.250.192:8966
            try:
                callback_message = self.methods_service[data['method']](*self.check_params_amqp(data))
            except KeyError as e:
                error_message = {'error': f"Метод {data['method']} не поддерживается"}
                ch.basic_publish(exchange=config_service['exchange'],
                                 routing_key=get_route_key(config_service['quenue']),
                                 body=create_callback_message_amqp(message=error_message,
                                                                   result=False, response_id=data['id']))
            except Exception as e:
                error_message = {'error': f"Ошибка {str(e)}"}
                ch.basic_publish(exchange=config_service['exchange'],
                                 routing_key=get_route_key(config_service['quenue']),
                                 body=create_callback_message_amqp(message=error_message,
                                                                   result=False, response_id=data['id']))
            try:
                ch.basic_ack(delivery_tag=method_request.delivery_tag)
            except Exception as e:
                print(e)
            ch.basic_publish(exchange=config_service['exchange'],
                             routing_key=get_route_key(config_service['quenue']),
                             body=serialize_message(callback_message).encode('utf-8'))

        connection = self._open_amqp_connection_current_service()
        channel = connection.channel()

        channel.basic_consume(on_message_callback=on_request, queue=self.queue)
        channel.start_consuming()

    def check_params_amqp(self, params: dict):
        # TODO: вынести отдельно
        """ Проверка входящих/исходящих параметров """
        # Проверка наличия id, response_id, method, method_callback
        assert 'id' in params.keys() \
               and 'service_callback' in params.keys() \
               and 'method' in params.keys() \
               and 'method_callback' in params.keys()

        schema_service = self.schema[self.service_name]
        method = find_method(params['method'], schema_service)
        params_method = copy.copy(params)
        del params_method['id']
        del params_method['service_callback']
        del params_method['method']
        del params_method['method_callback']

        method.check_params(InputParam.from_dict(params_method))

        return params_method, params['id'], params['service_callback'], params['method'], params['method_callback']

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
        check_rls(self.schema[self.service_name], requested_service, self.service_name, method_name)
        # TODO: RLS
        # check_method_available(method, self.schema[self.service_name], requested_service)

        if method.type_conn == 'HTTP':
            return json.loads(self.make_request_api_http(method, params))

        if method.type_conn == 'AMQP':
            return self.make_request_api_amqp(method, params)

    @staticmethod
    def make_request_api_http(method: MethodApi, params: List[InputParam]) -> str:
        r"""
        Запрос на определенный метод сервиса через http.

        Args:
            method : Метод отправки.
            params : Параметры.

        Returns:
            str: Ответ сервиса.
        """
        url = method.get_url()
        message = method.get_message_http(params)
        # headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
        auth = None
        if method.config.auth:
            auth = (method.config.username, method.config.password)

        if method.config.type == 'POST':
            return requests.post(url, data=message, auth=auth).text
        if method.config.type == 'GET':
            return requests.get(url, data=message, auth=auth).text

    def _open_amqp_connection_current_service(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=self.address_amqp,
            credentials=self.credentials,
            port=self.port))

        return connection

    def make_request_api_amqp(self, method: MethodApi, params: List[InputParam], callback_method_name: str = ''):
        r"""
        Запрос на определенный метод сервиса через кролика.

        Args:
            method (dict): Метод отправки.
            params (List[dict], dict): Параметры, может быть набором параметров.
            callback_method_name: Имя метода - обработчика колбека.

        Returns:
            str: id отправленного сообщения
        """
        # TODO: добавить проверку РЛС
        connection = self._open_amqp_connection_current_service()
        channel = connection.channel()

        message = method.get_message_amqp(params, self.service_name, callback_method_name)

        channel.basic_publish(exchange=method.config.exchange,
                              routing_key=get_route_key(method.config.quenue),
                              body=serialize_message(message).encode('utf-8'))

        channel.close()
        connection.close()

        return message['id']
