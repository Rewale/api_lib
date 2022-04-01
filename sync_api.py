import traceback
from typing import List, Optional

import loguru
import pika
import requests
from requests.auth import HTTPBasicAuth

from api_lib.utils.messages import create_callback_message_amqp
from api_lib.utils.rabbit_utils import *
from api_lib.utils.validation_utils import find_method, InputParam, MethodApi, check_rls


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
            methods: Словарь обработчиков для каждого метода сервиса {название метода: функция}
        функция(params: dict,
        response_id: str,
        service_callback: str,
        method: str,
        method_callback: str)
        -> (сообщение: dict, результат: bool):
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
        self.logger = loguru.logger

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
            БЛОКИРУЮЩАЯ ФУНКЦИЯ.
            Слушает очередь сообщений сервиса.
            Валидирует входящее и исходящее сообщение.
            Обработка методов в соответствии с обработчиками конструктора.
        """

        def on_request(ch, method_request, props, body):
            queue_callback = None
            # Проверка серилизации
            try:
                data = json.loads(body.decode('utf-8'))
            except Exception as e:
                ch.basic_ack(delivery_tag=method_request.delivery_tag)
                return
            # Проверка наличия такого сервиса в схеме АПИ
            service_callback = data['service_callback']
            try:
                config_service = self.schema[service_callback]['AMQP']['config']
            except:
                ch.basic_ack(delivery_tag=method_request.delivery_tag)
                return
            # Проверка доступности метода
            try:
                check_rls(service_from_schema=self.schema[service_callback], service_to_name=self.service_name,
                          service_from_name=service_callback, method_name=data['method'])
            except (ServiceMethodNotAllowed, AllServiceMethodsNotAllowed):
                error_message = {'error': f"Метод {data['method']} не доступен из сервиса {service_callback}"}
                ch.basic_publish(exchange=config_service['exchange'],
                                 routing_key=get_route_key(config_service['quenue']),
                                 body=create_callback_message_amqp(message=error_message,
                                                                   result=False, response_id=data['id'],
                                                                   method_name=data['method']))
                ch.basic_ack(delivery_tag=method_request.delivery_tag)
                return

            # Вызов функции для обработки метода
            callback_message = None
            try:
                params_method, response_id, service_callback, method, method_callback = check_params_amqp(
                    self.schema[self.service_name],
                    data
                )
                callback_message = self.methods_service[data['method']](*check_params_amqp(
                    self.schema[self.service_name],
                    data))
            except KeyError as e:
                error_message = {'error': f"Метод {data['method']} не поддерживается"}
                ch.basic_publish(exchange=config_service['exchange'],
                                 routing_key=get_route_key(config_service['quenue']),
                                 body=create_callback_message_amqp(message=error_message,
                                                                   result=False, response_id=data['id'],
                                                                   method_name=data['method']))
                ch.basic_ack(delivery_tag=method_request.delivery_tag)
                return
            except Exception as e:
                error_message = {'error': f"Ошибка {str(e)}"}
                ch.basic_publish(exchange=config_service['exchange'],
                                 routing_key=get_route_key(config_service['quenue']),
                                 body=create_callback_message_amqp(message=error_message,
                                                                   result=False, response_id=data['id'],
                                                                   method_name=data['method']))
                ch.basic_ack(delivery_tag=method_request.delivery_tag)
                return

            ch.basic_ack(delivery_tag=method_request.delivery_tag)
            if callback_message is None:
                return
            try:
                json_callback = create_callback_message_amqp(message=callback_message[0],
                                                             result=callback_message[1],
                                                             response_id=response_id)

            except TypeError:
                raise TypeError('Функция-обработчик должна возвращать tuple(сообщение, результат сообщения) или None')

            ch.basic_publish(exchange=config_service['exchange'],
                             routing_key=get_route_key(config_service['quenue']),
                             body=json_callback.encode('utf-8'))

        def on_request2(ch, method_request, props, body):
            ch.basic_ack(delivery_tag=method_request.delivery_tag)
            try:
                # Проверка серилизации
                data = json.loads(body.decode('utf-8'))
                exchange_name_callback = get_exchange_service(data['service_callback'], self.schema)
                queue_name_callback = get_queue_service(data['service_callback'], self.schema)
                self.logger.info(f"[SER] Серилизован {data=}")
            except Exception as e:
                self.logger.info(f"[SER] Ошибка серилизации {body.decode('utf-8')}")
                return

            if 'response_id' in data:
                # Обработка колбека
                self.logger.warning('[Callback] У синхронной версии библиотеки не доступна обработка колбека')
                return
            else:
                # Обработка обычного сообщения
                try:
                    self.logger.info("[Message] Начало обработки сообщения")
                    out = self.process_incoming_message(data)
                    if out is None:
                        out_message = None
                    else:
                        out_message = out.encode('utf-8')
                    self.logger.info(f"[Message] Конец обработки сообщения {out=}")
                except Exception as e:
                    self.logger.info(f"[Message] {e}")
                    return

            if out_message is not None:
                ch.basic_publish(exchange=exchange_name_callback,
                                 routing_key=get_route_key(queue_name_callback),
                                 body=out_message)
                self.logger.info(f"Сообщение отправлено в очередь {queue_name_callback}")

        connection = self._open_amqp_connection_current_service()
        channel = connection.channel()

        channel.basic_consume(on_message_callback=on_request2, queue=self.queue)
        channel.start_consuming()

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
            AMQP - Ответ от сервера. HTTP - ID сообщения.
        """

        if requested_service not in self.schema:
            raise ServiceNotFound
        method = find_method(method_name, self.schema[requested_service])
        check_rls(self.schema[self.service_name], requested_service, self.service_name, method_name)

        if method.type_conn == 'HTTP':
            return self.make_request_api_http(method, params)

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
        url = method.get_url_http()
        message = method.get_message_http(params)
        headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
        auth = None
        if method.config.auth:
            auth = HTTPBasicAuth(method.config.username, method.config.password)
        response = None
        if method.config.type == 'POST':
            response = requests.post(url, headers=headers, data=message, auth=auth).text
        if method.config.type == 'GET':
            response = requests.get(url, data=message, headers=headers, auth=auth).text

        return response

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
        connection = self._open_amqp_connection_current_service()
        channel = connection.channel()

        message = method.get_message_amqp(params, self.service_name, callback_method_name)

        channel.basic_publish(exchange=method.config.exchange,
                              routing_key=get_route_key(method.config.quenue),
                              body=message.json().encode('utf-8'))

        channel.close()
        connection.close()

        return message.id

    def send_callback(self, service_name: str, message: dict, response_id: str, result: bool,
                      method_callback: str, channel=None):
        """
        Отправка колбека через кролика.
        :param channel: открытое соединение с кроликов. Если не указан будет создано временное
        :param method_callback: колбек для обработки метода
        :param result: результат
        :param response_id: id сообщения на который делается колбек.
        :param service_name: Название сервиса на который делается колбек.
        :param message: сообщение
        :return:
        """
        queue_callback = get_queue_service(service_name, self.schema)
        exchange_callback = get_exchange_service(service_name, self.schema)
        callback_message = create_callback_message_amqp(message=message, result=result,
                                                        response_id=response_id)

        if not channel:
            connection = self._open_amqp_connection_current_service()
            channel = connection.channel()
            channel.basic_publish(exchange=exchange_callback,
                                  routing_key=get_route_key(queue_callback),
                                  body=callback_message.encode('utf-8'))
            channel.close()
            connection.close()
        else:
            channel.basic_publish(exchange=exchange_callback,
                                  routing_key=get_route_key(queue_callback),
                                  body=callback_message.encode('utf-8'))

    def process_incoming_message(self, data: dict) -> Optional[str]:
        # Проверка наличия такого сервиса в схеме АПИ
        service_callback = data['service_callback']
        try:
            config_service = self.schema[service_callback]['AMQP']['config']
        except KeyError as e:
            self.logger.error(f'Нет сервиса {service_callback} в апи')
            return
        # Проверка доступности метода
        try:
            check_rls(service_from_schema=self.schema[service_callback], service_to_name=self.service_name,
                      service_from_name=service_callback, method_name=data['method'])
        except (ServiceMethodNotAllowed, AllServiceMethodsNotAllowed):
            error_message = {'error': f"Метод {data['method']} не доступен из сервиса {service_callback}"}
            body_message = create_callback_message_amqp(error_message, False, data['id'],
                                                        service_name=self.service_name)
            return body_message

        # Вызов функции для обработки метода
        try:
            callback_message = self.methods_service[data['method']](check_params_amqp(
                self.schema[self.service_name],
                data))
        except KeyError as e:
            error_message = {'error': f"Метод {data['method']} не поддерживается"}
            body_message = create_callback_message_amqp(error_message, False, data['id'])
            return body_message
        except Exception as e:
            self.logger.error(traceback.format_exc())
            error_message = {'error': f"Ошибка {str(e)}"}
            body_message = create_callback_message_amqp(error_message, False, data['id'])
            return body_message

        if callback_message is None:
            return

        json_callback = callback_message.json()

        return json_callback

    def process_callback_message(self, data: dict) -> Optional[str]:
        pass
