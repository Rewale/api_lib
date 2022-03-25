""" Модуль утилит для работы с кроликом """
import copy
import hashlib
import json
from typing import Union

from .custom_exceptions import *
from .utils_message import create_hash, serialize_message
from .validation_utils import find_method, InputParam


def get_route_key(queue_name: str):
    """ Оборачивает имя очереди в # """
    return f'#{queue_name}#'


def check_params_amqp(schema_service: dict, params: dict):
    """ Проверка входящих/исходящих параметров """
    try:
        assert 'id' in params.keys() \
               and 'service_callback' in params.keys() \
               and 'method' in params.keys() \
               and 'method_callback' in params.keys()
    except AssertionError:
        raise ValueError('Сообщение обязательно должно содержать id, response_id, method, method_callback')
    method = find_method(params['method'], schema_service)
    params_method = copy.copy(params)
    del params_method['id']
    del params_method['service_callback']
    del params_method['method']
    del params_method['method_callback']

    method.check_params(InputParam.from_dict(params_method))

    return IncomingMessage(response_id=params['id'],
                           params=params_method,
                           method_callback=params['method_callback'],
                           service_callback=params['service_callback'])
    # params_method, params['id'], params['service_callback'], params['method'], params['method_callback']


def service_amqp_url(service_schema: dict):
    """ AQMP url из описания сервиса """
    service_schema = service_schema['AMQP']
    login = service_schema['config']['username']
    password = service_schema['config']['password']
    address = service_schema['config']['address']
    port = 5672 if service_schema['config']['port'] == '' else service_schema['config']['port']
    vhost = service_schema['config']['virtualhost']
    amqp = f'amqp://{login}:{password}@{address}:{port}/{vhost}'
    return amqp


# def json_to_response(json_response: dict, response_id: int, result: bool, method: str = None,
#                      service_callback: str = None):
#     """ Оборачивает строку json в корректный для сервиса вид """
#     correct_json = {
#         'response_id': response_id,
#         'method': method,
#         'service_callback': service_callback,
#         'message': {
#             'result': result,
#             'response': json_response
#         }
#     }
#     response_without_id = json.dumps(correct_json, default=str, ensure_ascii=False)
#     correct_json['id'] = hashlib.md5(response_without_id.encode('utf-8')).digest().hex(' ', 1).upper()
#
#     return json.dumps(correct_json, ensure_ascii=False, default=str)


def check_schema_decorator(func):
    def wrapped(service_name: str, schema: dict):
        try:
            return func(service_name, schema)
        except:
            if service_name not in schema:
                raise Exception(f'{service_name} не существует в схеме АПИ!')
            else:
                raise Exception(f'{service_name} не поддерживает работу по AMQP')

    return wrapped


# Для упрощения работы со схемой апи
@check_schema_decorator
def get_queue_service(service_name: str, schema: dict):
    return schema[service_name]['AMQP']['config']['quenue']


@check_schema_decorator
def get_amqp_address_service(service_name: str, schema: dict):
    return schema[service_name]['AMQP']['config']['address']


@check_schema_decorator
def get_virtualhost_service(service_name: str, schema: dict):
    return schema[service_name]['AMQP']['config']['virtualhost']


@check_schema_decorator
def get_exchange_service(service_name: str, schema: dict):
    return schema[service_name]['AMQP']['config']['exchange']


@check_schema_decorator
def get_amqp_username_service(service_name: str, schema: dict):
    return schema[service_name]['AMQP']['config']['username']


@check_schema_decorator
def get_amqp_password_service(service_name: str, schema: dict):
    return schema[service_name]['AMQP']['config']['password']


@check_schema_decorator
def get_port_amqp_service(service_name: str, schema: dict):
    return schema[service_name]['AMQP']['config']['port']


@check_schema_decorator
def get_method_service(service_name: str, method_name: str, schema: dict):
    return schema[service_name]['AMQP']['methods']['write'][method_name]


def check_methods_handlers(service_schema: dict, methods: dict):
    for name_type in service_schema['AMQP']['methods']:
        for name_method in service_schema['AMQP']['methods'][name_type]:
            if methods is None:
                raise MethodsNotSet()
            if name_method not in methods:
                raise MethodNotSet(name_method)


class CallbackMessage:
    def __init__(self,
                 method: str,
                 service_callback: str,
                 response_id: str,
                 result: bool,
                 response: Union[str, dict],
                 id:str = None):
        self.id = id
        self.method = method
        self.service_callback = service_callback
        self.response_id = response_id
        self.result = result
        if isinstance(response, str):
            self.response = json.loads(response)
        else:
            self.response = response

    def json(self) -> str:
        """ Возвращает строку json """
        correct_json = {
            'response_id': self.response_id,
            'service_callback': self.service_callback,
            'method': self.method,
            'message': {
                'result': self.result,
                'response': self.response
            }
        }

        hash_id = create_hash(correct_json)
        correct_json['id'] = hash_id

        return serialize_message(correct_json)

    @staticmethod
    def from_dict(message: dict):
        return CallbackMessage(
            id=message['id'],
            method=message['method'],
            service_callback=message['service_callback'],
            response_id=message['response_id'],
            result=message['message']['result'],
            response=message['message']['response']
        )

class IncomingMessage:
    def __init__(self, response_id: str, service_callback: str, params: dict, method_callback: str):
        self.params = params
        self.service_callback = service_callback
        self.id = response_id
        self.method_callback = method_callback

    @staticmethod
    def from_dict(message: dict):
        params = copy.copy(message)
        del params['id']
        del params['method_callback']
        del params['service_callback']
        return IncomingMessage(
            response_id=message['id'],
            service_callback=message['service_callback'],
            params=params,
            method_callback=message['method_callback']
        )

    def callback_message(self, param: dict, result: bool):
        """

        :param param: Выходные параметры
        :param result: результат выполнения
        :return: сообщение колбека
        """
        return CallbackMessage(response_id=self.id,
                               service_callback=self.service_callback,
                               method=self.method_callback,
                               result=result,
                               response=param)


def create_callback_message_amqp(message: dict,
                                 result: bool,
                                 response_id: str,
                                 service_name: str = None,
                                 method_name: str = None) -> str:
    """
    Получить отформатирванное сообщения с hash id для колбека
    :param method_name: Имя метода который отправляет колбек
    :param message: Сообщение в виде словаря из хендлера.
    :param result: Успешность выполнения.
    :param service_name: Название сервиса.
    :param callback_method_name: Метод колбека для текущего сервиса.
    :param response_id: ID сообщения на который делается колбек
    :return: json-строка
    """
    correct_json = {
        'response_id': response_id,
        'service_callback': service_name,
        'method': method_name,
        'message': {
            'result': result,
            'response': message
        }
    }

    hash_id = create_hash(correct_json)
    correct_json['id'] = hash_id

    return serialize_message(correct_json)
