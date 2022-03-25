""" Модуль утилит для работы с кроликом """
import copy
import hashlib
import json
from .custom_exceptions import *
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

    return params_method, params['id'], params['service_callback'], params['method'], params['method_callback']


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
    def __init__(self, id: str, method: str, service_callback: str, response_id: str, result: bool, response: str):
        self.id = id
        self.method = method
        self.service_callback = service_callback
        self.response_id = response_id
        self.result = result
        self.response = json.loads(response)

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
