""" Проверки """
import abc
import hashlib
import re
import datetime
import json
from typing import Union, List
from .rabbit_utils import *

from utils.custom_exceptions import ServiceMethodNotAllowed, RequireParamNotSet, ParamNotFound, MethodNotFound, \
    ParamValidateFail


class Config(abc.ABC):

    def __init__(self, address, username, password, timeout: int, port: int):
        self.address = address
        self.username = username
        self.password = password
        self.timeout = timeout
        self.port = port


class ConfigAMQP(Config):
    def __init__(self, address, username, password, timeout, port, quenue, virtualhost, exchange):
        super().__init__(address, username, password, timeout, port)
        self.quenue = quenue
        self.virtualhost = virtualhost
        self.exchange = exchange


class ConfigHTTP(Config):

    def __init__(self, address, auth: bool, ssl: bool, type_http, endpoint, username, password, timeout: int, port: int):
        super().__init__(address, username, password, timeout, port)
        self.auth = auth
        self.ssl = ssl
        self.type = type_http
        self.endpoint = endpoint


class Param(object):

    def __init__(self, type_param, length, is_required, name):
        self.type = type_param
        self.length = length
        self.is_required = is_required
        self.name = name

    def check_value(self, value: any):
        # TODO проверка на бинарный тип, md5, guid
        value_type = self.type
        size = self.length

        if value_type == 'str':
            assert isinstance(value, str)
            if size is not None:
                assert len(value) <= size
        elif value_type == 'int':
            assert isinstance(value, int)
            if size is not None:
                assert value <= 10 ** size
        elif value_type == 'guid':
            assert isinstance(value, str) and len(value) == 36
        elif value_type == 'md5':
            hashlib.md5(value)
        elif value_type == 'json':
            json.loads(value)
        elif value_type == 'bin':
            pass
        elif value_type == 'base64':
            assert isinstance(value, str)
        elif value_type == 'date':
            datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
        elif value_type == 'bool':
            assert isinstance(value, bool)


class InputParam(object):

    def __init__(self, name: str, value: any):
        self.name = name
        self.value = value

    @staticmethod
    def from_dict(param: dict):
        list_input_params: List[InputParam] = list()
        for name, value in param.items():
            list_input_params.append(InputParam(name=name, value=value))

        return list_input_params


class MessageApi(object):

    def __init__(self):
        self.response_id = None
        self.id = None
        self.method = None
        self.service_callback = None


class MethodApi(object):

    def __init__(self, params: dict, type_conn, type_method, config: dict, method_name):
        self.params = list()
        for name, value in params.items():
            self.params.append(Param(*value, name=name))
        self.type_conn = type_conn
        self.type_method = type_method
        if type_conn == 'AMQP':
            self.config: ConfigAMQP
            self.config = ConfigAMQP(**config)
        elif type_conn == 'HTTP':
            self.config: ConfigHTTP
            self.config = ConfigHTTP(**config)
        else:
            ValueError(f'Неизвестный тип подключения {type_conn}')
        self.name = method_name

    def check_params(self, input_params: List[InputParam]):
        params_copy = self.params.copy()
        for input_param in input_params:
            # Проверка на существование параметра
            try:
                param = next(x for x in params_copy if x.name == input_param.name)
            except StopIteration:
                raise ParamNotFound(f'Параметра с именем {input_param.name} не существует')
            # Проверка на тип
            try:
                param.check_value(input_param.value)
            except Exception as e:
                raise ParamValidateFail(f'Параметр {param.name} ошибка {e}')

            param.is_set = True
        # Проверка на наличие всех обязательных параметров
        for param in params_copy:
            if param.is_required and not hasattr(param, 'is_set'):
                raise RequireParamNotSet(f'Обязательный параметр {param.name} не задан')

        return True


def find_method(method_name, service_schema: dict):
    """ Поиск метода в схеме, возвращает метод с типом подключения """
    for key, value in service_schema.items():
        if 'methods' not in value:
            continue

        method: MethodApi
        if 'write' in value['methods']:
            for method_key, method_value in value['methods']['write'].items():
                if method_key == method_name:
                    method = MethodApi(params=method_value, type_conn=key, type_method='write',
                                       config=value['config'], method_name=method_name)
                    return method
        if 'read' in value['methods']:
            for method_key, method_value in value['methods']['read'].items():
                if method_key == method_name:
                    method = MethodApi(params=method_value, type_conn=key, type_method='read',
                                       config=value['config'], method_name=method_name)

                    return method

    raise MethodNotFound


def check_method_available(method, curr_service_schema, requested_service):
    r"""
    Проверка доступности метода

    Args:
        method: метод апи.
        curr_service_schema: Схема апи текущего сервиса.
        requested_service: Имя сервиса - адресата.
    Raises:
        ServiceMethodNotAllowed - Метод сервиса не доступен из текущего метода.
    Return:
        Всегда true - иначе исключение
    """

    if 'RLS' not in curr_service_schema:
        return True

    if requested_service not in curr_service_schema['RLS']:
        return ServiceMethodNotAllowed

    if 'allowed' not in curr_service_schema['RLS'] \
            and 'disallowed' not in curr_service_schema['RLS']:
        return True

    if 'allowed' in curr_service_schema['RLS'][requested_service] \
            and len(curr_service_schema['RLS'][requested_service]['RLS']['allowed']):
        if method not in curr_service_schema['RLS'][requested_service]:
            raise ServiceMethodNotAllowed
    elif 'disallowed' in curr_service_schema['RLS'][requested_service] \
            and curr_service_schema['RLS'][requested_service]['disallowed']:
        if method in curr_service_schema['RLS'][requested_service]['disallowed']:
            raise ServiceMethodNotAllowed
    else:
        raise ServiceMethodNotAllowed

    return True


def check_date(value_date: str):
    """ Проверка даты на соответствие ISO формату YYYY-MM-DDThh:mm:ss±hh:mm """
    pattern = r'^([\+-]?\d{4}(?!\d{2}\b))((-?)((0[1-9]|1[0-2])(\3([12]\d|0[1-9]|3[01]))?|W([0-4]\d|5[0-2])(-?[' \
              r'1-7])?|(00[1-9]|0[1-9]\d|[12]\d{2}|3([0-5]\d|6[1-6])))([T\s]((([01]\d|2[0-3])((:?)[0-5]\
              d)?|24\:?00)(' \
              r'[\.,]\d+(?!:))?)?(\17[0-5]\d([\.,]\d+)?)?([zZ]|([\+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)?$'
    result = re.fullmatch(pattern, value_date)

    assert len(value_date) == 25
    assert result is not None


def check_params(method: MethodApi, params: Union[dict, list]):
    r"""
    Валидация параметров

    Args:
        method: Параметры метода
        params: Передаваемые параметры в api.
    Raises:
        AssertionError - тип параметра не соответствует типу в методе.
        RequireParamNotSet - не указан обязательный параметр.
        ParamNotFound - параметр не найден
    Return:
        Всегда true - иначе исключение
    """

    def check_param(param: dict):
        # TODO проверка на бинарный тип, md5, guid

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
                assert isinstance(value, str) and len(value) == 36
            elif value_type == 'md5':
                pass
            elif value_type == 'json':
                json.loads(value)
            elif value_type == 'bin':
                pass
            elif value_type == 'base64':
                assert isinstance(value, str)
            elif value_type == 'date':
                datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
            elif value_type == 'bool':
                assert isinstance(value, bool)

    if isinstance(params, dict):
        params = [params]

    for param in params:
        check_param(param)

    return True


def check_hash():
    pass


def check_hash_sum(message):
    """ Проверка хеш суммы сообщения при получении """
    # TODO: сделать проверку хеш-суммы
    pass
