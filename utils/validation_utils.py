""" Проверки """
import re
import datetime
import json
from typing import Union
from .rabbit_utils import *

from utils.custom_exceptions import ServiceMethodNotAllowed, RequireParamNotSet, ParamNotFound, MethodNotFound


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
              r'1-7])?|(00[1-9]|0[1-9]\d|[12]\d{2}|3([0-5]\d|6[1-6])))([T\s]((([01]\d|2[0-3])((:?)[0-5]\d)?|24\:?00)(' \
              r'[\.,]\d+(?!:))?)?(\17[0-5]\d([\.,]\d+)?)?([zZ]|([\+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)?$'
    result = re.fullmatch(pattern, value_date)

    assert len(value_date) == 25
    assert result is not None


def check_params(method_params: dict, params: Union[dict, list]):
    r"""
    Валидация параметров

    Args:
        method_params: Параметры метода api.
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

        if len(param) != method_params['Params']:
            raise ParamNotFound

    if isinstance(params, dict):
        params = [params]

    for param in params:
        check_param(param)

    return True


def check_hash():
    pass








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


def check_hash_sum(message):
    """ Проверка хеш суммы сообщения при получении """
    pass
