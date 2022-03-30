""" Проверки """
import abc
import hashlib
import re
import datetime
import json
import uuid
from typing import Union, List, Tuple

from .custom_exceptions import ServiceMethodNotAllowed, RequireParamNotSet, ParamNotFound, MethodNotFound, \
    ParamValidateFail, WrongTypeParam, WrongSizeParam, AllServiceMethodsNotAllowed
from .messages import IncomingMessage
from .utils_message import create_hash


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

    def __init__(self, address, auth: bool, ssl: bool, type, endpoint, username, password, timeout: int,
                 port: int):
        super().__init__(address, username, password, timeout, port)
        self.auth = auth
        self.ssl = ssl
        self.type = type
        self.endpoint = endpoint

    def get_url(self, method_name):
        # TODO: убрать после тестов
        # url = f"http{'s' if self.ssl else ''}://{self.address}:{self.port}{self.endpoint}{method_name}"
        url = f"http{'s' if self.ssl else ''}://{self.address}{self.endpoint}{method_name}"
        return url


class Param(object):

    def __init__(self, type_param, length, is_required, name):
        self.type = type_param
        self.length = length
        self.is_required = is_required
        self.name = name

    def check_value(self, value: any):
        # TODO проверка на бинарный тип, md5
        # TODO более подробная обработка ошибок валидации данных
        value_type = self.type
        size = self.length

        if value_type == 'str':
            if not isinstance(value, str):
                raise WrongTypeParam(self.name, 'str')
            if size is not None:
                if len(value) > size:
                    raise WrongSizeParam(self.name, size)
        elif value_type == 'int':
            if not isinstance(value, int):
                raise WrongTypeParam(self.name, 'int')
            if size is not None:
                if len(str(value)) > size:
                    raise WrongSizeParam(self.name, size)
        elif value_type == 'guid':
            try:
                uuid.UUID(value)
            except Exception:
                raise WrongTypeParam(self.name, 'guid')
        elif value_type == 'md5':
            try:
                hashlib.md5(value)
            except:
                raise WrongTypeParam(self.name, 'md5')
        elif value_type == 'json':
            try:
                json.loads(value)
            except:
                raise WrongTypeParam(self.name, 'json')
        elif value_type == 'bin':
            # TODO: разобраться в способе передачи bin
            pass
            # if not isinstance(value, bytes):
            #     raise WrongTypeParam(self.name, 'bin')
        elif value_type == 'base64':
            if not isinstance(value, str):
                raise WrongTypeParam(self.name, 'base64')
            if not value.startswith('base64='):
                ValueError('Параметр с типов base64 должен начинаться с base64=')
        elif value_type == 'date':
            value: str
            regex = r'^([0-9]{4})-([0-1][0-9])-([0-3][0-9])T([0-1][0-9]|[2][0-3]):([0-5][0-9]):([0-5][0-9])±(0[' \
                    r'0-9]|1[0-9]|2[0-3]):([0-5][0-9])$'
            if not re.fullmatch(regex, value):
                raise WrongTypeParam(self.name, 'date')
        elif value_type == 'float':
            if not isinstance(value, float):
                raise WrongTypeParam(self.name, 'float')
            value_str = str(value)
            common_length = len(value_str.split('.')[0])
            drob_length = len(value_str.split('.')[1])
            if common_length > int(size.split('.')[0]):
                raise WrongSizeParam(self.name, size)
            if drob_length > int(size.split('.')[1]):
                raise WrongSizeParam(self.name, size)
        elif value_type == 'bool':
            if not isinstance(value, bool):
                raise WrongTypeParam(self.name, 'bool')


def convert_date_into_iso(convert_date: datetime.datetime) -> str:
    """
    :param convert_date: Дата
    :return: Строковое представление даты в исо формате YYYY-MM-DDThh:mm:ss±hh:mm
    """

    return convert_date.replace(microsecond=0).astimezone().isoformat().replace('+', '±')


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

    def get_value(self):
        """ Возвращает отформатирванное значение для серилизации """
        if isinstance(self.value, bytes):
            return self.value.decode('utf-8')

        return self.value

    def to_dict(self):
        return {self.name: self.get_value()}


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
            param.check_value(input_param.value)

            param.is_set = True
        # Проверка на наличие всех обязательных параметров
        for param in params_copy:
            if param.is_required and not hasattr(param, 'is_set'):
                raise RequireParamNotSet(f'Обязательный параметр {param.name} не задан')

        return True

    def get_url_http(self):
        return self.config.get_url(self.name)

    def get_message_http(self, params: List[InputParam]) -> Union[str, dict]:
        self.check_params(params)

        if len(params) == 1:
            params = params[0].to_dict()
        else:
            params = [param.to_dict() for param in params]

        if isinstance(self.config, ConfigHTTP):
            # return json.dumps(params, ensure_ascii=True, default=str)
            return params

    def get_message_amqp(self, params: List[InputParam], service_name: str,
                         callback_method_name: str) -> IncomingMessage:
        self.check_params(params)
        if isinstance(self.config, ConfigAMQP):
            message: dict = {'service_callback': service_name,
                             'method': self.name,
                             'method_callback': callback_method_name}
            for i in params:
                message[i.name] = i.get_value()

            hash_id = create_hash(message)
            message['id'] = str(hash_id)
            return IncomingMessage.from_dict(message)


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


def check_hash_sum(message):
    """ Проверка хеш суммы сообщения при получении """
    # TODO: сделать проверку хеш-суммы
    pass


def check_rls(service_from_schema: dict, service_to_name: str, service_from_name: str, method_name: str):
    if 'RLS' not in service_from_schema:
        return

    if service_to_name not in service_from_schema['RLS']:
        raise AllServiceMethodsNotAllowed(service_from=service_from_name, name_service=service_to_name)

    if service_from_schema['RLS'][service_to_name] is None:
        return

    if 'disallowed' in service_from_schema['RLS'][service_to_name]:
        if method_name in service_from_schema['RLS'][service_to_name]['disallowed']:
            raise ServiceMethodNotAllowed(name_service=service_to_name, service_from=service_from_name,
                                          name_method=method_name)

    if 'allowed' in service_from_schema['RLS'][service_to_name]:
        if method_name not in service_from_schema['RLS'][service_to_name]['allowed']:
            raise ServiceMethodNotAllowed(name_service=service_to_name, service_from=service_from_name,
                                          name_method=method_name)
