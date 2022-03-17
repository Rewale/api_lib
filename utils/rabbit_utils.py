""" Модуль утилит для работы с кроликом """
import hashlib
import json


def get_route_key(queue_name: str):
    """ Оборачивает имя очереди в # """
    return f'#{queue_name}#'


def service_amqp_url(service_name: dict):
    """ AQMP url из описания сервиса """
    login = service_name['config']['username']
    password = service_name['config']['password']
    address = service_name['config']['address']
    port = 5672 if service_name['config']['port'] == '' else service_name['config']['port']
    vhost = service_name['config']['virtualhost']
    amqp = f'amqp://{login}:{password}@{address}:{port}/{vhost}'
    return amqp


def json_to_response(json_response: dict, response_id: int, result: bool, method: str = None,
                     service_callback: str = None):
    """ Оборачивает строку json в корректный для сервиса вид """
    correct_json = {
        'response_id': response_id,
        'method': method,
        'service_callback': service_callback,
        'message': {
            'result': result,
            'response': json_response
        }
    }
    response_without_id = json.dumps(correct_json, default=str, ensure_ascii=False)
    correct_json['id'] = hashlib.md5(response_without_id.encode('utf-8')).digest().hex(' ', 1).upper()

    return json.dumps(correct_json, ensure_ascii=False, default=str)


# Для упрощения работы со схемой апи
def get_queue_service(service_name: str, schema: dict):
    return schema[service_name]['AMQP']['config']['quenue']


def get_amqp_address_service(service_name: str, schema: dict):
    return schema[service_name]['AMQP']['config']['address']


def get_virtualhost_service(service_name: str, schema: dict):
    return schema[service_name]['AMQP']['config']['virtualhost']


def get_exchange_service(service_name: str, schema: dict):
    return schema[service_name]['AMQP']['config']['exchange']


def get_amqp_username_service(service_name: str, schema: dict):
    return schema[service_name]['AMQP']['config']['username']


def get_amqp_password_service(service_name: str, schema: dict):
    return schema[service_name]['AMQP']['config']['password']


def get_port_amqp_service(service_name: str, schema: dict):
    return schema[service_name]['AMQP']['config']['port']


def get_method_service(service_name: str, method_name: str, schema: dict):
    return schema[service_name]['AMQP']['methods']['write'][method_name]
