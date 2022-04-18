import base64
import datetime
import json


def base64_to_json(base64_string: str):
    return json.loads(base64.b64decode(base64_string.replace('base64=', '')))


def convert_date_into_iso(convert_date: datetime.datetime) -> str:
    """
    :param convert_date: Дата
    :return: Строковое представление даты в исо формате YYYY-MM-DDThh:mm:ss±hh:mm
    """
    return convert_date.replace(microsecond=0).astimezone().isoformat().replace('+', '±')


def convert_date_from_iso(convert_date: str) -> datetime.datetime:
    """
    :param convert_date: Дата
    :return: Строковое представление даты в исо формате YYYY-MM-DDThh:mm:ss±hh:mm
    """
    return datetime.datetime.fromisoformat(convert_date.replace('±', '+'))


def decode_b64(message: str):
    message_bytes = message.encode('utf-8')
    base64_bytes = base64.b64encode(message_bytes)
    return 'base64=' + base64_bytes.decode('utf-8')


def add_progr(service_name: str):
    return f'{service_name}PROGR'
