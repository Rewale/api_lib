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
