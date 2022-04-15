import copy
import datetime
import json
from typing import Union

from api_lib.utils import convert_utils
from api_lib.utils.utils_message import create_hash, serialize_message


class IncomingMessage:
    def __init__(self, response_id: str, service_callback: str, params: dict, method_callback: str,
                 additional_data: dict = None, method=None):
        """

        :param response_id: id сообщения
        :param service_callback: сервис колбека
        :param params: Параметры запроса
        :param method_callback: метод для обработки колбека
        :param additional_data: Дополнительные данные
         (указываются при обработке колбека и связывании исходящего сообщения и колбека)
        """
        self.params = params
        self.service_callback = service_callback
        self.id = response_id
        self.method = method
        self.method_callback = method_callback
        self.additional_data = additional_data

    @staticmethod
    def from_dict(message: dict):
        params = copy.copy(message)
        del params['id']
        del params['method_callback']
        del params['service_callback']
        if 'additional_data' not in params:
            return IncomingMessage(
                response_id=message['id'],
                service_callback=message['service_callback'],
                params=params,
                method_callback=message['method_callback']
            )
        else:
            return IncomingMessage(
                response_id=message['id'],
                service_callback=message['service_callback'],
                params=params,
                method_callback=message['method_callback'],
                additional_data=params['additional_data']
            )

    def json(self, additional_data: dict = None):
        """
        Проводит сообщение к json
        :param additional_data: Дополнительные для записи в редис
        :return:
        """
        correct_json = {
            "method": self.method,
            "service_callback": self.service_callback,
            'method_callback': self.method_callback,
        }
        # TODO: id протестировать работу + не учитывать допольнительные данные при хешировании
        if additional_data:
            correct_json = {**correct_json, **self.params, 'id': self.id, 'additional_data': additional_data}
        else:
            correct_json = {**correct_json, **self.params, 'id': self.id}

        return serialize_message(correct_json)

    def callback_message(self, param: Union[dict, list, str], result: bool):
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

    def recheck_message(self, recheck_date: datetime.datetime):
        """ Отправка сообщения обратно в ту же очередь для повторной обработки """
        # TODO: проверка на recheck_date
        self.params['recheck_date'] = convert_utils.convert_date_into_iso(recheck_date)
        return self


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


class CallbackMessage:
    def __init__(self,
                 method: str,
                 service_callback: str,
                 response_id: str,
                 result: bool,
                 response: Union[str, dict, list],
                 id: str = None,
                 incoming_message: IncomingMessage = None):
        """

        :param method: метод-обработчик колбек
        :param service_callback: сервис вернувший колбека
        :param response_id: айди сообщения на который был совершен колбека
        :param result: успешность выполнения
        :param response: ответ
        :param id: айди колбека
        :param incoming_message:  сообщения на которое был совершен колбек
        """
        self.incoming_message = incoming_message
        self.id = id
        self.method = method
        self.service_callback = service_callback
        self.response_id = response_id
        self.result = result
        if isinstance(response, str):
            try:
                self.response = json.loads(response)
            except json.JSONDecodeError:
                self.response = response
        else:
            self.response = response

    def json(self) -> str:
        """ Возвращает строку json """
        correct_json = {
            'response_id': self.response_id,
            'service_callback': self.service_callback,
            # 'method': self.method,
            'method': None,
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
