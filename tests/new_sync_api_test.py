import datetime
import unittest

from sync_api import ApiSync


class TestSyncApi(unittest.TestCase):

    # Test Data
    test_schema = {
        'CallbackService': {
            'AMQP': {
                'config': {
                    'address': '192.168.0.216',
                    'port': 5672,
                    'username': 'guest',
                    'password': 'guest',
                    'exchange': '',
                    'quenue': 'SendService_q',
                    'virtualhost': '/',
                },
                'methods': {
                    'write': {
                        'test_method': {
                            'test_str': ['str', 32, True]
                        }

                    }
                }
            }

        },
        'SendService': {
            'AMQP': {
                'config': {
                    'address': '192.168.0.216',
                    'port': 5672,
                    'username': 'guest',
                    'password': 'guest',
                    'exchange': '',
                    'quenue': 'ReadService_q',
                    'virtualhost': '/',
                },
                'methods': {
                    'write': {
                        'test_method': {
                            'test_str': ['str', 32, True]
                        }

                    }
                }
            }

        }

    }

    def test_open_connection(self):
        """ Открытие соединение """
        self.api = ApiSync('CallbackService')
        self.api.schema = self.test_schema

    def test_start_reading(self):
        """ Начать слушать очередь """
        def method(params):
            return str(datetime.datetime.now()) + params['test_str']
        self.api.listen_queue(test_method=method)

    # def test_send_callback(self):
    #     """ Отправить на сервис запрос, получить корректный ответ"""
    #     pass
    #
    # def test_send_callback_error(self):
    #     """ Отправить на сервис не соответсвующий запрос """
    #     pass
