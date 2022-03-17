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
    api_callback = ApiSync('CallbackService', schema=test_schema)
    api_sending = ApiSync('SendService', schema=test_schema)

    def test_start_reading(self):
        """ Начать слушать очередь """
        def method(params):
            assert True
            return str(datetime.datetime.now()) + params['test_str']
        self.api_callback.listen_queue(test_method=method)

    # def test_send_callback(self):
    #     """ Отправить на сервис запрос, получить корректный ответ"""
    #     pass
    #
    # def test_send_callback_error(self):
    #     """ Отправить на сервис не соответсвующий запрос """
    #     pass
