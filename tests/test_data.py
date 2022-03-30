test_schema_rpc = {
    'CallbackService': {
        'AMQP': {
            'config': {
                'address': '192.168.0.216',
                'port': 5672,
                'username': 'guest',
                'password': 'guest',
                'exchange': 'callbackExchange',
                'quenue': 'callbackQueue',
                'virtualhost': '/',
                'timeout': 30000
            },
            'methods': {
                'write': {
                    'test_method': {
                        'test_str': ['str', 32, True],
                        'test_not_set': ['str', 32, False],
                        'guid': ['guid', 32, False],
                        'bin': ['bin', 32, True],
                        'float': ['float', '8.2', True],
                        'int': ['int', 32, True],
                        'bool': ['bool', None, True],
                        'base64': ['base64', None, True],
                        'date': ['date', None, True],
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
                'exchange': 'sendExchange',
                'quenue': 'sendQueue',
                'virtualhost': '/',
                'timeout': 30000
            },
            'methods': {
                # 'write': {
                #     'test_method': {
                #         'test_str': ['str', 32, True]
                #     }
                #
                # }
            }
        }

    }

}
