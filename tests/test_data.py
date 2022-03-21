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
                        'test_not_set': ['str', 32, False]
                        # TODO: все типы данных

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