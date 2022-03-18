test_schema_rpc = {
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
                'timeout': 30000
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
                'timeout': 30000
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