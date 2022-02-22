from main import ApiSync
api_client = ApiSync('GPSPROGR')


def test_send_request():
    api_client.send_request_api('newuser', {'fio': "12312", 'organizations': ['111', '111']}, False)

def test_wrond_type():
    api_client.send_request_api('newuser', {'fio': 123, 'organizations': ['111', '111']}, False)
