import json
import uuid

from api_lib.sync_api import ApiSync
import unittest

from api_lib.utils.custom_exceptions import AllServiceMethodsNotAllowed, ServiceNotFound
from api_lib.utils.rabbit_utils import get_queue_service, get_exchange_service
from api_lib.utils.validation_utils import InputParam


class SetProgrTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.api = ApiSync('GIBDD', '', '', methods={'check_info': None}, is_test=True)

    def test_queue(self):
        self.assertEqual('gibddprogr', get_queue_service(self.api.service_name, self.api.schema))

    def test_exchange(self):
        self.assertEqual('gibddprogr', get_exchange_service(self.api.service_name, self.api.schema))

    def test_send_KAA(self):
        with self.assertRaises(AllServiceMethodsNotAllowed):
            self.api.send_request_api(method_name='update_client',
                                      params=[
                                          InputParam(name='guid', value=str(uuid.uuid4())),
                                          InputParam(name='args', value=json.dumps({'test': 123}))],
                                      requested_service='KAA')

    def test_send_KAAPROGR(self):
        with self.assertRaises(ServiceNotFound):
            self.api.send_request_api(method_name='update_client',
                                      params=[
                                          InputParam(name='guid', value=str(uuid.uuid4())),
                                          InputParam(name='args', value=json.dumps({'test': 123}))],
                                      requested_service='KAAPROGR')


class WorkProgrSetTestCase(unittest.TestCase):
    # TODO: написать тест для рабочей
    def setUp(self) -> None:
        self.api = ApiSync('GIBDD', '', '', methods={'check_info': None}, is_test=False)

    def test_queue(self):
        self.assertEqual('gibdd', get_queue_service(self.api.service_name, self.api.schema))

    def test_exchange(self):
        self.assertEqual('gibdd', get_exchange_service(self.api.service_name, self.api.schema))

    def test_send_KAA(self):
        with self.assertRaises(AllServiceMethodsNotAllowed):
            self.api.send_request_api(method_name='update_client',
                                      params=[
                                          InputParam(name='guid', value=str(uuid.uuid4())),
                                          InputParam(name='args', value=json.dumps({'test': 123}))],
                                      requested_service='GPS')

    def test_send_KAAPROGR(self):
        with self.assertRaises(AllServiceMethodsNotAllowed):
            self.api.send_request_api(method_name='update_client',
                                      params=[
                                          InputParam(name='guid', value=str(uuid.uuid4())),
                                          InputParam(name='args', value=json.dumps({'test': 123}))],
                                      requested_service='KAAPROGR')
