import datetime
import unittest
from api_lib.utils.convert_utils import convert_date_from_iso, convert_date_into_iso


class DateTestCase(unittest.TestCase):

    def test_convert_to_from(self):
        date = datetime.datetime.now()
        date_str = convert_date_into_iso(date)
        date_from_iso = convert_date_from_iso(date_str)
        self.assertTrue(date == date_from_iso)
