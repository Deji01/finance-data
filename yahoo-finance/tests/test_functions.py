import unittest
from datetime import datetime, timezone
from helpers import combine_text, create_uuid, create_date

class TestCombineText(unittest.TestCase):
    def test_combine_text(self):
        record = {"content": "This is the content.", "article": "This is the article."}
        expected_result = "This is the content. This is the article"
        self.assertEqual(combine_text(record), expected_result)

class TestCreateUUID(unittest.TestCase):
    def test_create_uuid(self):
        datetime_obj = datetime(2022, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        expected_result = create_uuid(datetime_obj)
        # c33f0000-6a95-11ec check that the first 18 characters are the same
        self.assertEqual(expected_result[:18], create_uuid(datetime_obj)[:18])
        # check that the last 6 characters are not the same
        self.assertNotEqual(expected_result[-6:], create_uuid(datetime_obj)[-6:])
        # check that the total length is the same
        self.assertEqual(len(expected_result), len(create_uuid(datetime_obj)))

class TestCreateDate(unittest.TestCase):
    def test_create_date(self):
        input_string = "1 Jan 2022 00:00:00 +0000"
        expected_result = datetime(2022, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(create_date(input_string), expected_result)

if __name__ == '__main__':
    unittest.main()