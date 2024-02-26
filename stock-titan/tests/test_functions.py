import unittest
from datetime import datetime, timezone
from helpers import combine_text, create_uuid, create_date#, create_node
# import pandas as pd

class TestCombineText(unittest.TestCase):
    def test_combine_text(self):
        record = {"summary": "This is the content.", "article": "This is the article."}
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

# class TestCreateNode(unittest.TestCase):
#     def test_create_node(self):
#         # Mocking the row data with the schema
#         row_data = {
#             "id": "some_uuid",
#             "source": "source_value",
#             "title": "title_value",
#             "url": "url_value",
        #     "content": "content_value",
        #     "article": "article_value",
        #     "created_at": "2022-01-01 12:00:00",
        # }
        # record = pd.DataFrame(row_data, index=[0])
        # # Calling the create_node function with the row data
        # node = create_node(record)
        # text = combine_text(record.to_dict())
        # # Asserting the properties of the created node
        # self.assertEqual(node.text, f"title_value\n\nImpact Score: \n\nSentiment: \n\n{text}")
        # self.assertEqual(node.metadata["created_at"], "2022-01-01T12:00:00")
        # self.assertEqual(node.metadata["source"], "Stock Titan")
        # self.assertEqual(node.metadata["url"], "https://www.stocktitan.net/news/")

if __name__ == "__main__":
    unittest.main()

if __name__ == '__main__':
    unittest.main()