# Set up source path for pytest. Make sure this file is loaded before any other test files.
import unittest
from etl import elsapi
from pymongo import MongoClient


class MongoTestCase(unittest.TestCase):
    def test_search_by_serial_title(self):
        client = MongoClient('localhost', 27017)
        db = client['scopus']
        collection = db['serial']
        title = 'Lancet'
        self.assertIsNotNone(collection.find_one({'dc:title': {'$in': [title, 'The '+title]}}))


if __name__ == '__main__':
    unittest.main()
