# Set up source path for pytest. Make sure this file is loaded before any other test files.
import unittest
from etl import elsapi

class ElsApiTestCase(unittest.TestCase):
    def test_get_docs_by_year(self):
        elsapi.get_docs_by_year(2018)

if __name__ == '__main__':
    unittest.main()
