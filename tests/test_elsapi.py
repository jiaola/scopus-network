# Set up source path for pytest. Make sure this file is loaded before any other test files.
import unittest
from etl import elsapi


class ElsApiTestCase(unittest.TestCase):

    # def test_get_doc_refs_no_refs(self):
    #     refs = elsapi.get_doc_refs('85046457751')
    #     self.assertIsNone(refs)
    #
    # def test_get_doc_refs_one_ref(self):
    #     refs = elsapi.get_doc_refs('85044232847').get('references', {}).get('reference', [])
    #     self.assertTrue(isinstance(refs, dict))
    #
    # def test_get_doc_refs_multi_refs(self):
    #     refs = elsapi.get_doc_refs('85041379112').get('references', {}).get('reference', [])
    #     self.assertTrue(isinstance(refs, list))
    #     self.assertGreater(len(refs), 30)
    #
    # def test_get_serial_by_title(self):
    #     results = elsapi.get_serial_by_title('lancet')
    #     self.assertGreater(len(results), 10)

    def test_get_docs_by_author(self):
        results = elsapi.get_docs_by_author('23065981600')
        self.assertGreater(len(results), 80)


if __name__ == '__main__':
    unittest.main()
