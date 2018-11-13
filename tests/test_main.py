import unittest
from etl.elsapy import elsclient


class MyTestCase(unittest.TestCase):
    def test_elsapy(self):
        client = elsclient.ElsClient('fake api key')
        self.assertIsNotNone(self, client)


if __name__ == '__main__':
    unittest.main()
