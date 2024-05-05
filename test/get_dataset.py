from dotenv import load_dotenv
import os
import unittest
from strdset import StreamDsetClient
load_dotenv()

class DatasetTest(unittest.TestCase):
    def setUp(self):
        self.client = StreamDsetClient(
            os.environ.get("TEST_ACCESS_KEY_ID", ""),
            os.environ.get("TEST_ACCESS_KEY_SECRET","")
        )

    def test_get_datasets(self):
        resp = self.client.get_datasets()
        self.assertGreaterEqual(len(resp), 0)
    
    def test_get_dataset(self):
        resp = self.client.get_dataset(1)
        self.assertTrue(resp.id)
        
def test():
    unittest.main()

if __name__ == "__main__":
    test()
