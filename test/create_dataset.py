from dotenv import load_dotenv
import os
import unittest
from strdset import StreamDsetClient, ColumnDefinition, StreamDataset
load_dotenv()

class DatasetTest(unittest.TestCase):
    def setUp(self):
        self.client = StreamDsetClient(
            os.environ.get("TEST_ACCESS_KEY_ID", ""),
            os.environ.get("TEST_ACCESS_KEY_SECRET","")
        )

    def test_create_dataset(self):
        dataset = self.client.create_dataset("batch_test", [
            ColumnDefinition("music name", "string"),
            ColumnDefinition("file", "file"),
        ])
        self.assertIsInstance(dataset, StreamDataset)
        
def test():
    unittest.main()

if __name__ == "__main__":
    test()
