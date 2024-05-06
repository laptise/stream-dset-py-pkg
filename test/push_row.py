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
    
    def test_push_row(self):
        dataset = self.client.get_dataset(9)
        before = dataset.row_counts 
        dataset.push_row({
            "music name": "a",
            'file': '/workspaces/stream-dset-py-pkg/dummy/1-08 Cancer.mp3',
        })
        after = dataset.row_counts
        self.assertTrue(before < after)

def test():
    unittest.main()
    
if __name__ == "__main__":
    test()
