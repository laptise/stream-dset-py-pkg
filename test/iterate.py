from dotenv import load_dotenv
import os
import unittest
from strdset import StreamDsetClient
import shutil
load_dotenv()

class DatasetTest(unittest.TestCase):
    def setUp(self):
        tempdir = "temp"
        # shutil.rmtree("/workspaces/stream-dset-py-pkg/temp")
        self.client = StreamDsetClient(
            os.environ.get("TEST_ACCESS_KEY_ID", ""),
            os.environ.get("TEST_ACCESS_KEY_SECRET",""),
            temp_dir=tempdir
        )
    
    def test_iterate(self):
        dataset = self.client.get_dataset(8).with_batch(6)
        generator = iter(dataset)
        iterated_count = 0
        for _ in generator:
            print("iter", type(_))
            iterated_count += 1
        # self.assertTrue(iterated_count == dataset.row_counts)
            
def test():
    unittest.main()

if __name__ == "__main__":
    test()
