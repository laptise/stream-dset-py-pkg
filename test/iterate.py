from dotenv import load_dotenv
import os
import unittest
from strdset import StreamDsetClient
load_dotenv()

class DatasetTest(unittest.TestCase):
    def setUp(self):
        tempdir = "temp"
        self.client = StreamDsetClient(
            os.environ.get("TEST_ACCESS_KEY_ID", ""),
            os.environ.get("TEST_ACCESS_KEY_SECRET",""),
            temp_dir=tempdir
        )
    
    def test_iterate(self):
        dataset = self.client.get_dataset(9)
        # dataset.with_batch(2)
        generator = iter(dataset)
        iterated_count = 0
        for _ in generator:
            print(len(_))
            iterated_count += 1
        # self.assertTrue(iterated_count == dataset.row_counts)
            
def test():
    unittest.main()

if __name__ == "__main__":
    test()
