from strdset import StreamDsetClient
import os
import unittest

class DatasetTest(unittest.TestCase):
    def setUp(self):
        tempdir = "temp"
        # shutil.rmtree("/workspaces/stream-dset-py-pkg/temp")
        self.client = StreamDsetClient(
            os.environ.get("TEST_ACCESS_KEY_ID", ""),
            os.environ.get("TEST_ACCESS_KEY_SECRET",""),
            temp_dir=tempdir
        )
    
    def test_csv(self):
        dataset = self.client.get_dataset(8)
        ds = dataset.to_datasets()
        print(ds)

            
def test():
    unittest.main()

if __name__ == "__main__":
    test()
