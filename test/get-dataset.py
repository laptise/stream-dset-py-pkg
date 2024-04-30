from dotenv import load_dotenv
import os
load_dotenv()

from streamdset import StreamDsetClient

client = StreamDsetClient(
    os.environ.get("TEST_ACCESS_KEY_ID", ""),
    os.environ.get("TEST_ACCESS_KEY_SECRET","")
)

# datasets = client.get_datasets()
# print(datasets)
# print(datasets)

dataset = client.get_dataset(1)
dataset.push_row({
    "music_name": "a",
    "x": "/workspaces/stream-dset-py-pkg/dummy/Flow.wav",
    'y': '/workspaces/stream-dset-py-pkg/dummy/Hot-Mess.wav',
})

# generator = iter(dataset)
# for i in generator:
#     print(i)
