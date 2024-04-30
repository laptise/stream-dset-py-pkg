from dotenv import load_dotenv
import os
load_dotenv()

from streamdset import StreamDsetClient

client = StreamDsetClient(
    os.environ["TEST_ACCESS_KEY_ID"],
    os.environ["TEST_ACCESS_KEY_SECRET"]
)

dataset = client.get_dataset(3)
# dataset.push_row({
#     "music name": "a",
#     "x": "/Users/yoonsookim/Library/CloudStorage/GoogleDrive-ml.laptise@gmail.com/マイドライブ/audio/training/mix.wav",
#     'y': '/Users/yoonsookim/Library/CloudStorage/GoogleDrive-ml.laptise@gmail.com/マイドライブ/audio/training/mix.wav',
# })

generator = iter(dataset)
for i in generator:
    print(i)
