from dotenv import load_dotenv
import os
load_dotenv()

API_ENDPOINT = os.getenv('API_ENDPOINT', "https://stream-dset-client-dev.vercel.app/api")
