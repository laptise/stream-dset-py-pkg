import threading
from queue import Queue
from typing import Any, Literal
import requests
import json
import os
from torch.utils.data import IterableDataset
import multiprocessing
from .constant import API_ENDPOINT
from multiprocessing import Manager
from concurrent.futures import as_completed
from multiprocessing import Pool


def stream_file(filename, chunk_size=200):
    with open(filename, 'rb') as file:
        while True:
            chunk = file.read(chunk_size)
            if not chunk:
                break
            yield chunk

def send_file(url, filename):
    with requests.post(url, data=stream_file(url, filename)) as response:
        print(response.text)

def download_file(row: tuple[dict, dict, str, Queue[Any]]):
    data, meta, datasetdir, m_queue = row
    parsed = {}
    for value_key in data:
        value = data[value_key]
        if isinstance(value, list):
            dtype, *_ =  value
            if dtype == "file":
                _, presigned_url, filename = value
                tosave_dir = os.path.join(
                    datasetdir, 
                    str(meta['id']),
                    value_key
                )
                tosave_path = os.path.join(tosave_dir, filename)
                if os.path.exists(tosave_path):
                    parsed[value_key] = ['file',tosave_path]
                    continue
                r = requests.get(presigned_url, allow_redirects=True)
                os.makedirs(tosave_dir, exist_ok=True)
                with open(tosave_path, "wb") as f:
                    f.write(r.content)
            parsed[value_key] = ['file',tosave_path]
        else:
            parsed[value_key] = value
    print("r")
    m_queue.put(parsed)

class SDCredential:
    def __init__(self, access_key_id:str, access_key_secret:str):
        self.access_key_id = access_key_id
        self.access_key_secret = access_key_secret
    
    def get_tuple(self):
        return (self.access_key_id, self.access_key_secret)

class SDColumn:
    def __init__(self, name:str, type:str):
        self.name = name
        self.type = type
    
    @staticmethod
    def _from_api_response(response):
        column = SDColumn(
            response['name'],
            response['type'],
        )
        return column

    def __dict__(self):
        return {
            'name': self.name,
            'type': self.type,
        }
    
    def __repr__(self) -> str:
        return f"<SDColumn:{self.type}> {self.name}"

class StreamDataset(IterableDataset):

    def __init__(
        self, 
        id:int, 
        name:str, 
        credential:SDCredential,
        batch_size:int | None = None,
        row_counts:int = 0,
        columns:list[SDColumn] = [], 
        temp_dir: str = os.getcwd()
    ):
        self.id = id
        self.name = name
        self.columns = columns
        self.rows = []
        self.credential = credential
        self.row_counts = row_counts
        self.temp_dir = temp_dir
        self.batch_size = batch_size
        self._resolved_rows = []
    
    @staticmethod
    def _from_api_response(response, credential:SDCredential, temp_dir: str = os.getcwd()):
        data = response['data']
        dataset = StreamDataset(
            data['id'],
            data['name'],
            credential=credential,
            row_counts=response['count'],
            temp_dir=temp_dir
        )
        columns = data['columns']
        columns = json.loads(columns)
        columns = [SDColumn._from_api_response(column) for column in columns]
        dataset.columns = columns
        return dataset

    def _resolve_payload(self, payload:dict, columns:list[SDColumn]):
        column_names = [column.name for column in columns]
        not_defined_columns = list(filter(lambda key: key not in column_names, payload.keys()))
        if len(not_defined_columns) > 0:
            raise Exception(f"Invalud value provided. Columns {not_defined_columns} are not defined in the dataset")
        not_provided_columns = list(filter(lambda column: column.name not in payload.keys(), columns))
        if len(not_provided_columns) > 0:
            raise Exception(f"Missing values for columns {not_provided_columns}")
        file_columns = list(filter(lambda column: column.type == 'file', columns))
        data = {}
        for column in file_columns:
            provided = payload[column.name]
            if isinstance(provided, str):
                filename = os.path.basename(provided)
                if not os.path.exists(provided):
                    raise Exception(f"File {provided} does not exist")
                presinged_resp = requests.get(
                    f"{API_ENDPOINT}/datasets/{self.id}/presigned", 
                    params={'filename': filename},
                    auth=(self.credential.get_tuple())
                )
                presinged_resp = presinged_resp.json()
                url = presinged_resp['signedUrl']
                file = open(provided, 'rb')
                resp = requests.put(url, data=file)
                file.close()
                resp = resp.json()
                filekey = resp['Key']
                data[column.name] = ['file', filekey, filename]
        other_columns = list(filter(lambda column: column.type != 'file', columns))
        for column in other_columns:
            data[column.name] = payload[column.name]
        return data
    
    def push_row(self, body:dict):
        resolved = self._resolve_payload(body, self.columns)
        resp = requests.post(f"{API_ENDPOINT}/datasets/{self.id}/rows", json=resolved, auth=(self.credential.get_tuple()))
        resp = resp.json()
        self.row_counts = resp
        print("Row pushed successfully")

    def with_batch(self, batch_size:int):
        self.batch_size = batch_size
        return self

    def _batch_download_list(self, datalist:list, queue:Queue):
        manager = Manager()
        m_queue = manager.Queue()  # multiprocessing.Queue for process-safe communication
        datasetdir = os.path.join(self.temp_dir, str(self.id))
        collated = list(map(lambda data: (data[0], data[1], datasetdir, m_queue), datalist))
        with Pool(16) as pool:
            pool.map(download_file, collated)
        while not m_queue.empty():
            queue.put(m_queue.get())

    def _prepare_all(self, initial_target:str, queue: Queue):
        target = initial_target
        while target:
            try:
                resp = requests.get(target, auth=(self.credential.get_tuple()))
                resp = resp.json()
                data_list = resp['data']
                self._batch_download_list(data_list, queue)
                target = resp['next']
            except Exception as e:
                print(e)
                break
        print("Downloaded all data")
        

    def __repr__(self) -> str:
        return f"name: {self.name}\n" + \
            f"id: {self.id}\n" + \
            f"  rows: {self.row_counts}\n" + \
            f"  columns:\n" + \
            "\n".join([f"    - {column}" for column in self.columns]) 
    
    def __iter__(self):
        queue = Queue()
        thread = threading.Thread(target=self._prepare_all, args=(f"{API_ENDPOINT}/datasets/{self.id}/rows", queue))
        thread.start()
        
        batch = []  # バッチデータを保持するリスト
        while not queue.empty() or thread.is_alive():
            parsed = queue.get()
            if self.batch_size is None:  # バッチサイズが None の場合、各アイテムを直接返す
                yield parsed
            else:
                batch.append(parsed)  # バッチにデータを追加
                if len(batch) == self.batch_size:  # バッチが指定サイズに達したら
                    yield batch
                    batch = []  # バッチをリセット
        if batch:  # バッチサイズが設定されていて、バッチに残っているデータがあれば、最後にそれらを返す
            yield batch

    
    def __len__(self):
        return self.row_counts

class ColumnDefinition:
    def __init__(self, name:str, type:Literal['file', 'string', 'number']):
        self.name = name
        self.type = type
    
    def __dict__(self):
        return {
            'name': self.name,
            'type': self.type,
        }
    
    def __repr__(self) -> str:
        return f"<ColumnDefinition:{self.type}> {self.name}"

class StreamDsetClient:
    def __init__(self, access_key_id:str, access_key_secret:str, temp_dir: str = os.getcwd()):
        self.credential = SDCredential(access_key_id, access_key_secret)
        self.temp_dir = temp_dir
        res = requests.get(f"{API_ENDPOINT}", auth=(self.credential.get_tuple()))
        if res.status_code != 200:
            raise Exception("API is not available")
    
    def get_datasets(self):
        res = requests.get(f"{API_ENDPOINT}/datasets", auth=(self.credential.get_tuple()))
        res = res.json()
        return [
            StreamDataset._from_api_response(
                dataset, 
                self.credential, 
                temp_dir=self.temp_dir
            ) for dataset in res
        ]
    
    def get_dataset(self, dataset_id:int):
        try:
            res = requests.get(f"{API_ENDPOINT}/datasets/{dataset_id}", auth=(self.credential.get_tuple())).json()
            return StreamDataset._from_api_response(
                res, 
                self.credential,
                temp_dir=self.temp_dir
            )
        except:
            raise Exception("Dataset not found")
    
    def create_dataset(self, name:str, columns:list[ColumnDefinition]):
        cols = []
        for column in columns:
            cols.append({
                "name": column.name,
                "type": column.type
            })
        payload = { 'name': name, 'columns': cols }
        resp = requests.post(
            f"{API_ENDPOINT}/datasets", 
            json=payload, 
            auth=(self.credential.get_tuple())
        )
        resp = resp.json()
        return self.get_dataset(resp['id'])

