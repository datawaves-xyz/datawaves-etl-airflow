import json
import os
from glob import glob
from typing import Any, Dict, List


def read_json_file(filepath: str) -> Dict[str, Any]:
    with open(filepath) as file_handle:
        content = file_handle.read()
        return json.loads(content)


def read_file(filepath):
    with open(filepath) as file_handle:
        content = file_handle.read()
        return content


def dags_folder() -> str:
    return os.environ.get('DAGS_FOLDER', '/opt/airflow/dags/repo/dags')


def dataset_folders(chain: str) -> List[str]:
    contracts_folder = os.path.join(dags_folder(), f'resources/contracts/{chain}/*')
    return glob(contracts_folder)


def get_list_of_files(dataset_folder: str, filter: str = '*.json') -> List[str]:
    return [f for f in glob(os.path.join(dataset_folder, filter))]
