import json
from dataclasses import dataclass
from typing import Optional, List, Tuple

import requests

from chains.contracts import Contract


@dataclass
class ContractDTO(Contract):
    id: str = ""
    chain: str = ""
    name: str = ""
    project: str = ""
    type: str = ""
    abi: Optional[str] = None
    address: Optional[str] = None

    def __post_init__(self):
        self.contract_name = self.name
        self.contract_address = self.address
        self.dataset_name = self.project

    def abi_element(self) -> List[Tuple[str, str]]:
        if self.abi is not None:
            return [(element.get('type'), element.get('name')) for element in json.loads(self.abi)]
        else:
            return []


class ContractService:
    endpoint = "http://datawaves-etl-service.tellery-prod.svc.cluster.local:8080/contract"

    def get_contracts_by_chain(self, chain: str) -> List[ContractDTO]:
        res = requests.get(f"{self.endpoint}/chain/{chain}")

        if not str(res.status_code).startswith('2'):
            raise Exception(f'Get contracts by chain failed: {chain}')

        return [ContractDTO.from_dict(item) for item in res.json()['data']]
