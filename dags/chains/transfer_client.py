from dataclasses import dataclass, field
from typing import List, Any, Dict, Union, Optional

from mashumaro import DataClassDictMixin

from chains.contracts import EvmContract
from utils.common import dataset_folders, get_list_of_files, read_json_file


@dataclass(frozen=True)
class ClientConfig(DataClassDictMixin):
    load_all: str

    @staticmethod
    def application_args(conf: Dict[str, Any]) -> List[any]:
        result: List[str] = []

        for key, value in conf.items():
            result.append('--' + key.replace('_', '-'))
            result.append(value if type(value) == str else str(value))

        return result


@dataclass(frozen=True)
class DatabricksClientConfig(ClientConfig):
    databricks_server_hostname: str
    databricks_http_path: str
    databricks_port: int
    databricks_personal_access_token: str
    s3_access_key: str
    s3_secret_key: str
    s3_region: str
    s3_bucket: str
    s3_bucket_path_prefix: Optional[str] = None


@dataclass(frozen=True)
class TransferABI(DataClassDictMixin):
    chain: str
    dataset_name: str
    contract_name: str
    abi_name: str
    abi_type: str

    @property
    def upstream_dag_name(self) -> str:
        return f'{self.chain}_parse_{self.dataset_name}_dag'

    @property
    def upstream_task_name(self) -> str:
        return f'{self.chain}.{self.upstream_task_id}'

    @property
    def upstream_task_id(self) -> str:
        return f'{self.dataset_name}.{self.contract_name}_{"call" if self.abi_type == "function" else "evt"}_{self.abi_name}'

    # TODO: optimize
    @property
    def key(self) -> str:
        return f'{self.chain}_{self.dataset_name}_{self.contract_name}_{self.abi_name}_{self.abi_type}'


@dataclass(frozen=True)
class TransferContract(DataClassDictMixin):
    chain: str
    project_name: str
    contract_name: Optional[str] = None

    def get_abis(self) -> List[TransferABI]:
        filter_path = '*.json' if self.contract_name is None else f'{self.contract_name}.json'
        abis: List[TransferABI] = []

        for folder in dataset_folders(self.chain):
            if not folder.endswith(self.project_name):
                continue

            json_files = get_list_of_files(folder, filter_path)
            contracts = [EvmContract.new_instance(read_json_file(json_file)) for json_file in json_files]
            abis.extend([TransferABI(
                chain=self.chain,
                dataset_name=contract.dataset_name,
                contract_name=contract.contract_name,
                abi_name=a.name,
                abi_type=a.type
            ) for contract in contracts for a in contract.abi])

        return abis


@dataclass(frozen=True)
class TransferRawTable(DataClassDictMixin):
    chain: str
    table: str

    @property
    def upstream_dag_name(self) -> str:
        return f'{self.chain}_load_dag'

    @property
    def upstream_task_name(self) -> str:
        return f'{self.chain}.{self.upstream_task_id}'

    @property
    def upstream_task_id(self) -> str:
        return f'enrich_{self.table}'


@dataclass(frozen=True)
class TransferClient(DataClassDictMixin):
    company: str
    config: Union[DatabricksClientConfig]
    raws: List[TransferRawTable] = field(default_factory=list)
    abis: List[TransferABI] = field(default_factory=list)
    contracts: List[TransferContract] = field(default_factory=list)

    @property
    def dag_name(self) -> str:
        return f'transfer_{self.company.lower()}_dag'

    @property
    def application_args(self) -> List[any]:
        config_dict = {k: v for k, v in self.config.to_dict().items() if v is not None}
        return ClientConfig.application_args(config_dict)

    @property
    def all_abis(self) -> List[TransferABI]:
        contract_abi_map = {abi.key: abi for contract in self.contracts for abi in contract.get_abis()}
        for abi in self.abis:
            if abi.key not in contract_abi_map:
                contract_abi_map[abi.key] = abi

        return list(contract_abi_map.values())


@dataclass(frozen=True)
class TransformConfig(DataClassDictMixin):
    clients: List[TransferClient]
