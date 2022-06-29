from dataclasses import dataclass
from typing import List, Any, Dict, Union

from mashumaro import DataClassDictMixin


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
    s3_bucket_path_prefix: str


@dataclass(frozen=True)
class TransformABI(DataClassDictMixin):
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


@dataclass(frozen=True)
class TransformRawTable(DataClassDictMixin):
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
class TransformClient(DataClassDictMixin):
    company: str
    raws: List[TransformRawTable]
    abis: List[TransformABI]
    config: Union[DatabricksClientConfig]

    @property
    def dag_name(self) -> str:
        return f'transform_{self.company.lower()}_dag'

    @property
    def application_args(self) -> List[any]:
        return ClientConfig.application_args(self.config.to_dict())


@dataclass(frozen=True)
class TransformConfig(DataClassDictMixin):
    clients: List[TransformClient]
