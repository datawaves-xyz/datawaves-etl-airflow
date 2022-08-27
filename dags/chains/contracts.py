from dataclasses import dataclass, field
from typing import Optional, List, Union

from mashumaro import DataClassDictMixin


@dataclass(frozen=True)
class ContractElement(DataClassDictMixin):
    name: Optional[str] = None
    type: Optional[str] = None


@dataclass
class Contract(DataClassDictMixin):
    dataset_name: str = ""
    contract_name: str = ""
    contract_address: Optional[str] = None


@dataclass(frozen=True)
class EvmAbiEventElement(DataClassDictMixin):
    name: str
    type: str
    indexed: bool = False
    internalType: Optional[str] = None
    components: Optional[List['EvmAbiEventElement']] = None


@dataclass(frozen=True)
class EvmAbiFunctionElement(DataClassDictMixin):
    name: str
    type: str
    internalType: Optional[str] = None
    components: Optional[List['EvmAbiFunctionElement']] = None


@dataclass(frozen=True)
class EvmAbiEvent(ContractElement):
    inputs: List[EvmAbiEventElement] = field(default_factory=list)
    anonymous: Optional[bool] = None


@dataclass(frozen=True)
class EvmAbiFunction(ContractElement):
    inputs: List[EvmAbiFunctionElement] = field(default_factory=list)
    outputs: List[EvmAbiFunctionElement] = field(default_factory=list)
    constant: Optional[bool] = None
    payable: Optional[bool] = None
    stateMutability: Optional[str] = None


EvmAbiElement = Union[EvmAbiEvent, EvmAbiFunction]


@dataclass
class EvmContract(Contract):
    abi: List[EvmAbiElement] = field(default_factory=list)

    @staticmethod
    def from_contract_dict(obj: dict) -> 'EvmContract':
        abi: List[EvmAbiElement] = []

        for element in obj.get('abi', []):
            if element.get('type') == 'function':
                abi.append(EvmAbiFunction.from_dict(element))
            elif element.get('type') == 'event':
                abi.append(EvmAbiEvent.from_dict(element))

        return EvmContract(
            dataset_name=obj.get('dataset_name'),
            contract_name=obj.get('contract_name'),
            contract_address=obj.get('contract_address'),
            abi=abi
        )
