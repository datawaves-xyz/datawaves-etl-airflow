from dataclasses import dataclass, field
from typing import Optional, List, Union

from mashumaro import DataClassDictMixin


@dataclass(frozen=True)
class ContractElement(DataClassDictMixin):
    name: Optional[str] = None
    type: Optional[str] = None


@dataclass(frozen=True)
class Contract(DataClassDictMixin):
    dataset_name: str
    contract_name: str
    contract_address: Optional[str] = None


@dataclass(frozen=True)
class EvmAbiEventElement(DataClassDictMixin):
    indexed: bool
    name: str
    type: str
    components: Optional[List['EvmAbiEventElement']] = None


@dataclass(frozen=True)
class EvmAbiFunctionElement(DataClassDictMixin):
    name: str
    type: str
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


@dataclass(frozen=True)
class EvmContract(Contract):
    abi: List[EvmAbiElement] = field(default_factory=list)
