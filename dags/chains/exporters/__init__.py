from dataclasses import dataclass


@dataclass
class EvmChainMixin:
    chain: str
    provider_uris: str
    export_max_workers: int
    export_batch_size: int
    export_genesis_traces_option: bool = False
    export_daofork_traces_option: bool = False
