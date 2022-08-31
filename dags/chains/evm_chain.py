from itertools import groupby
from typing import List, Optional

from chains.blockchain import Blockchain
from chains.contracts import EvmContract
from chains.exporter import Exporter
from chains.exporters.python import (
    BlocksAndTransactionsPythonExporter,
    TracesPythonExporter,
    ExtractContractsPythonExporter,
    ExtractTokenTransfersPythonExporter,
    ReceiptsAndLogsPythonExporter,
    ExtractTokensPythonExporter,
    GethTracesPythonExporter,
    ExtractContractsFromGethTracesPythonExporter,
)
from chains.loader import Loader
from chains.parser import Parser, EvmParser
from chains.resource_apply import SparkResource
from experiments.contract_service import ContractService
from experiments.experiment_parser import ExperimentEvmParser
from offchains.exporters.prices import PricesExporter
from utils.common import dataset_folders, get_list_of_files, read_json_file


def build_evm_exporters(
        chain: str,
        output_bucket: str,
        export_max_workers: int = 10,
        export_batch_size: int = 10,
        **kwargs
) -> List[Exporter]:
    return [
        BlocksAndTransactionsPythonExporter(
            task_id='export_blocks_and_transactions',
            toggle=kwargs.get('export_blocks_and_transactions_toggle'),
            dependencies=[],
            provider_uris=kwargs.get('provider_uris'),
            export_batch_size=export_batch_size,
            export_max_workers=export_max_workers,
            chain=chain,
            output_bucket=output_bucket,
        ),
        ReceiptsAndLogsPythonExporter(
            task_id='export_receipts_and_logs',
            toggle=kwargs.get('export_receipts_and_logs_toggle'),
            dependencies=['export_blocks_and_transactions'],
            provider_uris=kwargs.get('provider_uris'),
            export_batch_size=export_batch_size,
            export_max_workers=export_max_workers,
            chain=chain,
            output_bucket=output_bucket
        ),
        ExtractTokenTransfersPythonExporter(
            task_id='extract_token_transfers',
            toggle=kwargs.get('extract_token_transfers_toggle'),
            provider_uris=kwargs.get('provider_uris'),
            dependencies=['export_receipts_and_logs'],
            export_batch_size=export_batch_size,
            export_max_workers=export_max_workers,
            chain=chain,
            output_bucket=output_bucket
        ),
        TracesPythonExporter(
            task_id='export_traces',
            toggle=kwargs.get('export_traces_toggle'),
            dependencies=[],
            provider_uris=kwargs.get('provider_uris'),
            export_batch_size=export_batch_size,
            export_max_workers=export_max_workers,
            chain=chain,
            output_bucket=output_bucket,
            export_daofork_traces_option=kwargs.get('export_daofork_traces_option'),
            export_genesis_traces_option=kwargs.get('export_genesis_traces_option')
        ),
        ExtractContractsPythonExporter(
            task_id='extract_contracts',
            toggle=kwargs.get('extract_contracts_toggle'),
            dependencies=['export_traces'],
            provider_uris=kwargs.get('provider_uris'),
            export_batch_size=export_batch_size,
            export_max_workers=export_max_workers,
            chain=chain,
            output_bucket=output_bucket
        ),
        ExtractTokensPythonExporter(
            task_id='extract_tokens',
            toggle=kwargs.get('extract_tokens_toggle'),
            dependencies=['extract_contracts'],
            provider_uris=kwargs.get('provider_uris'),
            export_batch_size=export_batch_size,
            export_max_workers=export_max_workers,
            chain=chain,
            output_bucket=output_bucket
        ),
        PricesExporter(
            task_id='export_prices',
            toggle=kwargs.get('export_prices_toggle'),
            dependencies=[],
            chain=chain,
            output_bucket=output_bucket,
            coinpaprika_auth_key=kwargs.get('coinpaprika_auth_key')
        )
    ]


def build_polygon_exporters(
        chain: str,
        output_bucket: str,
        export_max_workers: int = 10,
        export_batch_size: int = 10,
        **kwargs
) -> List[Exporter]:
    return [
        BlocksAndTransactionsPythonExporter(
            task_id='export_blocks_and_transactions',
            toggle=kwargs.get('export_blocks_and_transactions_toggle'),
            dependencies=[],
            provider_uris=kwargs.get('provider_uris'),
            export_batch_size=export_batch_size,
            export_max_workers=export_max_workers,
            chain=chain,
            output_bucket=output_bucket
        ),
        ReceiptsAndLogsPythonExporter(
            task_id='export_receipts_and_logs',
            toggle=kwargs.get('export_receipts_and_logs_toggle'),
            dependencies=['export_blocks_and_transactions'],
            provider_uris=kwargs.get('provider_uris'),
            export_batch_size=export_batch_size,
            export_max_workers=export_max_workers,
            chain=chain,
            output_bucket=output_bucket
        ),
        ExtractTokenTransfersPythonExporter(
            task_id='extract_token_transfers',
            toggle=kwargs.get('extract_token_transfers_toggle'),
            provider_uris=kwargs.get('provider_uris'),
            dependencies=['export_receipts_and_logs'],
            export_batch_size=export_batch_size,
            export_max_workers=export_max_workers,
            chain=chain,
            output_bucket=output_bucket
        ),
        GethTracesPythonExporter(
            task_id='export_traces',
            toggle=kwargs.get('export_traces_toggle'),
            dependencies=[],
            provider_uris=kwargs.get('provider_uris'),
            export_batch_size=export_batch_size,
            export_max_workers=export_max_workers,
            chain=chain,
            output_bucket=output_bucket
        ),
        ExtractContractsFromGethTracesPythonExporter(
            task_id='extract_contracts',
            toggle=kwargs.get('extract_contracts_toggle'),
            dependencies=['export_traces'],
            provider_uris=kwargs.get('provider_uris'),
            export_batch_size=export_batch_size,
            export_max_workers=export_max_workers,
            chain=chain,
            output_bucket=output_bucket
        ),
        ExtractTokensPythonExporter(
            task_id='extract_tokens',
            toggle=kwargs.get('extract_tokens_toggle'),
            dependencies=['extract_contracts'],
            provider_uris=kwargs.get('provider_uris'),
            export_batch_size=export_batch_size,
            export_max_workers=export_max_workers,
            chain=chain,
            output_bucket=output_bucket
        )
    ]


def build_evm_loaders(chain: str) -> List[Loader]:
    return [
        Loader(chain=chain, name='blocks',
               clean_dependencies=['transactions', 'logs', 'token_transfers', 'traces', 'contracts']),
        Loader(chain=chain, name='transactions', enrich_dependencies=['blocks', 'receipts']),
        Loader(chain=chain, name='receipts', clean_dependencies=['transactions'], enrich_toggle=False),
        Loader(chain=chain, name='logs', enrich_dependencies=['blocks']),
        Loader(chain=chain, name='token_transfers', enrich_dependencies=['blocks']),
        Loader(chain=chain, name='traces', enrich_dependencies=['blocks']),
        Loader(chain=chain, name='contracts', enrich_dependencies=['blocks']),
        Loader(chain=chain, name='tokens'),
        Loader(chain=chain, name='prices', file_format='csv')
    ]


def build_polygon_loaders(chain: str) -> List[Loader]:
    return [
        Loader(chain=chain, name='blocks',
               clean_dependencies=['transactions', 'logs', 'token_transfers', 'geth_traces', 'contracts']),
        Loader(chain=chain, name='transactions', enrich_dependencies=['blocks', 'receipts'],
               clean_dependencies=['geth_traces']),
        Loader(chain=chain, name='receipts', clean_dependencies=['transactions'], enrich_toggle=False),
        Loader(chain=chain, name='logs', enrich_dependencies=['blocks']),
        Loader(chain=chain, name='token_transfers', enrich_dependencies=['blocks']),
        Loader(chain=chain, name='traces', resource='geth_traces', enrich_dependencies=['blocks', 'transactions'],
               enrich_operator_custom_spark_resource=SparkResource(
                   executor_cores=4,
                   executor_memory=10,
                   executor_instances=3,
                   driver_cores=1,
                   driver_memory=2
               )),
        Loader(chain=chain, name='contracts', enrich_dependencies=['blocks']),
        Loader(chain=chain, name='tokens')
    ]


def build_evm_parsers(chain: str) -> List[Parser]:
    parsers: List[Parser] = []
    for folder in dataset_folders(chain):
        json_files = get_list_of_files(folder)
        contracts = [EvmContract.from_contract_dict(read_json_file(json_file)) for json_file in json_files]
        parsers.append(EvmParser(chain, contracts))

    return parsers


def build_evm_experiment_parsers(chain: str) -> List[Parser]:
    contract_service = ContractService()
    all_contracts = [
        contract
        for contract in contract_service.get_contracts_by_chain(chain)
        # TODO: just use uniswap to test
        if contract.project == 'uniswap' or contract.project == 'seaport2'
    ]

    return [ExperimentEvmParser(chain, list(contracts))
            for _, contracts in groupby(all_contracts, lambda x: x.project)]


def build_evm_chain(
        chain: str,
        output_bucket: str,
        export_max_workers: int = 10,
        export_batch_size: int = 10,
        export_schedule_interval: str = '30 0 * * *',
        load_schedule_interval: str = '0 1 * * *',
        parse_schedule_interval: str = '30 1 * * *',
        verify_schedule_interval: str = '30 1 * * *',
        notification_emails: Optional[List[str]] = None,
        **kwargs
) -> Blockchain:
    if chain == 'polygon':
        exporters = build_polygon_exporters(chain, output_bucket, export_max_workers, export_batch_size, **kwargs)
        loaders = build_polygon_loaders(chain)
    else:
        exporters = build_evm_exporters(chain, output_bucket, export_max_workers, export_batch_size, **kwargs)
        loaders = build_evm_loaders(chain)

    parsers = build_evm_parsers(chain)

    experiment_parsers = build_evm_experiment_parsers(chain)

    return Blockchain(
        name=chain,
        exporters=exporters,
        loaders=loaders,
        parsers=parsers,
        experiment_parsers=experiment_parsers,
        export_schedule_interval=export_schedule_interval,
        load_schedule_interval=load_schedule_interval,
        parse_schedule_interval=parse_schedule_interval,
        verify_schedule_interval=verify_schedule_interval,
        notification_emails=notification_emails
    )
