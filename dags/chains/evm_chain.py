import logging
import os
from tempfile import TemporaryDirectory
from typing import List, Tuple, Optional

from ethereumetl.cli import (
    get_block_range_for_date,
    export_blocks_and_transactions,
    extract_field,
    export_receipts_and_logs,
    extract_contracts,
    extract_tokens,
    extract_token_transfers,
    export_traces
)
from pendulum import datetime

from chains.blockchain import Blockchain
from chains.contracts import EvmContract
from chains.exporter import Exporter
from chains.loader import Loader
from chains.parser import Parser, EvmParser
from offchains.prices import CoinpaprikaPriceProvider
from utils.common import dataset_folders, get_list_of_files, read_json_file
from utils.s3_operator import S3Operator


def build_evm_exporters(
        chain: str,
        output_bucket: str,
        export_max_workers: int = 10,
        export_batch_size: int = 10,
        **kwargs
) -> List[Exporter]:
    s3 = S3Operator(aws_conn_id='aws_default', bucket=output_bucket)
    ep = Exporter.export_folder_path
    export_daofork_traces_option = kwargs.get('export_daofork_traces_option')
    export_genesis_traces_option = kwargs.get('export_genesis_traces_option')

    def get_block_range(tempdir: str, date: datetime, provider_uri: str) -> Tuple[int, int]:
        logging.info(f'Calling get_block_range_for_date({provider_uri}, {date}, ...)')
        get_block_range_for_date.callback(
            provider_uri=provider_uri, date=date, output=os.path.join(tempdir, "blocks_meta.txt")
        )

        with open(os.path.join(tempdir, "blocks_meta.txt")) as block_range_file:
            block_range = block_range_file.read()
            start_block, end_block = block_range.split(",")

        return int(start_block), int(end_block)

    def export_blocks_and_transactions_command(logical_date: datetime, provider_uri: str, **kwargs) -> None:
        with TemporaryDirectory() as tempdir:
            start_block, end_block = get_block_range(tempdir, logical_date, provider_uri)

            logging.info('Calling export_blocks_and_transactions({}, {}, {}, {}, {}, ...)'.format(
                start_block, end_block, export_batch_size, provider_uri, export_max_workers))

            export_blocks_and_transactions.callback(
                start_block=start_block,
                end_block=end_block,
                batch_size=export_batch_size,
                provider_uri=provider_uri,
                max_workers=export_max_workers,
                blocks_output=os.path.join(tempdir, "blocks.json"),
                transactions_output=os.path.join(tempdir, "transactions.json"),
            )

            s3.copy_to_export_path(os.path.join(tempdir, "blocks_meta.txt"), ep(chain, "blocks_meta", logical_date))
            s3.copy_to_export_path(os.path.join(tempdir, "blocks.json"), ep(chain, "blocks", logical_date))
            s3.copy_to_export_path(os.path.join(tempdir, "transactions.json"), ep(chain, "transactions", logical_date))

    def export_receipts_and_logs_command(logical_date: datetime, provider_uri: str, **kwargs) -> None:
        with TemporaryDirectory() as tempdir:
            s3.copy_from_export_path(ep(chain, "transactions", logical_date),
                                     os.path.join(tempdir, "transactions.json"))

            logging.info('Calling extract_csv_column(...)')
            extract_field.callback(
                input=os.path.join(tempdir, "transactions.json"),
                output=os.path.join(tempdir, "transaction_hashes.txt"),
                field="hash",
            )

            logging.info('Calling export_receipts_and_logs({}, ..., {}, {}, ...)'.format(
                export_batch_size, provider_uri, export_max_workers))

            export_receipts_and_logs.callback(
                batch_size=export_batch_size,
                transaction_hashes=os.path.join(tempdir, "transaction_hashes.txt"),
                provider_uri=provider_uri,
                max_workers=export_max_workers,
                receipts_output=os.path.join(tempdir, "receipts.json"),
                logs_output=os.path.join(tempdir, "logs.json"),
            )

            s3.copy_to_export_path(os.path.join(tempdir, "receipts.json"), ep(chain, "receipts", logical_date))
            s3.copy_to_export_path(os.path.join(tempdir, "logs.json"), ep(chain, "logs", logical_date))

    def extract_contracts_command(logical_date: datetime, **kwargs) -> None:
        with TemporaryDirectory() as tempdir:
            s3.copy_from_export_path(ep(chain, "traces", logical_date), os.path.join(tempdir, "traces.json"))

            logging.info('Calling extract_contracts(..., {}, {})'.format(
                export_batch_size, export_max_workers
            ))

            extract_contracts.callback(
                traces=os.path.join(tempdir, "traces.json"),
                output=os.path.join(tempdir, "contracts.json"),
                batch_size=export_batch_size,
                max_workers=export_max_workers,
            )

            s3.copy_to_export_path(os.path.join(tempdir, "contracts.json"), ep(chain, "contracts", logical_date))

    def extract_tokens_command(logical_date: datetime, provider_uri: str, **kwargs):
        with TemporaryDirectory() as tempdir:
            s3.copy_from_export_path(ep(chain, "contracts", logical_date), os.path.join(tempdir, "contracts.json"))

            logging.info('Calling extract_tokens(..., {}, {})'.format(export_max_workers, provider_uri))

            extract_tokens.callback(
                contracts=os.path.join(tempdir, "contracts.json"),
                output=os.path.join(tempdir, "tokens.json"),
                max_workers=export_max_workers,
                provider_uri=provider_uri,
                values_as_strings=True,
            )

            s3.copy_to_export_path(os.path.join(tempdir, "tokens.json"), ep(chain, "tokens", logical_date))

    def extract_token_transfers_command(logical_date: datetime, **kwargs):
        with TemporaryDirectory() as tempdir:
            s3.copy_from_export_path(ep(chain, "logs", logical_date), os.path.join(tempdir, "logs.json"))

            logging.info('Calling extract_token_transfers(..., {}, ..., {})'.format(
                export_batch_size, export_max_workers
            ))
            extract_token_transfers.callback(
                logs=os.path.join(tempdir, "logs.json"),
                batch_size=export_batch_size,
                output=os.path.join(tempdir, "token_transfers.json"),
                max_workers=export_max_workers,
                values_as_strings=True,
            )

            s3.copy_to_export_path(os.path.join(tempdir, "token_transfers.json"),
                                   ep(chain, "token_transfers", logical_date))

    def export_traces_command(logical_date: datetime, provider_uri: str, **kwargs):
        with TemporaryDirectory() as tempdir:
            start_block, end_block = get_block_range(tempdir, logical_date, provider_uri)

            logging.info('Calling export_traces({}, {}, {}, ...,{}, {}, {}, {})'.format(
                start_block, end_block, export_batch_size, export_max_workers, provider_uri,
                export_genesis_traces_option, export_daofork_traces_option
            ))
            export_traces.callback(
                start_block=start_block,
                end_block=end_block,
                batch_size=export_batch_size,
                output=os.path.join(tempdir, "traces.json"),
                max_workers=export_max_workers,
                provider_uri=provider_uri,
                genesis_traces=export_genesis_traces_option,
                daofork_traces=export_daofork_traces_option,
            )

            s3.copy_to_export_path(os.path.join(tempdir, "traces.json"), ep(chain, "traces", logical_date))

    def export_prices_command(logical_date: datetime, **kwargs):
        with TemporaryDirectory() as tempdir:
            start_ts = int(logical_date.start_of('day').timestamp())
            end_ts = int(logical_date.start_of('day').timestamp())

            logging.info('Calling export_prices({}, {})'.format(start_ts, end_ts))

            prices_provider = CoinpaprikaPriceProvider(auth_key=kwargs.get('coinpaprika_auth_key'))
            prices_provider.create_temp_csv(
                output_path=os.path.join(tempdir, "prices.csv"),
                start=start_ts,
                end=end_ts
            )

            s3.copy_to_export_path(os.path.join(tempdir, "prices.csv"), ep(chain, "prices", logical_date))

    return [
        Exporter(
            task_id='export_blocks_and_transactions',
            toggle=kwargs.get('export_blocks_and_transactions_toggle'),
            export_callable=export_blocks_and_transactions_command,
        ),
        Exporter(
            task_id='export_receipts_and_logs',
            toggle=kwargs.get('export_receipts_and_logs_toggle'),
            export_callable=export_receipts_and_logs_command,
            dependencies=['export_blocks_and_transactions']
        ),
        Exporter(
            task_id='extract_token_transfers',
            toggle=kwargs.get('extract_token_transfers_toggle'),
            export_callable=extract_token_transfers_command,
            dependencies=['export_receipts_and_logs']
        ),
        Exporter(
            task_id='export_traces',
            toggle=kwargs.get('export_traces_toggle'),
            export_callable=export_traces_command,
        ),
        Exporter(
            task_id='extract_contracts',
            toggle=kwargs.get('extract_contracts_toggle'),
            export_callable=extract_contracts_command,
            dependencies=['export_traces']
        ),
        Exporter(
            task_id='extract_tokens',
            toggle=kwargs.get('extract_tokens_toggle'),
            export_callable=extract_tokens_command,
            dependencies=['extract_contracts']
        ),
        Exporter(
            task_id='export_prices',
            toggle=kwargs.get('export_prices_toggle'),
            export_callable=export_prices_command,
            off_chain=True
        )
    ]


def build_evm_loaders(chain: str) -> List[Loader]:
    return [
        Loader(chain=chain, resource='blocks',
               clean_dependencies=['transactions', 'logs', 'token_transfers', 'traces', 'contracts']),
        Loader(chain=chain, resource='transactions', enrich_dependencies=['blocks', 'receipts']),
        Loader(chain=chain, resource='receipts', clean_dependencies=['transactions'], enrich_toggle=False),
        Loader(chain=chain, resource='logs', enrich_dependencies=['blocks']),
        Loader(chain=chain, resource='token_transfers', enrich_dependencies=['blocks']),
        Loader(chain=chain, resource='traces', enrich_dependencies=['blocks']),
        Loader(chain=chain, resource='contracts', enrich_dependencies=['blocks']),
        Loader(chain=chain, resource='tokens'),
        Loader(chain=chain, resource='prices', file_format='csv')
    ]


def build_evm_parsers(chain: str) -> List[Parser]:
    parsers: List[Parser] = []
    for folder in dataset_folders(chain):
        json_files = get_list_of_files(folder)
        contracts = [EvmContract.from_dict(read_json_file(json_file)) for json_file in json_files]
        parsers.append(EvmParser(chain, contracts))

    return parsers


def build_evm_chain(
        chain: str,
        output_bucket: str,
        export_max_workers: int = 10,
        export_batch_size: int = 10,
        export_schedule_interval: str = '30 0 * * *',
        load_schedule_interval: str = '0 1 * * *',
        parse_schedule_interval: str = '30 1 * * *',
        notification_emails: Optional[List[str]] = None,
        **kwargs
) -> Blockchain:
    exporters = build_evm_exporters(chain, output_bucket, export_max_workers, export_batch_size, **kwargs)
    loaders = build_evm_loaders(chain)
    parsers = build_evm_parsers(chain)

    return Blockchain(
        name=chain,
        exporters=exporters,
        loaders=loaders,
        parsers=parsers,
        export_schedule_interval=export_schedule_interval,
        load_schedule_interval=load_schedule_interval,
        parse_schedule_interval=parse_schedule_interval,
        notification_emails=notification_emails
    )
