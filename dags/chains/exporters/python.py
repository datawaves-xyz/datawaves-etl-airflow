
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

from polygonetl.cli import (
    export_geth_traces,
    extract_geth_traces
)

from datetime import timedelta, datetime
from typing import Callable, Tuple
from dataclasses import dataclass
from tempfile import TemporaryDirectory
from airflow import DAG
from airflow.operators.python import PythonOperator

from chains.exporter import Exporter, ExporterMixIn
from chains.exporters import EvmChainMixin
from utils.s3_operator import S3Operator

import logging
import os


def export_path(chain: str, directory: str, date: datetime) -> str:
    return f'export/{chain}/{directory}/block_date={date.strftime("%Y-%m-%d")}/'


@dataclass
class EvmChainPythonExporter(EvmChainMixin, ExporterMixIn, Exporter):

    def __post_init__(self):
        self.s3 = S3Operator(aws_conn_id='aws_default', bucket=self.output_bucket)
        self.provider_uri = None

    def gen_export_task(self, dag: DAG) -> None:
        if self.toggle:
            self.operator = PythonOperator(
                task_id=self.task_id,
                python_callable=self.get_fallback_callable,
                provide_context=True,
                execution_timeout=timedelta(hours=15),
                dag=dag
            )
        else:
            self.operator = None

    def export_func(self, logical_date: datetime) -> None:
        raise NotImplementedError()

    def get_fallback_callable(self) -> Callable:
        def python_callable_with_fallback(**kwargs):
            for index, provider_uri in enumerate(self.provider_uris):
                self.provider_uri = provider_uri
                try:
                    self.export_func(**kwargs)
                    break
                except Exception as e:
                    if index < (len(self.provider_uris) - 1):
                        logging.exception('An exception occurred. Trying another uri')
                    else:
                        raise e

        return python_callable_with_fallback

    def get_block_range(self, tempdir: str, date: datetime) -> Tuple[int, int]:
        logging.info(f'Calling get_block_range_for_date({self.provider_uri}, {date}, ...)')
        get_block_range_for_date.callback(
            provider_uri=self.provider_uri,
            date=date,
            output=os.path.join(tempdir, "blocks_meta.txt")
        )

        with open(os.path.join(tempdir, "blocks_meta.txt")) as block_range_file:
            block_range = block_range_file.read()
            start_block, end_block = block_range.split(",")

        return int(start_block), int(end_block)


class BlocksAndTransactionsPythonExporter(EvmChainPythonExporter):

    def export_func(self, logical_date: datetime) -> None:
        with TemporaryDirectory() as tempdir:
            start_block, end_block = self.get_block_range(tempdir, logical_date)

            logging.info('Calling export_blocks_and_transactions({}, {}, {}, {}, {}, ...)'.format(
                start_block, end_block, self.export_batch_size, self.provider_uri, self.export_max_workers))

            export_blocks_and_transactions.callback(
                start_block=start_block,
                end_block=end_block,
                batch_size=self.export_batch_size,
                provider_uri=self.provider_uri,
                max_workers=self.export_max_workers,
                blocks_output=os.path.join(tempdir, "blocks.json"),
                transactions_output=os.path.join(tempdir, "transactions.json"),
            )

            self.s3.copy_to_export_path(os.path.join(tempdir, "blocks_meta.txt"),
                                        export_path(self.chain, "blocks_meta", logical_date))
            self.s3.copy_to_export_path(os.path.join(tempdir, "blocks.json"),
                                        export_path(self.chain, "blocks", logical_date))
            self.s3.copy_to_export_path(os.path.join(tempdir, "transactions.json"),
                                        export_path(self.chain, "transactions", logical_date))


class ReceiptsAndLogsPythonExporter(EvmChainPythonExporter):

    def export_func(self, logical_date: datetime) -> None:
        with TemporaryDirectory() as tempdir:
            self.s3.copy_from_export_path(export_path(self.chain, "transactions", logical_date),
                                          os.path.join(tempdir, "transactions.json"))

            logging.info('Calling extract_csv_column(...)')
            extract_field.callback(
                input=os.path.join(tempdir, "transactions.json"),
                output=os.path.join(tempdir, "transaction_hashes.txt"),
                field="hash",
            )

            logging.info('Calling export_receipts_and_logs({}, ..., {}, {}, ...)'.format(
                self.export_batch_size, self.provider_uri, self.export_max_workers))

            export_receipts_and_logs.callback(
                batch_size=self.export_batch_size,
                transaction_hashes=os.path.join(tempdir, "transaction_hashes.txt"),
                provider_uri=self.provider_uri,
                max_workers=self.export_max_workers,
                receipts_output=os.path.join(tempdir, "receipts.json"),
                logs_output=os.path.join(tempdir, "logs.json"),
            )

            self.s3.copy_to_export_path(os.path.join(tempdir, "receipts.json"),
                                        export_path(self.chain, "receipts", logical_date))
            self.s3.copy_to_export_path(os.path.join(tempdir, "logs.json"),
                                        export_path(self.chain, "logs", logical_date))


class ExtractContractsPythonExporter(EvmChainPythonExporter):
    def export_func(self, logical_date: datetime) -> None:
        with TemporaryDirectory() as tempdir:

            self.s3.copy_from_export_path(export_path(self.chain, "trace", logical_date),
                                          os.path.join(tempdir, "traces.json"))

            logging.info('Calling extract_contracts(..., {}, {})'.format(
                self.export_batch_size, self.export_max_workers
            ))

            extract_contracts.callback(
                traces=os.path.join(tempdir, "traces.json"),
                output=os.path.join(tempdir, "contracts.json"),
                batch_size=self.export_batch_size,
                max_workers=self.export_max_workers,
            )

            self.s3.copy_to_export_path(os.path.join(tempdir, "contracts.json"),
                                        export_path(self.chain, "contracts", logical_date))


class ExtractTokensPythonExporter(EvmChainPythonExporter):
    def export_func(self, logical_date: datetime) -> None:

        with TemporaryDirectory() as tempdir:
            self.s3.copy_from_export_path(export_path(self.chain, "contracts", logical_date),
                                          os.path.join(tempdir, "contracts.json"))

            logging.info('Calling extract_tokens(..., {}, {})'.format(
                self.export_max_workers, self.provider_uri
            ))

            extract_tokens.callback(
                contracts=os.path.join(tempdir, "contracts.json"),
                output=os.path.join(tempdir, "tokens.json"),
                max_workers=self.export_max_workers,
                provider_uri=self.provider_uri,
                values_as_strings=True,
            )

            self.s3.copy_to_export_path(os.path.join(tempdir, "tokens.json"),
                                        export_path(self.chain, "tokens", logical_date))


class ExtractTokenTransfersPythonExporter(EvmChainPythonExporter):
    def export_func(self, logical_date: datetime) -> None:
        with TemporaryDirectory() as tempdir:
            self.s3.copy_from_export_path(export_path(self.chain, "logs", logical_date),
                                          os.path.join(tempdir, "logs.json"))

            logging.info('Calling extract_token_transfers(..., {}, ..., {})'.format(
                self.export_batch_size, self.export_max_workers
            ))
            extract_token_transfers.callback(
                logs=os.path.join(tempdir, "logs.json"),
                batch_size=self.export_batch_size,
                output=os.path.join(tempdir, "token_transfers.json"),
                max_workers=self.export_max_workers,
                values_as_strings=True,
            )

            self.s3.copy_to_export_path(os.path.join(tempdir, "token_transfers.json"),
                                        export_path(self.chain, "token_transfers", logical_date))


class TracesPythonExporter(EvmChainPythonExporter):
    def export_func(self, logical_date: datetime) -> None:
        with TemporaryDirectory() as tempdir:
            start_block, end_block = self.get_block_range(tempdir, logical_date)

            logging.info('Calling export_traces({}, {}, {}, ..., {}, {}, {}, {})'.format(
                start_block, end_block, self.export_batch_size, self.export_max_workers, self.provider_uri,
                self.export_genesis_traces_option, self.export_daofork_traces_option
            ))
            export_traces.callback(
                start_block=start_block,
                end_block=end_block,
                batch_size=self.export_batch_size,
                output=os.path.join(tempdir, "traces.json"),
                max_workers=self.export_max_workers,
                provider_uri=self.provider_uri,
                genesis_traces=self.export_genesis_traces_option,
                daofork_traces=self.export_daofork_traces_option,
            )

            self.s3.copy_to_export_path(os.path.join(tempdir, "traces.json"),
                                        export_path(self.chain, "traces", logical_date))


class GethTracesPythonExporter(EvmChainPythonExporter):
    def export_func(self, logical_date: datetime) -> None:
        with TemporaryDirectory() as tempdir:
            start_block, end_block = self.get_block_range(tempdir, logical_date)

            logging.info('Calling export_geth_traces({}, {}, {}, ..., {}, {})'.format(
                start_block, end_block, self.export_batch_size, self.export_max_workers, self.provider_uri
            ))

            export_geth_traces.callback(
                start_block=start_block,
                end_block=end_block,
                batch_size=self.export_batch_size,
                output=os.path.join(tempdir, "geth_traces_temp.json"),
                max_workers=self.export_max_workers,
                provider_uri=self.provider_uri
            )

            logging.info('Calling extract_geth_traces({}, ..., {})'.format(
                self.export_batch_size, self.export_max_workers
            ))

            extract_geth_traces.callback(
                input=os.path.join(tempdir, "geth_traces_temp.json"),
                output=os.path.join(tempdir, "geth_traces.json"),
                max_workers=1,
                batch_size=1
            )

            self.s3.copy_to_export_path(os.path.join(tempdir, "geth_traces.json"),
                                        export_path(self.chain, "geth_traces", logical_date))


class ExtractContractsFromGethTracesPythonExporter(EvmChainPythonExporter):
    def export_func(self, logical_date: datetime) -> None:
        with TemporaryDirectory() as tempdir:

            self.s3.copy_from_export_path(export_path(self.chain, "geth_trace", logical_date),
                                          os.path.join(tempdir, "geth_traces.json"))

            logging.info('Calling extract_contracts(..., {}, {})'.format(
                self.export_batch_size, self.export_max_workers
            ))

            extract_contracts.callback(
                traces=os.path.join(tempdir, "geth_traces.json"),
                output=os.path.join(tempdir, "contracts.json"),
                batch_size=self.export_batch_size,
                max_workers=self.export_max_workers,
            )

            self.s3.copy_to_export_path(os.path.join(tempdir, "contracts.json"),
                                        export_path(self.chain, "contracts", logical_date))