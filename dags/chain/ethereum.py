import logging
import os
from datetime import datetime
from tempfile import TemporaryDirectory
from typing import List, Tuple, Optional

from ethereumetl.cli import get_block_range_for_date, export_blocks_and_transactions

from chain.blockchain import Blockchain
from chain.exporter import Exporter
from utils.s3_operator import S3Operator


def build_ethereum(
        provider_uris: List[str],
        output_bucket: str,
        export_max_workers: int = 10,
        export_batch_size: int = 10,
        exporter_schedule_interval: str = '30 0 * * *',
        notification_emails: Optional[List[str]] = None,
        **kwargs
) -> Blockchain:
    s3 = S3Operator(aws_conn_id='aws_default', bucket=output_bucket)
    ep = Exporter.export_path

    def get_block_range(tempdir: str, date: datetime, provider_uri: str) -> Tuple[int, int]:
        logging.info(f'Calling get_block_range_for_date({provider_uri}, {date}, ...)')
        get_block_range_for_date.callback(
            provider_uri=provider_uri, date=date, output=os.path.join(tempdir, "blocks_meta.txt")
        )

        with open(os.path.join(tempdir, "blocks_meta.txt")) as block_range_file:
            block_range = block_range_file.read()
            start_block, end_block = block_range.split(",")

        return int(start_block), int(end_block)

    def export_blocks_and_transactions_command(execution_date: datetime, provider_uri: str, **kwargs) -> None:
        with TemporaryDirectory() as tempdir:
            start_block, end_block = get_block_range(tempdir, execution_date, provider_uri)

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

            s3.copy_to_export_path(os.path.join(tempdir, "blocks_meta.txt"), ep("blocks_meta", execution_date))
            s3.copy_to_export_path(os.path.join(tempdir, "blocks.json"), ep("blocks", execution_date))
            s3.copy_to_export_path(os.path.join(tempdir, "transactions.json"), ep("transactions", execution_date))

    blocks_and_transaction_export = Exporter(
        task_id='export_blocks_and_transactions',
        toggle=kwargs.get('export_blocks_and_transactions_toggle'),
        provider_uris=provider_uris,
        export_callable=export_blocks_and_transactions_command,
        dependencies=[]
    )

    return Blockchain(
        name='ethereum',
        exporters=[blocks_and_transaction_export],
        exporter_schedule_interval=exporter_schedule_interval,
        notification_emails=notification_emails
    )
