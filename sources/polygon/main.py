from polygonetl.cli import (
    get_block_range_for_date,
    export_blocks_and_transactions,
    extract_field,
    export_receipts_and_logs,
    extract_contracts,
    extract_tokens,
    extract_token_transfers,
    export_geth_traces,
    extract_geth_traces
)
from datetime import datetime
from typing import Tuple
from tempfile import TemporaryDirectory

from base.s3 import S3

import logging
import os
import click


def export_path(chain: str, directory: str, date: datetime) -> str:
    return f'export/{chain}/{directory}/block_date={date.strftime("%Y-%m-%d")}/'


def get_block_range(tempdir: str, provider_uri: str, date: datetime) -> Tuple[int, int]:
    logging.info(f'Calling get_block_range_for_date({provider_uri}, {date}, ...)')
    get_block_range_for_date.callback(
        provider_uri=provider_uri,
        date=date,
        output=os.path.join(tempdir, "blocks_meta.txt")
    )

    with open(os.path.join(tempdir, "blocks_meta.txt")) as block_range_file:
        block_range = block_range_file.read()
        start_block, end_block = block_range.split(",")
        logging.info(f'start_block={start_block}, end_block={end_block}')

    return int(start_block), int(end_block)


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-c', '--chain', required=True, type=str, help='The chain network to connect to.')
@click.option('-d', '--logical-date', required=True, type=str, help='The logical date to export.')
@click.pass_context
def export_blocks_and_transactions_command(ctx, chain, logical_date):
    s3 = S3(ctx.obj['aws_access_key_id'],
            ctx.obj['aws_secret_access_key'],
            ctx.obj['region_name'],
            ctx.obj['bucket_name'])

    with TemporaryDirectory() as tempdir:

        start_block, end_block = get_block_range(tempdir, ctx.obj['provider_uri'],
                                                 datetime.strptime(logical_date, '%Y-%m-%d'))

        logging.info('Calling export_blocks_and_transactions({}, {}, {}, {}, {}, ...)'.format(
            start_block, end_block, ctx.obj['batch_size'], ctx.obj['provider_uri'], ctx.obj['max_workers']))

        export_blocks_and_transactions.callback(
            start_block=start_block,
            end_block=end_block,
            batch_size=ctx.obj['batch_size'],
            provider_uri=ctx.obj['provider_uri'],
            max_workers=ctx.obj['max_workers'],
            blocks_output=os.path.join(tempdir, "blocks.json"),
            transactions_output=os.path.join(tempdir, "transactions.json"),
        )

        s3.copy_to_export_path(os.path.join(tempdir, "blocks_meta.txt"),
                               export_path(chain, "blocks_meta", logical_date))
        s3.copy_to_export_path(os.path.join(tempdir, "blocks.json"),
                               export_path(chain, "blocks", logical_date))
        s3.copy_to_export_path(os.path.join(tempdir, "transactions.json"),
                               export_path(chain, "transactions", logical_date))


@click.group()
@click.pass_context
def cli(ctx):
    ctx.obj = {
        'aws_access_key_id': os.environ['aws_access_key_id'],
        'aws_secret_access_key': os.environ['aws_secret_access_key'],
        'region_name': os.environ['region_name'],
        'bucket_name': os.environ['bucket_name'],
        'provider_uri': os.environ['provider_uri'],
        'batch_size': int(os.environ['batch_size']),
        'max_workers': int(os.environ['max_workers'])
    }
    logging.info(ctx.obj)


cli.add_command(export_blocks_and_transactions_command, "export_blocks_and_transactions")

cli()
