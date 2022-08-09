
from datetime import timedelta
from dataclasses import dataclass
from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime
from tempfile import TemporaryDirectory

from offchains.prices import CoinpaprikaPriceProvider
from chains.exporter import Exporter


import logging
import os

from utils.s3_operator import S3Operator


def export_path(chain: str, directory: str, date: datetime) -> str:
    return f'export/{chain}/{directory}/block_date={date.strftime("%Y-%m-%d")}/'


@dataclass
class PricesMixin:
    chain: str
    coinpaprika_auth_key: str


class PricesExporter(Exporter, PricesMixin):

    def __post_init__(self):
        self.s3 = S3Operator(aws_conn_id='aws_default', bucket=self.output_bucket)

    def gen_export_task(self, dag: DAG) -> None:
        if self.toggle:
            self.operator = PythonOperator(
                task_id=self.task_id,
                python_callable=self.export_prices,
                provide_context=True,
                execution_timeout=timedelta(hours=15),
                dag=dag
            )
        else:
            self.operator = None

    def export_prices(self, logical_date: datetime):
        with TemporaryDirectory() as tempdir:
            start_ts = int(logical_date.start_of('day').timestamp())
            end_ts = int(logical_date.end_of('day').timestamp())

            logging.info('Calling export_prices({}, {})'.format(start_ts, end_ts))

            prices_provider = CoinpaprikaPriceProvider(auth_key=self.coinpaprika_auth_key)
            prices_provider.create_temp_csv(
                output_path=os.path.join(tempdir, "prices.csv"),
                start=start_ts,
                end=end_ts
            )

            self.s3.copy_to_export_path(os.path.join(tempdir, "prices.csv"),
                                        export_path(self.chain, "prices", logical_date))
