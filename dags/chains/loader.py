from typing import Optional, List

from airflow import DAG
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from templates.evm_load_template import load_temp_table_template_map, enrich_table_template_map, drop_table_sql
from variables import SparkConf

temp_table_names = {
    'temp_block_table': 'blocks_{{nodash}}',
    'temp_transaction_table': 'transactions_{{nodash}}',
    'temp_contract_table': 'contracts_{{nodash}}',
    'temp_log_table': 'logs_{{nodash}}',
    'temp_price_table': 'prices_{{nodash}}',
    'temp_transfer_table': 'token_transfers_{{nodash}}',
    'temp_token_table': 'tokens_{{nodash}}',
    'temp_trace_table': 'traces_{{nodash}}',
    'temp_receipt_table': 'receipts_{{nodash}}'
}


class Loader:
    resource: str
    file_format: str
    load_operator: BaseOperator
    enrich_operator: Optional[BaseOperator]
    clean_operator: BaseOperator
    enrich_dependencies: List[str]
    clean_dependencies: List[str]
    enrich_toggle: bool

    def __init__(
            self,
            resource: str,
            enrich_dependencies: List[str],
            clean_dependencies: List[str],
            file_format: str = 'json',
            enrich_toggle: bool = True
    ) -> None:
        self.resource = resource
        self.file_format = file_format
        self.enrich_dependencies = enrich_dependencies
        self.clean_dependencies = clean_dependencies
        self.enrich_toggle = enrich_toggle

    def gen_operators(
            self,
            dag: DAG,
            database: str,
            temp_database: str,
            output_bucket: str,
            spark_conf: SparkConf
    ) -> None:
        wait_sensor = S3KeySensor(
            task_id=f'wait_latest_{self.resource}',
            timeout=60 * 60,
            poke_interval=60,
            bucket_key=self.export_path,
            bucket_name=output_bucket,
            dag=dag
        )

        load_sql = load_temp_table_template_map[self.resource](
            database,
            self.temp_table,
            self.file_format,
            self.s3_export_full_path(output_bucket),
        )

        load_operator = SparkSubmitOperator(
            task_id=f'load_{self.resource}',
            java_class=spark_conf.java_class,
            application=spark_conf.application,
            conf=spark_conf.conf,
            jars=spark_conf.jars,
            application_args=['--sql', load_sql],
            dag=dag
        )

        wait_sensor >> load_operator
        self.load_operator = load_operator

        if self.enrich_toggle:
            enrich_sql = enrich_table_template_map[self.resource](
                database,
                temp_database,
                self.resource,
                self.temp_table,
                temp_table_names
            )

            enrich_operator = SparkSubmitOperator(
                task_id=f'enrich_{self.resource}',
                java_class=spark_conf.java_class,
                application=spark_conf.application,
                conf=spark_conf.conf,
                jars=spark_conf.jars,
                application_args=['--sql', enrich_sql],
                dag=dag
            )
            self.load_operator >> enrich_operator
            self.enrich_operator = enrich_operator
        else:
            self.enrich_operator = None

        s3_delete_operator = S3DeleteObjectsOperator(
            task_id=f'clean_{self.resource}_s3_file',
            bucket=output_bucket,
            keys=self.export_path,
            dag=dag
        )

        clean_operator = SparkSubmitOperator(
            task_id=f'clean_{self.resource}',
            java_class=spark_conf.java_class,
            application=spark_conf.application,
            conf=spark_conf.conf,
            jars=spark_conf.jars,
            application_args=['--sql', drop_table_sql(temp_database, self.temp_table)],
            dag=dag
        )

        if self.enrich_toggle:
            self.enrich_operator >> clean_operator

        clean_operator >> s3_delete_operator
        self.clean_operator = clean_operator

    @property
    def export_path(self) -> str:
        return 'export/{task}/block_date={{ds}}/{task}.{file_format}'.format(
            task=self.resource, file_format=self.file_format
        )

    @property
    def temp_table(self) -> str:
        return '{resource}_{{ds_nodash}}'.format(resource=self.resource)

    def s3_export_full_path(self, bucket: str) -> str:
        return f's3a://{bucket}/{self.export_path}'
