from typing import Optional, List

from airflow import DAG
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from templates.evm_load_template import load_temp_table_template_map, enrich_table_template_map, drop_table_sql
from variables import SparkConf
from resource_apply import SparkResource

import copy

temp_table_names = {
    'temp_block_table': 'blocks_{{ds_nodash}}',
    'temp_transaction_table': 'transactions_{{ds_nodash}}',
    'temp_contract_table': 'contracts_{{ds_nodash}}',
    'temp_log_table': 'logs_{{ds_nodash}}',
    'temp_price_table': 'prices_{{ds_nodash}}',
    'temp_transfer_table': 'token_transfers_{{ds_nodash}}',
    'temp_token_table': 'tokens_{{ds_nodash}}',
    'temp_trace_table': 'traces_{{ds_nodash}}',
    'temp_receipt_table': 'receipts_{{ds_nodash}}'
}

enrich_operator_spark_conf_map = {
    'geth_traces': SparkResource(
        executor_cores=2,
        executor_memory=10,
        executor_instances=2,
        driver_cores=1,
        driver_memory=2
    )
}

class Loader:
    chain: str
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
            chain: str,
            resource: str,
            file_format: str = 'json',
            enrich_toggle: bool = True,
            enrich_dependencies: List[str] = None,
            clean_dependencies: List[str] = None
    ) -> None:
        self.chain = chain
        self.resource = resource
        self.file_format = file_format
        self.enrich_toggle = enrich_toggle
        self.enrich_dependencies = [] if enrich_dependencies is None else enrich_dependencies
        self.clean_dependencies = [] if clean_dependencies is None else clean_dependencies

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
            temp_database,
            self.temp_table,
            self.file_format,
            self.s3_export_full_path(output_bucket),
        )

        load_operator = SparkSubmitOperator(
            task_id=f'load_{self.resource}',
            name='load_{resource}_{{{{ds_nodash}}}}'.format(resource=self.resource),
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
                **temp_table_names
            )

            custom_spark_conf = copy.deepcopy(spark_conf.conf)
            enrich_operator_custom_spark_conf = enrich_operator_spark_conf_map.get(self.resource)
            if enrich_operator_custom_spark_conf is not None:
                custom_spark_conf.update(enrich_operator_custom_spark_conf)

            enrich_operator = SparkSubmitOperator(
                task_id=f'enrich_{self.resource}',
                name='enrich_{resource}_{{{{ds_nodash}}}}'.format(resource=self.resource),
                java_class=spark_conf.java_class,
                application=spark_conf.application,
                conf=custom_spark_conf,
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
            name='clean_{resource}_{{{{ds_nodash}}}}'.format(resource=self.resource),
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
        return 'export/{chain}/{task}/block_date={{{{ds}}}}/{task}.{file_format}'.format(
            chain=self.chain, task=self.resource, file_format=self.file_format
        )

    @property
    def temp_table(self) -> str:
        return '{resource}_{{{{ds_nodash}}}}'.format(resource=self.resource)

    def s3_export_full_path(self, bucket: str) -> str:
        return f's3a://{bucket}/{self.export_path}'
