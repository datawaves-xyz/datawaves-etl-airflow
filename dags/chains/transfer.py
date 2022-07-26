from datetime import timedelta
from typing import Dict

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor

from chains.resource_apply import get_raw_table_spark_resource, get_abi_table_spark_resource
from chains.transfer_client import TransferClient
from variables import SparkConf, S3Conf


class Transfer:
    client: TransferClient

    def __init__(self, client: TransferClient):
        self.client = client

    def gen_operators(self, dag: DAG, spark_conf: SparkConf, schema_registry_s3_conf: S3Conf) -> None:
        for raw in self.client.raws:
            sensor_id = f'wait_for_{raw.upstream_task_name}'
            task_id = f'transfer_{raw.upstream_task_name}'

            spark_config: Dict[str, str] = spark_conf.conf.copy()
            raw_table_spark_resource_conf = get_raw_table_spark_resource(raw)
            if raw_table_spark_resource_conf is not None:
                spark_config.update(raw_table_spark_resource_conf)

            abi_element_args = [
                '--chain',
                raw.chain,
                '--table-name',
                raw.table,
            ]

            args = []
            args += spark_conf.application_args
            args += self.client.application_args
            args += abi_element_args
            args += [
                '--dt',
                '{{ds}}'
            ]

            sensor = ExternalTaskSensor(
                task_id=sensor_id,
                external_dag_id=raw.upstream_dag_name,
                external_task_id=raw.upstream_task_id,
                execution_delta=timedelta(hours=1),
                priority_weight=0,
                mode='reschedule',
                poke_interval=5 * 60,
                timeout=60 * 60 * 12,
                dag=dag
            )

            task = SparkSubmitOperator(
                task_id=task_id,
                name=task_id,
                java_class=spark_conf.java_class,
                application=spark_conf.application,
                conf=spark_config,
                jars=spark_conf.jars,
                application_args=args,
                dag=dag
            )

            sensor >> task

        for abi in self.client.all_abis:
            sensor_id = f'wait_for_{abi.upstream_task_name}'
            task_id = f'transfer_{abi.upstream_task_name}'

            spark_config: Dict[str, str] = spark_conf.conf.copy()
            abi_table_spark_resource_conf = get_abi_table_spark_resource(abi)
            if abi_table_spark_resource_conf is not None:
                spark_config.update(abi_table_spark_resource_conf)

            abi_element_args = [
                '--chain',
                abi.chain,
                '--group-name',
                abi.dataset_name,
                '--contract-name',
                abi.contract_name,
                '--abi-type',
                abi.abi_type,
                '--abi-name',
                abi.abi_name,
            ]

            args = []
            args += spark_conf.application_args
            args += self.client.application_args
            args += abi_element_args
            args += [
                '--schema-registry-s3-access-key',
                schema_registry_s3_conf.access_key,
                '--schema-registry-s3-secret-key',
                schema_registry_s3_conf.secret_key,
                '--schema-registry-s3-region',
                schema_registry_s3_conf.region,
                '--dt',
                '{{ds}}'
            ]

            sensor = ExternalTaskSensor(
                task_id=sensor_id,
                external_dag_id=abi.upstream_dag_name,
                external_task_id=abi.upstream_task_id,
                execution_delta=timedelta(minutes=30),
                priority_weight=0,
                mode='reschedule',
                poke_interval=5 * 60,
                timeout=60 * 60 * 12,
                dag=dag
            )

            task = SparkSubmitOperator(
                task_id=task_id,
                name=task_id,
                java_class=spark_conf.java_class,
                application=spark_conf.application,
                conf=spark_config,
                jars=spark_conf.jars,
                application_args=args,
                dag=dag
            )

            sensor >> task
