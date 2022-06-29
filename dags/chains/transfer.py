from datetime import timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor

from chains.transfer_client import TransformClient
from variables import SparkConf, S3Conf


class Transfer:
    client: TransformClient

    def __init__(self, client: TransformClient):
        self.client = client

    def gen_operators(self, dag: DAG, spark_conf: SparkConf, schema_registry_s3_conf: S3Conf) -> None:
        for raw in self.client.raws:
            sensor_id = f'wait_for_{raw.upstream_task_name}'
            task_id = f'transform_{raw.upstream_task_name}'

            args = []
            args += spark_conf.application_args
            args += self.client.application_args
            args += [
                '--chain',
                raw.chain,
                '--table-name',
                raw.table,
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
                conf=spark_conf.conf,
                jars=spark_conf.jars,
                application_args=args,
                dag=dag
            )

            sensor >> task

        for abi in self.client.abis:
            sensor_id = f'wait_for_{abi.upstream_task_name}'
            task_id = f'transform_{abi.upstream_task_name}'

            args = []
            args += spark_conf.application_args
            args += self.client.application_args
            args += [
                '--schema-registry-s3-access-key',
                schema_registry_s3_conf.access_key,
                '--schema-registry-s3-secret-key',
                schema_registry_s3_conf.secret_key,
                '--schema-registry-s3-region',
                schema_registry_s3_conf.region,
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
                conf=spark_conf.conf,
                jars=spark_conf.jars,
                application_args=args,
                dag=dag
            )

            sensor >> task
