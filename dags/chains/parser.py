import json
from datetime import timedelta
from typing import List, TypeVar

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor

from chains.contracts import EvmContract, Contract, EvmAbiElement
from variables import SparkConf, S3Conf

C = TypeVar('C', bound=Contract)


class Parser:
    chain: str
    contracts: List[C]
    is_experiment: bool = False

    def __init__(self, chain: str, contracts: List[C]) -> None:
        self.chain = chain
        self.contracts = contracts

    def gen_operators(self, dag: DAG, spark_conf: SparkConf, s3_conf: S3Conf) -> None:
        raise NotImplementedError()

    @property
    def dag_id(self) -> str:
        dag_name = f'{self.chain}_parse_{self.contracts[0].dataset_name}_dag'
        if self.is_experiment:
            dag_name = f'experiment_{dag_name}'
        return dag_name


class EvmParser(Parser):
    chain: str
    contracts: List[EvmContract]

    def gen_operators(self, dag: DAG, spark_conf: SparkConf, s3_conf: S3Conf) -> None:
        logs_sensor = ExternalTaskSensor(
            task_id=f'wait_for_{self.chain}_enrich_logs',
            external_dag_id=f'{self.chain}_load_dag',
            external_task_id=f'enrich_logs',
            execution_delta=timedelta(minutes=30),
            priority_weight=0,
            mode='reschedule',
            poke_interval=5 * 60,
            timeout=60 * 60 * 12,
            dag=dag
        )

        traces_sensor = ExternalTaskSensor(
            task_id=f'wait_for_{self.chain}_enrich_traces',
            external_dag_id=f'{self.chain}_load_dag',
            external_task_id=f'enrich_traces',
            execution_delta=timedelta(minutes=30),
            priority_weight=0,
            mode='reschedule',
            poke_interval=5 * 60,
            timeout=60 * 60 * 12,
            dag=dag
        )

        for contract in self.contracts:
            for element in contract.abi:
                etype = element.type

                if etype != 'event' and etype != 'function':
                    continue

                application_args = [
                    '--group-name',
                    contract.dataset_name,
                    '--contract-name',
                    contract.contract_name,
                    '--abi-type',
                    etype,
                    '--abi-json',
                    json.dumps(element.to_dict()),
                    '--abi-name',
                    element.name,
                    '--s3-access-key',
                    s3_conf.access_key,
                    '--s3-secret-key',
                    s3_conf.secret_key,
                    '--s3-bucket',
                    s3_conf.bucket,
                    '--s3-region',
                    s3_conf.region,
                    '--dt',
                    '{{ds}}'
                ]

                if contract.contract_address is not None:
                    application_args.extend([
                        '--contract-address',
                        contract.contract_address
                    ])

                task_name = self.get_task_name(contract, element)
                operator = SparkSubmitOperator(
                    task_id=task_name,
                    name=task_name,
                    java_class=spark_conf.java_class,
                    application=spark_conf.application,
                    conf=spark_conf.conf,
                    jars=spark_conf.jars,
                    application_args=application_args,
                    dag=dag
                )

                (logs_sensor if etype == 'event' else traces_sensor) >> operator

    @staticmethod
    def get_task_name(contract: Contract, element: EvmAbiElement) -> str:
        table_name = f'{contract.contract_name}_{"evt" if element.type == "event" else "call"}_{element.name}'
        return f'{contract.dataset_name}.{table_name}'
