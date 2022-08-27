from datetime import timedelta
from typing import List

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor

from chains.parser import Parser
from experiments.contract_service import ContractDTO
from variables import SparkConf, S3Conf


class ExperimentEvmParser(Parser):
    chain: str
    contracts: List[ContractDTO]
    is_experiment: bool = True

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
            for element in contract.abi_element():
                element_type = element[0]
                element_name = element[1]

                if element_type != 'event' and element_type != 'function':
                    continue

                application_args = [
                    '--contract-id',
                    contract.id,
                    '--abi-type',
                    element_type,
                    '--abi-name',
                    element_name,
                    '--dt',
                    '{{ds}}'
                ]

                task_name = self.get_task_name(contract, element_type, element_name)

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

                (logs_sensor if element_type == 'event' else traces_sensor) >> operator

    @staticmethod
    def get_task_name(contract: ContractDTO, element_type: str, element_name: str) -> str:
        table_name = f'{contract.name}_{"evt" if element_type == "event" else "call"}_{element_name}'
        return f'{contract.project}.{table_name}'
