
from datetime import timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow import DAG

from utils.common import read_file, dags_folder
from variables import SparkConf

import os
import logging


class Verifier:
    chain: str

    def __init__(self, chain: str, spark_conf: SparkConf):
        self.chain = chain
        self.spark_conf = spark_conf
        self.dags_folder = dags_folder()
        self.environment = {
            'schema_name': self.chain
        }

    def gen_verify_tasks(self, dag: DAG) -> None:
        enrich_blocks_sensor = ExternalTaskSensor(
            task_id=f'wait_for_{self.chain}_enrich_blocks',
            external_dag_id=f'{self.chain}_load_dag',
            external_task_id=f'enrich_blocks',
            execution_delta=timedelta(minutes=30),
            priority_weight=0,
            mode='reschedule',
            poke_interval=5 * 60,
            timeout=60 * 60 * 12,
            dag=dag
        )

        enrich_transactions_sensor = ExternalTaskSensor(
            task_id=f'wait_for_{self.chain}_enrich_transactions',
            external_dag_id=f'{self.chain}_load_dag',
            external_task_id=f'enrich_transactions',
            execution_delta=timedelta(minutes=30),
            priority_weight=0,
            mode='reschedule',
            poke_interval=5 * 60,
            timeout=60 * 60 * 12,
            dag=dag
        )

        enrich_logs_sensor = ExternalTaskSensor(
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

        enrich_traces_sensor = ExternalTaskSensor(
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

        self._add_verify_task('blocks_have_latest', dag, dependencies=[enrich_blocks_sensor])
        self._add_verify_task('transactions_have_latest', dag, dependencies=[enrich_transactions_sensor])

    def _add_verify_task(self, task, dag: DAG, dependencies=None):
        sql_path = os.path.join(self.dags_folder, 'resources/verify/{task}.sql'.format(task=task))
        sql = read_file(sql_path)

        logging.info(sql)

        verify_operator = SparkSubmitOperator(
            task_id='verify_{task}'.format(task=task),
            name='verify_{task}_{{{{ds_nodash}}}}'.format(task=task),
            java_class=self.spark_conf.java_class,
            application=self.spark_conf.application,
            conf=self.spark_conf.conf,
            jars=self.spark_conf.jars,
            application_args=['--sql', sql],
            params=self.environment,
            dag=dag
        )
        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                dependency >> verify_operator
        return verify_operator
