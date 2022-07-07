from typing import List, Optional

from chains.transfer import Transfer
from chains.transfer_client import TransformConfig
from constant import get_default_dag_args
from variables import SparkConf, S3Conf, read_transfer_vars, parse_dict, read_var

try:
    from airflow import DAG
except Exception as e:
    pass


def build_dags(
        transform_config: TransformConfig,
        schedule_interval: str,
        spark_conf: SparkConf,
        schema_registry_s3_conf: S3Conf,
        notification_emails: Optional[List[str]] = None,
        **kwargs
) -> List[DAG]:
    dags = []
    for client in transform_config.clients:
        dag = DAG(
            dag_id=client.dag_name,
            schedule_interval=schedule_interval,
            default_args=get_default_dag_args(notification_emails)
        )
        transfer = Transfer(client)
        transfer.gen_operators(dag, spark_conf, schema_registry_s3_conf)
        dags.append(dag)

    return dags


transfer_vars = read_transfer_vars(schedule_interval='0 2 * * *')

# TODO: use api
config = TransformConfig.from_dict(parse_dict(read_var(var_name='transfer_config', required=True)))

for dag in build_dags(transform_config=config, **transfer_vars):
    globals()[dag.dag_id] = dag
