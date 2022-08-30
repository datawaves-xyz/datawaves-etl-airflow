import logging

try:
    from airflow import DAG
except Exception as e:
    pass

from chains.evm_chain import build_evm_chain
from variables import read_evm_vars

# Ethereum
ethereum_vars = read_evm_vars(
    prefix='ethereum_',
    export_max_workers=10,
    export_batch_size=10,
    export_schedule_interval='30 0 * * *',
    load_schedule_interval='0 1 * * *',
    verify_schedule_interval='30 1 * * *',
    parse_schedule_interval='30 1 * * *',
    experiment_parse_schedule_interval='30 1 * * *'
)

logging.info(f"ethereum vars: {ethereum_vars}")

ethereum = build_evm_chain(chain='ethereum', **ethereum_vars)
for dag in ethereum.build_all_dags(**ethereum_vars):
    globals()[dag.dag_id] = dag

# Polygon
polygon_vars = read_evm_vars(
    prefix='polygon_',
    export_max_workers=10,
    export_batch_size=10,
    export_schedule_interval='30 0 * * *',
    load_schedule_interval='0 1 * * *',
    verify_schedule_interval='30 1 * * *',
    parse_schedule_interval='30 1 * * *'
)

logging.info(f"polygon vars: {polygon_vars}")

polygon = build_evm_chain(chain='polygon', **polygon_vars)
for dag in polygon.build_all_dags(**polygon_vars):
    globals()[dag.dag_id] = dag
