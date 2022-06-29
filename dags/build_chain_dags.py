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
    parse_schedule_interval='30 1 * * *'
)

ethereum = build_evm_chain(chain='ethereum', **ethereum_vars)
for dag in ethereum.build_all_dags(**ethereum_vars):
    globals()[dag.dag_id] = dag
