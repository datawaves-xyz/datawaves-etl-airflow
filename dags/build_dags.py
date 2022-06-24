# Can't remove
from airflow import DAG

from chain.ethereum import build_ethereum
from variables import read_ethereum_vars

# Ethereum
ethereum_vars = read_ethereum_vars(
    prefix='ethereum_',
    export_max_workers=10,
    export_batch_size=10,
    export_schedule_interval='30 0 * * *'
)

ethereum = build_ethereum(**ethereum_vars)

for dag in ethereum.build_all_dags():
    globals()[dag.dag_id] = dag

# Polygon and more
