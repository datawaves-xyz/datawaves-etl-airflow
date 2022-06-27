# Can't remove

from chains.evm_chain import build_evm_chain
from variables import read_evm_vars, read_evm_loader_spark_vars

# Ethereum
ethereum_vars = read_evm_vars(
    prefix='ethereum_',
    export_max_workers=10,
    export_batch_size=10,
    export_schedule_interval='30 0 * * *'
)

ethereum_loader_spark_conf = read_evm_loader_spark_vars('ethereum_loader_')
output_bucket = ethereum_vars['output_bucket']

ethereum = build_evm_chain(**ethereum_vars)

for dag in ethereum.build_all_dags(output_bucket, ethereum_loader_spark_conf):
    globals()[dag.dag_id] = dag
