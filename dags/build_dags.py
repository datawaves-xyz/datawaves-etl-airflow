from dags.chain.ethereum import build_ethereum
from dags.variables import read_ethereum_vars

# Ethereum
ethereum_vars = read_ethereum_vars()
ethereum = build_ethereum(**ethereum_vars)
ethereum.build_all_dags()

# Polygon and more
