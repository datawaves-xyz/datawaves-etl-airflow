from typing import Dict, Callable, Any


def create_temp_block_table_sql(database: str, table: str, file_format: str, file_path: str) -> str:
    return f"""CREATE TABLE IF NOT EXISTS `{database}`.`{table}`
    (
        number            BIGINT,
        hash              STRING,
        parent_hash       STRING,
        nonce             STRING,
        sha3_uncles       STRING,
        logs_bloom        STRING,
        transactions_root STRING,
        state_root        STRING,
        receipts_root     STRING,
        miner             STRING,
        difficulty        DECIMAL(38, 0),
        total_difficulty  DECIMAL(38, 0),
        size              BIGINT,
        extra_data        STRING,
        gas_limit         BIGINT,
        gas_used          BIGINT,
        timestamp         BIGINT,
        transaction_count BIGINT,
        base_fee_per_gas  BIGINT
    ) USING {file_format} OPTIONS (path "{file_path}");"""


def create_temp_contract_table_sql(database: str, table: str, file_format: str, file_path: str) -> str:
    return f"""CREATE TABLE IF NOT EXISTS `{database}`.`{table}`
    (
        address            STRING,
        bytecode           STRING,
        function_sighashes STRING,
        is_erc20           BOOLEAN,
        is_erc721          BOOLEAN,
        block_number       BIGINT
    ) USING {file_format} OPTIONS (path "{file_path}");"""


def create_temp_log_table_sql(database: str, table: str, file_format: str, file_path: str) -> str:
    return f"""CREATE TABLE IF NOT EXISTS `{database}`.`{table}`
    (
        log_index         BIGINT,
        transaction_hash  STRING,
        transaction_index BIGINT,
        block_hash        STRING,
        block_number      BIGINT,
        address           STRING,
        data              STRING,
        topics            STRING
    ) USING {file_format} OPTIONS (path "{file_path}");"""


def create_temp_price_table_sql(database: str, table: str, file_format: str, file_path: str) -> str:
    return f"""CREATE TABLE IF NOT EXISTS `{database}`.`{table}`
    (
        minute           TIMESTAMP,
        price            DOUBLE,
        decimals         BIGINT,
        contract_address STRING,
        symbol           STRING,
        dt               DATE
    ) USING {file_format} OPTIONS (path "{file_path}", header true);"""


def create_temp_receipt_table_sql(database: str, table: str, file_format: str, file_path: str) -> str:
    return f"""CREATE TABLE IF NOT EXISTS `{database}`.`{table}`
    (
        transaction_hash    STRING,
        transaction_index   BIGINT,
        block_hash          STRING,
        block_number        BIGINT,
        cumulative_gas_used BIGINT,
        gas_used            BIGINT,
        contract_address    STRING,
        root                STRING,
        status              BIGINT,
        effective_gas_price BIGINT
    ) USING {file_format} OPTIONS (path "{file_path}");"""


def create_temp_token_transfer_table_sql(database: str, table: str, file_format: str, file_path: str) -> str:
    return f"""CREATE TABLE IF NOT EXISTS `{database}`.`{table}`
    (
        token_address    STRING,
        from_address     STRING,
        to_address       STRING,
        value            DECIMAL(38, 0),
        transaction_hash STRING,
        log_index        BIGINT,
        block_number     BIGINT
    ) USING {file_format} OPTIONS (path "{file_path}");"""


def create_temp_token_table_sql(database: str, table: str, file_format: str, file_path: str) -> str:
    return f"""CREATE TABLE IF NOT EXISTS `{database}`.`{table}`
    (
        address      STRING,
        symbol       STRING,
        name         STRING,
        decimals     STRING,
        total_supply STRING,
        block_number BIGINT
    ) USING {file_format} OPTIONS (path "{file_path}");"""


def create_temp_traces_table_sql(database: str, table: str, file_format: str, file_path: str) -> str:
    return f"""CREATE TABLE IF NOT EXISTS `{database}`.`{table}`
    (
        block_number      BIGINT,
        transaction_hash  STRING,
        transaction_index BIGINT,
        from_address      STRING,
        to_address        STRING,
        value             DECIMAL(38, 0),
        input             STRING,
        output            STRING,
        trace_type        STRING,
        call_type         STRING,
        reward_type       STRING,
        gas               BIGINT,
        gas_used          BIGINT,
        subtraces         BIGINT,
        trace_address     STRING,
        error             STRING,
        status            BIGINT,
        trace_id          STRING
    ) USING {file_format} OPTIONS (path "{file_path}");"""


def create_temp_transaction_table_sql(database: str, table: str, file_format: str, file_path: str) -> str:
    return f"""CREATE TABLE IF NOT EXISTS `{database}`.`{table}`
    (
        hash                     STRING,
        nonce                    BIGINT,
        block_hash               STRING,
        block_number             BIGINT,
        transaction_index        BIGINT,
        from_address             STRING,
        to_address               STRING,
        value                    DECIMAL(38, 0),
        gas                      BIGINT,
        gas_price                BIGINT,
        input                    STRING,
        max_fee_per_gas          BIGINT,
        max_priority_fee_per_gas BIGINT,
        transaction_type         BIGINT
    ) USING {file_format} OPTIONS (path "{file_path}");"""


def enrich_block_table_sql(database: str, temp_database: str, table: str, temp_table: str, **kwargs) -> str:
    return """INSERT OVERWRITE TABLE `{database}`.`{table}`
        PARTITION (dt = date '{{{{ds}}}}')
        SELECT /*+ REPARTITION(1) */
            TIMESTAMP_SECONDS(blocks.timestamp) AS `timestamp`,
            blocks.number,
            blocks.hash,
            blocks.parent_hash,
            blocks.nonce,
            blocks.sha3_uncles,
            blocks.logs_bloom,
            blocks.transactions_root,
            blocks.state_root,
            blocks.receipts_root,
            blocks.miner,
            blocks.difficulty,
            blocks.total_difficulty,
            blocks.size,
            blocks.extra_data,
            blocks.gas_limit,
            blocks.gas_used,
            blocks.transaction_count,
            blocks.base_fee_per_gas
        FROM `{temp_database}`.`{temp_table}` AS blocks""".format(
        database=database, temp_database=temp_database, table=table, temp_table=temp_table
    )


def enrich_contract_table_sql(database: str, temp_database: str, table: str, temp_table: str, **kwargs) -> str:
    temp_block_table = kwargs['temp_block_table']
    return """INSERT OVERWRITE TABLE `{database}`.`{table}`
        PARTITION (dt = date '{{{{ds}}}}')
        SELECT /*+ REPARTITION(1) */
            contracts.address,
            contracts.bytecode,
            contracts.function_sighashes,
            contracts.is_erc20,
            contracts.is_erc721,
            TIMESTAMP_SECONDS(blocks.timestamp) AS block_timestamp,
            blocks.number                       AS block_number,
            blocks.hash                         AS block_hash
        FROM `{temp_database}`.`{temp_table}` AS contracts
        JOIN `{temp_database}`.`{temp_block_table}` AS blocks
            ON contracts.block_number = blocks.number""".format(
        database=database, temp_database=temp_database, table=table, temp_table=temp_table,
        temp_block_table=temp_block_table
    )


def enrich_log_table_sql(database: str, temp_database: str, table: str, temp_table: str, **kwargs) -> str:
    temp_block_table = kwargs['temp_block_table']
    return """INSERT OVERWRITE TABLE `{database}`.`{table}`
        PARTITION (dt = date '{{{{ds}}}}', address_hash, selector_hash)
        SELECT /*+ REPARTITION(1) */
            log_index,
            transaction_hash,
            transaction_index,
            address,
            data,
            topics,
            block_timestamp,
            block_number,
            block_hash,
            topics_arr,
            unhex_data,
            topics_arr[0]                 AS selector,
            address_hash,
            abs(hash(topics_arr[0])) % 10 AS selector_hash
        FROM (
             SELECT logs.log_index,
                    logs.transaction_hash,
                    logs.transaction_index,
                    logs.address,
                    logs.data,
                    logs.topics,
                    TIMESTAMP_SECONDS(blocks.timestamp) AS block_timestamp,
                    blocks.number                       AS block_number,
                    blocks.hash                         AS block_hash,
                    abs(hash(logs.address)) % 10        AS address_hash,
                    IF(
                        logs.topics rlike ',',
                        IF(logs.topics rlike '^[0-9]+', split(replace(logs.topics, '"', ''), ','),
                           from_json(logs.topics, 'array<string>')),
                        array(logs.topics)
                    )                                   AS topics_arr,
                    unhex(substr(logs.data, 3))         AS unhex_data
             FROM `{temp_database}`.`{temp_block_table}` AS blocks
             JOIN `{temp_database}`.`{temp_table}` AS logs
                 ON blocks.number = logs.block_number)""".format(
        database=database, temp_database=temp_database, table=table, temp_table=temp_table,
        temp_block_table=temp_block_table
    )


def enrich_price_table_sql(database: str, temp_database: str, table: str, temp_table: str, **kwargs) -> str:
    return """INSERT OVERWRITE TABLE `{database}`.`{table}`
        PARTITION (dt = date '{{{{ds}}}}')
        SELECT /*+ REPARTITION(1) */
            prices.minute,
            prices.price,
            prices.decimals,
            prices.contract_address,
            prices.symbol
        FROM `{temp_database}`.`{temp_table}` AS prices""".format(
        database=database, temp_database=temp_database, table=table, temp_table=temp_table
    )


def enrich_token_transfer_table_sql(database: str, temp_database: str, table: str, temp_table: str, **kwargs) -> str:
    temp_block_table = kwargs['temp_block_table']
    return """INSERT OVERWRITE TABLE `{database}`.`{table}`
        PARTITION (dt = date '{{{{ds}}}}')
        SELECT /*+ REPARTITION(1) */
            token_transfers.token_address,
            token_transfers.from_address,
            token_transfers.to_address,
            token_transfers.value,
            token_transfers.transaction_hash,
            token_transfers.log_index,
            TIMESTAMP_SECONDS(blocks.timestamp) AS block_timestamp,
            blocks.number                       AS block_number,
            blocks.hash                         AS block_hash
        FROM `{temp_database}`.`{temp_block_table}` AS blocks
        JOIN `{temp_database}`.`{temp_table}` AS token_transfers
            ON blocks.number = token_transfers.block_number""".format(
        database=database, temp_database=temp_database, table=table, temp_table=temp_table,
        temp_block_table=temp_block_table
    )


def enrich_token_table_sql(database: str, temp_database: str, table: str, temp_table: str, **kwargs) -> str:
    return """INSERT INTO TABLE `{database}`.`{table}`
    SELECT /*+ REPARTITION(1) */
        tokens.address,
        tokens.symbol,
        tokens.name,
        tokens.decimals,
        tokens.total_supply
    FROM `{temp_database}`.`{temp_table}` AS tokens
    WHERE tokens.address IN
    (
        SELECT address FROM `{temp_database}`.`{temp_table}`
        EXCEPT
        SELECT address FROM `{database}`.`{table}`
    )""".format(
        database=database, temp_database=temp_database, table=table, temp_table=temp_table
    )


def enrich_trace_table_sql(database: str, temp_database: str, table: str, temp_table: str, **kwargs) -> str:
    temp_block_table = kwargs['temp_block_table']
    temp_transaction_table = kwargs['temp_transaction_table']

    return """INSERT OVERWRITE TABLE `{database}`.`{table}`
    PARTITION (dt = date '{{{{ds}}}}', address_hash, selector_hash)
    SELECT /*+ REPARTITION(1) */
        transactions.hash                           AS transaction_hash,
        traces.transaction_index,
        traces.from_address,
        traces.to_address,
        traces.value,
        traces.input,
        traces.output,
        traces.trace_type,
        traces.call_type,
        traces.reward_type,
        traces.gas,
        traces.gas_used,
        traces.subtraces,
        traces.trace_address,
        traces.error,
        traces.status,
        traces.trace_id,
        TIMESTAMP_SECONDS(blocks.timestamp)         AS block_timestamp,
        blocks.number                               AS block_number,
        blocks.hash                                 AS block_hash,
        substr(traces.input, 1, 10)                 AS selector,
        unhex(substr(traces.input, 3))              AS unhex_input,
        unhex(substr(traces.output, 3))             AS unhex_output,
        abs(hash(traces.to_address)) % 10           AS address_hash,
        abs(hash(substr(traces.input, 1, 10))) % 10 AS selector_hash
    FROM `{temp_database}`.`{temp_block_table}` AS blocks
    JOIN `{temp_database}`.`{temp_table}` AS traces
        ON blocks.number = traces.block_number
    JOIN `{temp_database}`.`{temp_transaction_table}` AS transactions
        ON traces.transaction_index = transactions.transaction_index
        AND traces.block_number = transactions.block_number
    """.format(
        database=database, temp_database=temp_database, table=table, temp_table=temp_table,
        temp_block_table=temp_block_table, temp_transaction_table=temp_transaction_table
    )


def enrich_transaction_table_sql(database: str, temp_database: str, table: str, temp_table: str, **kwargs) -> str:
    temp_block_table = kwargs['temp_block_table']
    temp_receipt_table = kwargs['temp_receipt_table']
    return """INSERT OVERWRITE TABLE `{database}`.`{table}`
    PARTITION (dt = date '{{{{ds}}}}')
    SELECT /*+ REPARTITION(1) */
        transactions.hash,
        transactions.nonce,
        transactions.transaction_index,
        transactions.from_address,
        transactions.to_address,
        transactions.value,
        transactions.gas,
        transactions.gas_price,
        transactions.input,
        receipts.cumulative_gas_used        AS receipt_cumulative_gas_used,
        receipts.gas_used                   AS receipt_gas_used,
        receipts.contract_address           AS receipt_contract_address,
        receipts.root                       AS receipt_root,
        receipts.status                     AS receipt_status,
        TIMESTAMP_SECONDS(blocks.timestamp) AS block_timestamp,
        blocks.number                       AS block_number,
        blocks.hash                         AS block_hash,
        transactions.max_fee_per_gas,
        transactions.max_priority_fee_per_gas,
        transactions.transaction_type,
        receipts.effective_gas_price        AS receipt_effective_gas_price
    FROM `{temp_database}`.`{temp_block_table}` AS blocks
    JOIN `{temp_database}`.`{temp_table}` AS transactions
        ON blocks.number = transactions.block_number
    JOIN `{temp_database}`.`{temp_receipt_table}` AS receipts
        ON transactions.hash = receipts.transaction_hash""".format(
        database=database, temp_database=temp_database, table=table, temp_table=temp_table,
        temp_block_table=temp_block_table, temp_receipt_table=temp_receipt_table
    )


def drop_table_sql(database: str, table: str) -> str:
    return f'DROP TABLE {database}.{table}'


load_temp_table_template_map: Dict[str, Callable[[str, str, str, str], str]] = {
    'blocks': create_temp_block_table_sql,
    'contracts': create_temp_contract_table_sql,
    'logs': create_temp_log_table_sql,
    'prices': create_temp_price_table_sql,
    'receipts': create_temp_receipt_table_sql,
    'token_transfers': create_temp_token_transfer_table_sql,
    'tokens': create_temp_token_table_sql,
    'traces': create_temp_traces_table_sql,
    'transactions': create_temp_transaction_table_sql
}

enrich_table_template_map: Dict[str, Callable[[str, str, str, str, Dict[str, Any]], str]] = {
    'blocks': enrich_block_table_sql,
    'contracts': enrich_contract_table_sql,
    'logs': enrich_log_table_sql,
    'prices': enrich_price_table_sql,
    'token_transfers': enrich_token_transfer_table_sql,
    'tokens': enrich_token_table_sql,
    'traces': enrich_trace_table_sql,
    'transactions': enrich_transaction_table_sql
}
