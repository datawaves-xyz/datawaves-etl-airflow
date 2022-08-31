select if(
(
select count(transaction_hash)
from {{ params.schema_name }}.traces
where trace_address is null and transaction_hash is not null
    and dt = '{{ ds }}'
) =
(
select count(*)
from {{ params.schema_name }}.transactions
where dt = '{{ ds }}'
), 1,
raise_error('Total number of traces with null address is not equal to transaction count'))