select if(
(
  select count(1) as cnt
  from {{ params.schema_name }}.transactions
  where dt = '{{ ds }}'
) > 0, 1, raise_error('There are no latest transactions'))