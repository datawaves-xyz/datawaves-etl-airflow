select if(
(
  select count(1) as cnt
  from {{ params.schema_name }}.logs
  where dt > '{{ ds }}'
) > 0, 1,
cast((select 'There are no latest logs') as integer))