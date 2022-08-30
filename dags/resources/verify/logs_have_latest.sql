select if(
(
  select count(1) as cnt
  from {{ params.schema_name }}.traces
  where dt = '{{ ds }}'
) > 0, 1,
cast((select 'There are no latest traces') as integer))