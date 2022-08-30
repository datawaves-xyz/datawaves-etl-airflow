select if(
(
  select count(1) as cnt
  from `{{ params.schema_name }}.transactions`
  where date(timestamp) = '{{ ds }}'
) > 0, 1,
cast((select 'There are no latest transactions') as integer))