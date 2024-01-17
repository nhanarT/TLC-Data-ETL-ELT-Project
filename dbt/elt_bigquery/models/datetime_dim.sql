{{ config(materialized='incremental') }}

select
    format_date('%Y%m%d-%H', date_hour) datetime_id,
    {{ dbt_date.date_part('year', 'date_hour') }} year,
    {{ dbt_date.date_part('month', 'date_hour') }} month,
    {{ dbt_date.date_part('day', 'date_hour') }} day,
    {{ dbt_date.date_part('hour', 'date_hour') }} hour,
    {{ dbt_date.date_part('dayofweek', 'date_hour') }} weekday
from
    {{ ref('raw_datetime') }}
{% if is_incremental() %}
where format_date('%Y%m%d-%H', date_hour) > (select max(old_dt.datetime_id) from {{ this }} old_dt)
{% endif %}
