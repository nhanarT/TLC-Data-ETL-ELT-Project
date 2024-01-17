{{ config(materialized='incremental', unique_key='hvfhs_license_id') }}

with distinct_hvfhs_num as (
    select
        distinct hvfhs_license_num
    from 
        {{ source('elt_staging', 'nyc_tlc_trip') }}
    order by 1
)
select
    {{ dbt_utils.generate_surrogate_key(['hvfhs_license_num']) }} hvfhs_license_id,
    hvfhs_license_num
from
    distinct_hvfhs_num
