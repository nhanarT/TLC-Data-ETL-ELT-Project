{{ config(materialized='incremental', unique_key='base_num_id') }}

with distinct_base_num as (
    select
        distinct originating_base_num base_num
    from 
        {{ source('elt_staging', 'nyc_tlc_trip') }}
    union distinct
    select
        distinct dispatching_base_num base_num
    from 
        {{ source('elt_staging', 'nyc_tlc_trip') }}
    )
select
    {{ dbt_utils.generate_surrogate_key(['base_num']) }} base_num_id,
    base_num
from
    distinct_base_num 
