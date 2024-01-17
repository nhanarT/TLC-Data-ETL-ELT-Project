{{ config(materialized='incremental') }}

select 
    {{ dbt_utils.generate_surrogate_key(['hvfhs_license_num']) }} hvfhs_license_id,
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num']) }} originating_base_id,
    {{ dbt_utils.generate_surrogate_key(['originating_base_num']) }} dispatching_base_id,
    format_date('%Y%m%d-%H', pickup_datetime) pickup_datetime_id,
    format_date('%Y%m%d-%H', dropoff_datetime) dropoff_datetime_id,
    PULocationID,
    DOLocationID,
    base_passenger_fare,
    tolls,
    bcf,
    sales_tax,
    congestion_surcharge,
    airport_fee,
    tips,
    driver_pay,
    trip_miles,
    trip_time,
    access_a_ride_flag,
    shared_request_flag,
    shared_match_flag,
    wav_match_flag,
    wav_request_flag
from
    {{ source('elt_staging', 'nyc_tlc_trip') }} rd
{% if is_incremental() %}
where format_date('%Y%m%d-%H', pickup_datetime) > (select max(pickup_datetime_id) from {{ this }})
{% endif %}
