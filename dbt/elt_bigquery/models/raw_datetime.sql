{{ config(materialized='ephemeral') }}

{% set start_date = var('start_date')%}
{% set end_date = var('end_date')%}

{{
    dbt_utils.date_spine(
        datepart="hour",
        start_date="cast('" + start_date + "' as datetime)",
        end_date="cast('" + end_date + "' as datetime)",
    )
}}
