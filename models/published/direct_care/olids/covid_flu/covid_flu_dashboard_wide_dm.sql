{{
    config(
        materialized='view'
    )
}}


select * 
FROM {{ ref('int_covid_flu_dashboard_wide') }}