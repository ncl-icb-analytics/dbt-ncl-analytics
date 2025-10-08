{{
    config(
        materialized='view',
        tags=['valproate']
    )
}}

SELECT *
FROM {{ ref('int_valproate_base') }}