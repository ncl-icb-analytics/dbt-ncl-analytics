{{
    config(
        materialized='table',
        tags=['daily']
    )
}}

/* Stable reporting contract for source content-currency signals. */

select *
from {{ ref('stg_source_content_freshness') }}
