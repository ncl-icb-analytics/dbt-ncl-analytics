{{ config(materialized='view') }}

/* Thin input for source_content_freshness_history. */

select
    source_schema,
    content_date
from {{ ref('source_content_freshness') }}
