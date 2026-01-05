{{
    config(
        materialized='view',
        tags=['staging', 'emis', 'reference']
    )
}}

/*
EMIS List Size - Staging

Stages EMIS list size reference data with proper date type conversion.
Source: emis_list_size seed file
Extract Date: 04/11/2025

Data Quality Notes:
- Converts EXTRACT_DATE from VARCHAR to DATE
- Standardizes column naming to lowercase
- Used as reference data for OLIDS validation
*/

select
    BOROUGH as borough,
    CODE as practice_code,
    GP_PRACTICE as practice_name,
    LIST_SIZE as list_size,
    TO_DATE(EXTRACT_DATE, 'DD/MM/YYYY') as extract_date

from {{ ref('emis_list_size') }}
where EXTRACT_DATE is not null
