{{
    config(
        materialized = 'table',
        tags=['mhsds']
        )
}}
--ward details can be linked to mhs502wardstay using ward_code.
select *
FROM {{ ref('raw_mhsds_mhs903warddetails') }}