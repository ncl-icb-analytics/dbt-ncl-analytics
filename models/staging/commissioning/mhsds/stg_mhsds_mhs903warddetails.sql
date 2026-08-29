{{
    config(
        materialized = 'view',
        tags=['mhsds']
        )
}}
-- Ward details must be joined to MHS502 by provider, submission and ward code.
{{ select_active_mhsds_records(ref('raw_mhsds_mhs903warddetails')) }}
