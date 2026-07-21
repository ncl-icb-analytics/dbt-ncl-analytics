{{
    config(materialized = 'view')
}}

select primarykey_id
    ,snomed_id
    ,rownumber_id
    ,code
from {{ ref('raw_sus_ecds_clinical_investigations_snomed') }}