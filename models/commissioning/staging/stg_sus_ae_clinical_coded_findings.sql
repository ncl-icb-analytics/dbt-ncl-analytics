{{
    config(materialized = 'view')
}}

select primarykey_id
    ,coded_findings_id
    ,rownumber_id
    ,code
from {{ ref('raw_sus_ae_clinical_coded_findings') }}