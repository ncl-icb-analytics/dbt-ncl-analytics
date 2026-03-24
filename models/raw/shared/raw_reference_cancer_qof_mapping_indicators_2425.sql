{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.CANCER__QOF_MAPPING_INDICATORS_2425 \ndbt: source(''reference_analyst_managed'', ''CANCER__QOF_MAPPING_INDICATORS_2425'') \nColumns:\n  INDICATOR_CODE -> indicator_code\n  INDICATOR_DESCRIPTION -> indicator_description\n  INDICATOR_POINT_VALUE -> indicator_point_value\n  GROUP_CODE -> group_code\n  GROUP_DESCRIPTION -> group_description\n  SUB_DOMAIN_CODE -> sub_domain_code\n  SUB_DOMAIN_DESCRIPTION -> sub_domain_description\n  DOMAIN_CODE -> domain_code\n  DOMAIN_DESCRIPTION -> domain_description\n  PATIENT_LIST_TYPE -> patient_list_type"
    )
}}
select
    "INDICATOR_CODE" as indicator_code,
    "INDICATOR_DESCRIPTION" as indicator_description,
    "INDICATOR_POINT_VALUE" as indicator_point_value,
    "GROUP_CODE" as group_code,
    "GROUP_DESCRIPTION" as group_description,
    "SUB_DOMAIN_CODE" as sub_domain_code,
    "SUB_DOMAIN_DESCRIPTION" as sub_domain_description,
    "DOMAIN_CODE" as domain_code,
    "DOMAIN_DESCRIPTION" as domain_description,
    "PATIENT_LIST_TYPE" as patient_list_type
from {{ source('reference_analyst_managed', 'CANCER__QOF_MAPPING_INDICATORS_2425') }}
