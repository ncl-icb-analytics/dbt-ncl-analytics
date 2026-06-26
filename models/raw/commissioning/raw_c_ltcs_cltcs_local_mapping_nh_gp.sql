{{
    config(
        description="Raw layer (C-LTCS tables). 1:1 passthrough with cleaned column names. \nSource: {{ 'PUBLISHED_REPORTING__DIRECT_CARE' if target.name in ['prod', 'snowflake-prod'] else ('DEV__PUBLISHED_REPORTING__DIRECT_CARE' if target.name in ['dev', 'snowflake-dev'] else target.name | upper | trim ~ '__PUBLISHED_REPORTING__DIRECT_CARE') }}.C_LTCS.CLTCS_LOCAL_MAPPING_NH_GP \ndbt: source(''c_ltcs'', ''CLTCS_LOCAL_MAPPING_NH_GP'') \nColumns:\n  PRACTICE_CODE -> practice_code\n  NEIGHBOURHOOD_CODE -> neighbourhood_code\n  PRACTICE_NAME -> practice_name\n  NEIGHBOURHOOD_REGISTERED -> neighbourhood_registered\n  LOCAL_AUTHORITY -> local_authority"
    )
}}
select
    "PRACTICE_CODE" as practice_code,
    "NEIGHBOURHOOD_CODE" as neighbourhood_code,
    "PRACTICE_NAME" as practice_name,
    "NEIGHBOURHOOD_REGISTERED" as neighbourhood_registered,
    "LOCAL_AUTHORITY" as local_authority
from {{ source('c_ltcs', 'CLTCS_LOCAL_MAPPING_NH_GP') }}
