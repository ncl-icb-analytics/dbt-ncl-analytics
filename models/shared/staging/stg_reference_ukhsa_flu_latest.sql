-- Staging model for reference_terminology.UKHSA_FLU_LATEST
-- Source: "DATA_LAKE__NCL"."TERMINOLOGY"
-- Description: Reference terminology data including SNOMED, BNF, and other code sets

select
    "CODING_SCHEME" as coding_scheme,
    "CODE_LIBRARY" as code_library,
    "CODE_GROUP" as code_group,
    "CODE_GROUP_DESCRIPTION" as code_group_description,
    "SNOMED_CODE" as snomed_code,
    "SNOMED_DESCRIPTION" as snomed_description,
    "DATE_CREATED" as date_created,
    "VALIDATED_SCTID" as validated_sctid,
    "EMIS_ASTRX" as emis_astrx,
    "UNNAMED_9" as unnamed_9,
    "TPP_ASTRX" as tpp_astrx
from {{ source('reference_terminology', 'UKHSA_FLU_LATEST') }}
