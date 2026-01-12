{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.UKHSA_FLU_LATEST \ndbt: source(''reference_terminology'', ''UKHSA_FLU_LATEST'') \nColumns:\n  CODING_SCHEME -> coding_scheme\n  CODE_LIBRARY -> code_library\n  CODE_GROUP -> code_group\n  CODE_GROUP_DESCRIPTION -> code_group_description\n  SNOMED_CODE -> snomed_code\n  SNOMED_DESCRIPTION -> snomed_description\n  DATE_CREATED -> date_created\n  VALIDATED_SCTID -> validated_sctid\n  EMIS_ASTRX -> emis_astrx\n  UNNAMED_9 -> unnamed_9\n  TPP_ASTRX -> tpp_astrx"
    )
}}
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
