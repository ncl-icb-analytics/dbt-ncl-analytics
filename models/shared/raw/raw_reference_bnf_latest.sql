-- Raw layer model for reference_terminology.BNF_LATEST
-- Source: "DATA_LAKE__NCL"."TERMINOLOGY"
-- Description: Reference terminology data including SNOMED, BNF, and other code sets
-- This is a 1:1 passthrough from source with standardized column names
select
    "PRESENTATION_PACK_LEVEL" as presentation_pack_level,
    "VMP_VMPP_AMP_AMPP" as vmp_vmpp_amp_ampp,
    "BNF_CODE" as bnf_code,
    "BNF_NAME" as bnf_name,
    "SNOMED_CODE" as snomed_code,
    "DM_D_PRODUCT_DESCRIPTION" as dm_d_product_description,
    "STRENGTH" as strength,
    "UNIT_OF_MEASURE" as unit_of_measure,
    "DM_D_PRODUCT_PACK_DESCRIPTION" as dm_d_product_pack_description,
    "PACK" as pack,
    "SUB_PACK" as sub_pack,
    "VTM" as vtm,
    "VTM_NAME" as vtm_name
from {{ source('reference_terminology', 'BNF_LATEST') }}
