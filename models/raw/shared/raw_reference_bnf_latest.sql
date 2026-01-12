{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.BNF_LATEST \ndbt: source(''reference_terminology'', ''BNF_LATEST'') \nColumns:\n  PRESENTATION_PACK_LEVEL -> presentation_pack_level\n  VMP_VMPP_AMP_AMPP -> vmp_vmpp_amp_ampp\n  BNF_CODE -> bnf_code\n  BNF_NAME -> bnf_name\n  SNOMED_CODE -> snomed_code\n  DM_D_PRODUCT_DESCRIPTION -> dm_d_product_description\n  STRENGTH -> strength\n  UNIT_OF_MEASURE -> unit_of_measure\n  DM_D_PRODUCT_PACK_DESCRIPTION -> dm_d_product_pack_description\n  PACK -> pack\n  SUB_PACK -> sub_pack\n  VTM -> vtm\n  VTM_NAME -> vtm_name"
    )
}}
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
