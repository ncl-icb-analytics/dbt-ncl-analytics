-- Staging model for dictionary_dbo.BNF_Substance_Product_Presentation
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_BNFID" as sk_bnfid,
    "SK_BNFParentID" as sk_bnf_parent_id,
    "SK_BNFChapterID" as sk_bnf_chapter_id,
    "TypeNum" as type_num,
    "Type" as type,
    "Code" as code,
    "Name" as name,
    "Path" as path,
    "Path_Depth" as path_depth,
    "IsSubstance" as is_substance,
    "IsProduct" as is_product,
    "IsPresentation" as is_presentation,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated,
    "IsGeneric" as is_generic,
    "SK_BNFID_GenericEquivalent" as sk_bnfid_generic_equivalent
from {{ source('dictionary_dbo', 'BNF_Substance_Product_Presentation') }}
