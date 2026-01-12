{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.BNF_Substance_Product_Presentation \ndbt: source(''dictionary_dbo'', ''BNF_Substance_Product_Presentation'') \nColumns:\n  SK_BNFID -> sk_bnfid\n  SK_BNFParentID -> sk_bnf_parent_id\n  SK_BNFChapterID -> sk_bnf_chapter_id\n  TypeNum -> type_num\n  Type -> type\n  Code -> code\n  Name -> name\n  Path -> path\n  Path_Depth -> path_depth\n  IsSubstance -> is_substance\n  IsProduct -> is_product\n  IsPresentation -> is_presentation\n  DateCreated -> date_created\n  DateUpdated -> date_updated\n  IsGeneric -> is_generic\n  SK_BNFID_GenericEquivalent -> sk_bnfid_generic_equivalent"
    )
}}
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
