-- Staging model for dictionary_dbo.ReadCodes
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_ReadCodeID" as sk_read_code_id,
    "ReadCode" as read_code,
    "Term" as term,
    "SnomedCTCode" as snomed_ct_code,
    "InNationalDataset" as in_national_dataset,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated,
    "IsSensitive" as is_sensitive,
    "SK_ReadCodeParentID" as sk_read_code_parent_id,
    "SK_BNFChapterID" as sk_bnf_chapter_id,
    "IsReadV2" as is_read_v2,
    "IsCTV3" as is_ctv3,
    "ReadCodeAlt" as read_code_alt,
    "SK_ReadCodeParentID_ReadV2" as sk_read_code_parent_id_read_v2,
    "SK_ReadCodeParentID_CTV3" as sk_read_code_parent_id_ctv3,
    "MatchesReadCodeRegex" as matches_read_code_regex
from {{ source('dictionary_dbo', 'ReadCodes') }}
