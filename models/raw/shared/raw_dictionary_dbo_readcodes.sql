{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.ReadCodes \ndbt: source(''dictionary_dbo'', ''ReadCodes'') \nColumns:\n  SK_ReadCodeID -> sk_read_code_id\n  ReadCode -> read_code\n  Term -> term\n  SnomedCTCode -> snomed_ct_code\n  InNationalDataset -> in_national_dataset\n  DateCreated -> date_created\n  DateUpdated -> date_updated\n  IsSensitive -> is_sensitive\n  SK_ReadCodeParentID -> sk_read_code_parent_id\n  SK_BNFChapterID -> sk_bnf_chapter_id\n  IsReadV2 -> is_read_v2\n  IsCTV3 -> is_ctv3\n  ReadCodeAlt -> read_code_alt\n  SK_ReadCodeParentID_ReadV2 -> sk_read_code_parent_id_read_v2\n  SK_ReadCodeParentID_CTV3 -> sk_read_code_parent_id_ctv3\n  MatchesReadCodeRegex -> matches_read_code_regex"
    )
}}
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
