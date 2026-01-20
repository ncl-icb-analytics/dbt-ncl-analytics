{{
    config(
        description="Raw layer (Reference data for outpatient procedures and treatments). 1:1 passthrough with cleaned column names. \nSource: Dictionary.OP.SourceOfReferrals \ndbt: source(''dictionary_op'', ''SourceOfReferrals'') \nColumns:\n  SK_SourceOfReferral -> sk_source_of_referral\n  BK_SourceOfReferralCode -> bk_source_of_referral_code\n  ReferralType -> referral_type\n  ReferralGroup -> referral_group\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
select
    "SK_SourceOfReferral" as sk_source_of_referral,
    "BK_SourceOfReferralCode" as bk_source_of_referral_code,
    "ReferralType" as referral_type,
    "ReferralGroup" as referral_group,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_op', 'SourceOfReferrals') }}
