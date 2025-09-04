-- Staging model for dictionary_op.SourceOfReferrals
-- Source: "Dictionary"."OP"
-- Description: Reference data for outpatient procedures and treatments

select
    "SK_SourceOfReferral" as sk_source_of_referral,
    "BK_SourceOfReferralCode" as bk_source_of_referral_code,
    "ReferralType" as referral_type,
    "ReferralGroup" as referral_group,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_op', 'SourceOfReferrals') }}
