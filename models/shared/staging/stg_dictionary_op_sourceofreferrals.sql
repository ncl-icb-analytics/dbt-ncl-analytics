-- Staging model for dictionary_op.SourceOfReferrals
-- Source: "Dictionary"."OP"
-- Description: Reference data for outpatient procedures and treatments

select
    "SK_SourceOfReferral" as sk_sourceofreferral,
    "BK_SourceOfReferralCode" as bk_sourceofreferralcode,
    "ReferralType" as referraltype,
    "ReferralGroup" as referralgroup,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary_op', 'SourceOfReferrals') }}
