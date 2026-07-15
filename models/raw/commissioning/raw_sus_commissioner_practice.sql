select
    "SK_ORGANISATIONID_PRACTICE" as sk_organisation_id_practice,
    "ORGANISATIONCODE_PRACTICE" as organisation_code_practice,
    "SK_ORGANISATIONID_COMMISSIONER" as sk_organisation_id_commissioner,
    "ORGANISATIONCODE_COMMISSIONER" as organisation_code_commissioner,
    "PRACTICESTARTDATE" as practice_start_date,
    "PRACTICEENDDATE" as practice_end_date,
    "RELATIONSHIPSTARTDATE" as relationship_start_date,
    "RELATIONSHIPENDDATE" as relationship_end_date,
    "ISACTIVE" as is_active,
    "ISPROXY" as is_proxy
from {{ source('sus_commissioner_reference', 'practice_commissioner') }}
