select
    org_id_provider,
    uniq_submission_id,
    reporting_period_end_date
from {{ref('raw_mhsds_activesubmission')}}
